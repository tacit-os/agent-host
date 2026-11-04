// agent-host's pi sign-in helper (signin.rs writes it to ~/.local/share/agent-host/pi-login.mjs).
// Signs in to, or out of, one pi provider through pi's own SDK, which saves the credential in
// $PI_CODING_AGENT_DIR/auth.json under pi's file lock.
//   node pi-login.mjs login <provider> <browser|device> <pi package dir>
//   node pi-login.mjs logout <provider> <pi package dir>
// Steps are JSON lines on stdout:
//   {"type":"auth_url",url,instructions?}  {"type":"device_code",userCode,verificationUri}
//   {"type":"progress"|"info",message}  {"type":"done"}  {"type":"error",message}
// A pasted code or redirect URL is read from stdin. pi asks for one (manual_code) only raced
// against its callback server, so the ask is not passed on.
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline";

const [cmd, provider, ...rest] = process.argv.slice(2);
const pkg = rest[rest.length - 1];
const method = cmd === "login" ? rest[0] : undefined;
const say = (o) => process.stdout.write(JSON.stringify(o) + "\n");

const lines = [];
const waiting = [];
createInterface({ input: process.stdin }).on("line", (l) => {
    const w = waiting.shift();
    if (w) w(l.trim());
    else lines.push(l.trim());
});

function nextLine(signal) {
    if (lines.length) return Promise.resolve(lines.shift());
    return new Promise((resolve, reject) => {
        const take = (l) => {
            signal?.removeEventListener("abort", stop);
            resolve(l);
        };
        const stop = () => {
            const i = waiting.indexOf(take);
            if (i >= 0) waiting.splice(i, 1);
            reject(new Error("Login cancelled"));
        };
        if (signal?.aborted) return stop();
        signal?.addEventListener("abort", stop, { once: true });
        waiting.push(take);
    });
}

const interaction = {
    async prompt(p) {
        switch (p.type) {
            case "select": {
                // OpenAI's choice between browser and device code
                const want = method === "device" ? /device|headless/i : null;
                const o = (want && p.options.find((o) => want.test(o.id) || want.test(o.label))) || p.options[0];
                return o.id;
            }
            case "manual_code":
                return nextLine(p.signal);
            case "text":
                // GitHub Copilot's enterprise domain: blank is github.com
                return "";
            default:
                throw new Error("This sign-in asks for a key; add the key in Settings");
        }
    },
    notify(e) {
        if (e.type === "auth_url") say({ type: "auth_url", url: e.url, instructions: e.instructions });
        else if (e.type === "device_code") say({ type: "device_code", userCode: e.userCode, verificationUri: e.verificationUri });
        else if (e.type === "progress" || e.type === "info") say({ type: e.type, message: e.message });
    },
};

try {
    if ((cmd !== "login" && cmd !== "logout") || !provider || !pkg) throw new Error("usage: pi-login.mjs login|logout <provider> …");
    const sdk = await import(pathToFileURL(join(pkg, "dist", "index.js")).href);
    const runtime = await sdk.ModelRuntime.create({ authPath: join(sdk.getAgentDir(), "auth.json"), refreshOnCreate: false });
    if (cmd === "logout") await runtime.logout(provider);
    else await runtime.login(provider, "oauth", interaction);
    say({ type: "done" });
    process.exit(0);
} catch (e) {
    say({ type: "error", message: e instanceof Error ? e.message : String(e) });
    process.exit(1);
}
