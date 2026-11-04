// Stands in for pi's SDK in signin.rs's tests: getAgentDir and a ModelRuntime whose login drives
// the helper's interaction by FAKE_MODE (browser, code, device, copilot, hang, fail) and notes what
// it was given in $FAKE_OUT/pi-login.json; logout notes $FAKE_OUT/pi-logout.json.
import { writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const out = process.env.FAKE_OUT;
const mode = process.env.FAKE_MODE || "browser";
const note = (f, o) => writeFileSync(join(out, f), JSON.stringify(o));
const later = (ms) => new Promise((r) => setTimeout(r, ms));

export function getAgentDir() {
    return process.env.PI_CODING_AGENT_DIR || join(homedir(), ".pi", "agent");
}

export class ModelRuntime {
    static async create(options) {
        return new ModelRuntime(options);
    }
    constructor(options) {
        this.options = options;
    }
    async login(provider, type, ia) {
        const seen = { pid: process.pid, provider, type, authPath: this.options.authPath };
        note("pi-login.json", seen);
        if (mode === "fail") throw new Error("Token exchange failed: 400");
        if (mode === "device") {
            seen.chose = await ia.prompt({ type: "select", message: "Select OpenAI Codex login method:",
                options: [{ id: "browser", label: "Browser login (default)" }, { id: "device_code", label: "Device code login (headless)" }] });
            note("pi-login.json", seen);
            ia.notify({ type: "device_code", userCode: "PIDE-VICE", verificationUri: "https://auth.example/pi-device" });
            await later(300);
            return { type: "oauth" };
        }
        if (mode === "copilot") {
            seen.domain = await ia.prompt({ type: "text", message: "GitHub Enterprise URL/domain (blank for github.com)" });
            note("pi-login.json", seen);
            ia.notify({ type: "device_code", userCode: "GHCO-PILT", verificationUri: "https://github.com/login/device" });
            await later(300);
            return { type: "oauth" };
        }
        ia.notify({ type: "auth_url", url: "https://auth.example/pi?state=s4", instructions: "A browser window should open." });
        const manual = new AbortController();
        const pasted = ia.prompt({ type: "manual_code", message: "Paste the code", signal: manual.signal });
        if (mode === "code") {
            seen.code = await pasted;
            note("pi-login.json", seen);
            if (seen.code !== "good-code") throw new Error("State mismatch");
            return { type: "oauth" };
        }
        if (mode === "hang") {
            await pasted;
            return { type: "oauth" };
        }
        // browser: the callback wins and the paste prompt is withdrawn
        pasted.catch(() => {});
        await later(300);
        manual.abort();
        return { type: "oauth" };
    }
    async logout(provider) {
        note("pi-logout.json", { provider, authPath: this.options.authPath });
    }
}
