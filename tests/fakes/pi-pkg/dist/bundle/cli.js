#!/usr/bin/env node
// Stands in for pi's CLI in signin.rs's tests: --version, `--offline --list-models` (the providers
// in FAKE_PI_AUTH, "provider:authType,…") and `auth check --provider X --json --no-refresh`.
const args = process.argv.slice(2);
const auth = Object.fromEntries((process.env.FAKE_PI_AUTH || "").split(",").filter(Boolean).map((p) => p.split(":")));
if (args[0] === "--version") {
    console.log("0.0.1");
} else if (args.includes("--list-models")) {
    const ids = Object.keys(auth);
    if (ids.length === 0) console.log("No models available. Use /login or set an API key environment variable.");
    else {
        console.log("provider    model           context  max-out  thinking  images");
        for (const p of ids) for (const m of ["m-one", "m-two"]) console.log(`${p.padEnd(11)} ${m.padEnd(15)} 128K     16K      yes       no`);
    }
} else if (args[0] === "auth" && args[1] === "check") {
    const p = args[args.indexOf("--provider") + 1];
    const r = auth[p] ? { status: "ready", provider: p, authType: auth[p] } : { status: "not_ready", provider: p, reason: "credentials_not_configured" };
    console.log(JSON.stringify(r));
    process.exitCode = auth[p] ? 0 : 1;
} else {
    process.exitCode = 2;
}
