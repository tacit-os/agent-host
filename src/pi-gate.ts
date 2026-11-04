// agent-host's permission gate for the pi sessions it runs:
//   pi --mode rpc --no-extensions -e pi-gate.ts --praxis-mode <mode>
// agent-host writes this file (it is compiled into the host) and answers its questions.
//
// Before bash, edit or write runs, the gate asks the host with ctx.ui.select, which RPC mode sends
// as an extension_ui_request; the host answers with an extension_ui_response whose value is
// "Allow", "Always" (this tool for the rest of the process) or "Deny". The title is "agent-host:"
// followed by JSON {tool, id, input}.
//
// It holds the mode: --praxis-mode at start, then "/praxis-mode <mode>" sent as a prompt (an
// extension command, which runs at once and is not written to the session). After either, and at
// session_start, it reports the mode it holds with setStatus("praxis-gate", mode); the host sends
// no prompt until it has seen that.
//
// Modes: manual (and default) ask for all three; acceptEdits and auto let edit and write through
// and ask for bash; plan refuses edit and write and asks for bash; bypassPermissions asks nothing.

const asks = ["bash", "edit", "write"];

export default function (pi: any) {
  pi.registerFlag("praxis-mode", { description: "agent-host's permission mode", type: "string", default: "manual" });
  let mode = "";
  const always = new Set<string>();
  const report = (ctx: any) => ctx.ui.setStatus("praxis-gate", mode);

  pi.on("session_start", async (_event: any, ctx: any) => {
    if (!mode) mode = String(pi.getFlag("praxis-mode") || "manual");
    report(ctx);
  });

  pi.registerCommand("praxis-mode", {
    description: "Set agent-host's permission mode",
    handler: async (args: string, ctx: any) => {
      mode = args.trim() || "manual";
      report(ctx);
    },
  });

  pi.on("tool_call", async (event: any, ctx: any) => {
    const tool = event.toolName;
    if (!asks.includes(tool) || mode === "bypassPermissions" || always.has(tool)) return undefined;
    const edit = tool !== "bash";
    if (edit && mode === "plan") return { block: true, reason: "Plan mode: no file changes until the plan is approved." };
    if (edit && (mode === "acceptEdits" || mode === "auto")) return undefined;
    const title = "agent-host:" + JSON.stringify({ tool, id: event.toolCallId, input: event.input });
    const choice = await ctx.ui.select(title, ["Allow", "Always", "Deny"]);
    if (choice === "Always") always.add(tool);
    if (choice === "Allow" || choice === "Always") return undefined;
    return { block: true, reason: "Denied in Praxis." };
  });
}
