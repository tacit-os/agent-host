# The Agents tab's host fixes against a private host on fixture transcripts:
#   python3 tests/agents_fix_e2e.py HOST-BINARY SCRATCH-DIR
# SCRATCH-DIR: a new, short path (the socket lives in it). The test writes HOME, the cache and the
# runtime dir there, starts the host with them, and removes the folder at the end (KEEP=1 keeps it).
# Starts no agent. One workflow agent's transcript that ended on StructuredOutput is copied read-only
# from the real ~/.claude/projects when there is one; a synthetic one is used otherwise.
# Checks: a StructuredOutput is the report (B1); a 44 k report comes back whole and a 250 k one is
# cut and says so (B2); a running step carries last.desc (P7); while a process holds the session, a
# step or model call older than 120 s stays running and an older process's agent does not (B3);
# two concurrent cold "agents" requests read the transcript once, by the host's CPU time (B16); an
# agent's own background agent in the middle of a step is running, and its parent's task
# notification ends it (B3).
import glob, json, os, queue, shutil, signal, socket, subprocess, sys, threading, time

BIN, SB = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
REAL_HOME = os.path.expanduser("~")
BIG_MB = int(os.environ.get("BIG_MB", "40"))
HOME, RT, CACHE = f"{SB}/home", f"{SB}/rt", f"{SB}/cache"
PROJ = f"{HOME}/.claude/projects/-tmp-agents-fix"
CWD = "/tmp/agents-fix"  # only written into the records
SID_A, SID_B = "a9e7f000-0000-4000-8000-00000000000a", "b9e7f000-0000-4000-8000-00000000000b"
SID_C1, SID_C2 = "c9e7f000-0000-4000-8000-0000000000c1", "c9e7f000-0000-4000-8000-0000000000c2"
NOW = time.time()

fails = []
def check(ok, what):
    print("ok  " if ok else "FAIL", what)
    if not ok:
        fails.append(what)

def iso(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int(t * 1000) % 1000:03d}Z"

def rec(kind, content, t, sid, **kw):
    # compact, as Claude Code writes them: the host's scans match "key":"value" without spaces
    return json.dumps({"type": kind, "sessionId": sid, "cwd": CWD, "timestamp": iso(t), "isSidechain": kw.pop("side", False),
                       "message": {"role": kind, "content": content, **kw}}, separators=(",", ":"))

def write(path, lines, mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    if mtime:
        os.utime(path, (mtime, mtime))

def launch(sid, call, agent, desc, t, side=False):
    # the session's (or with side, an agent's) background Agent call and its "launched" result
    return [rec("assistant", [{"type": "tool_use", "id": call, "name": "Agent",
                               "input": {"description": desc, "subagent_type": "general-purpose", "prompt": desc, "run_in_background": True}}], t, sid, side=side),
            rec("user", [{"type": "tool_result", "tool_use_id": call,
                          "content": f"Async agent launched successfully.\nagentId: {agent} (internal ID - do not mention to user.)"}], t, sid, side=side)]

def sub_agent(sid, agent, call, desc, lines, mtime):
    d = f"{PROJ}/{sid}/subagents"
    write(f"{d}/agent-{agent}.meta.json", [json.dumps({"agentType": "general-purpose", "description": desc, "toolUseId": call})])
    write(f"{d}/agent-{agent}.jsonl", lines, mtime)

def own(sid, t):
    # an agent's own transcript lines: sidechain records of the given kinds
    a = lambda content, **kw: rec("assistant", content, t, sid, side=True, model="claude-fixture", **kw)
    u = lambda content: rec("user", content, t, sid, side=True)
    return a, u

def tool(i, name, inp):
    return [{"type": "tool_use", "id": i, "name": name, "input": inp}]

def result(i, text, err=False):
    return [{"type": "tool_result", "tool_use_id": i, "content": text, "is_error": err}]

def real_structured():
    # the newest real workflow agent transcript whose last tool call is an accepted StructuredOutput
    files = glob.glob(f"{REAL_HOME}/.claude/projects/*/*/subagents/workflows/*/agent-*.jsonl")
    files = [f for f in files if NOW - os.path.getmtime(f) > 600]
    for f in sorted(files, key=os.path.getmtime, reverse=True)[:80]:
        last, refused = None, set()
        with open(f, "rb") as h:
            for line in h:
                if b"StructuredOutput" not in line and b"tool_use" not in line and b"is_error\":true" not in line:
                    continue
                try:
                    v = json.loads(line)
                except ValueError:
                    continue
                for b in v.get("message", {}).get("content") or []:
                    if not isinstance(b, dict):
                        continue
                    if b.get("type") == "tool_use":
                        last = b if b.get("name") == "StructuredOutput" else None
                    elif b.get("type") == "tool_result" and b.get("is_error"):
                        refused.add(b.get("tool_use_id"))
        if last and last["id"] not in refused:
            return f, last["input"]
    return None, None

# --- fixtures
if os.path.exists(SB):
    raise SystemExit(f"{SB} exists; give a new folder")
os.makedirs(SB, mode=0o700)
for d in (HOME, RT, CACHE):
    os.makedirs(d, exist_ok=True)
os.chmod(RT, 0o700)
t = NOW - 7200

# session A: a StructuredOutput agent, a long and a very long report, a running step, a workflow run
a_lines = [rec("user", "Run the checks", t, SID_A)]
a_lines += launch(SID_A, "toolu_so", "aso00001", "Structured agent", t)
a_lines += launch(SID_A, "toolu_big", "abig0001", "Long report", t)
a_lines += launch(SID_A, "toolu_huge", "ahuge001", "Very long report", t)
a_lines += launch(SID_A, "toolu_fresh", "afresh01", "Fresh agent", t)
a_lines += [rec("assistant", [{"type": "tool_use", "id": "toolu_wf", "name": "Workflow",
                               "input": {"name": "fixture-flow", "script": "meta = { name: 'fixture-flow', phases: [{ title: 'Check' }] }"}}], t, SID_A),
            rec("user", [{"type": "tool_result", "tool_use_id": "toolu_wf",
                          "content": f"Workflow launched.\nTask ID: wfix0001\nTranscript dir: {PROJ}/{SID_A}/subagents/workflows/wf_fix-001\n"}], t, SID_A),
            rec("user", "<task-notification>\n<task-id>abig0001</task-id>\n<status>completed</status>\n<summary>Agent \"Long report\" completed</summary>\n</task-notification>", t + 60, SID_A)]
write(f"{PROJ}/{SID_A}.jsonl", a_lines)

so_input = {"verdict": "real", "severity": "low", "findings": [{"file": "src/a.rs", "line": 12, "why": "off by one"}], "notes": "none"}
a, u = own(SID_A, t)
sub_agent(SID_A, "aso00001", "toolu_so", "Structured agent", [
    u("Review src/a.rs"), a([{"type": "text", "text": "Let me look at the file."}]), a(tool("r1", "Read", {"file_path": "/tmp/a.rs"})),
    u(result("r1", "fn main() {}")), a(tool("so1", "StructuredOutput", so_input)), u(result("so1", "Structured output provided successfully"))], NOW - 3600)

# 44 000 characters with a code fence across the old 8000 cut
body = ("Findings\n\n" + "A line of the report that says something about the code.\n" * 130 + "```rust\n" + "let x = compute(1, 2, 3);\n" * 80 + "```\n")
big = (body * 10)[:44000].strip()
big = big + "x" * (44000 - len(big))
sub_agent(SID_A, "abig0001", "toolu_big", "Long report", [u("Write a long report"), a([{"type": "text", "text": big}])], NOW - 3600)
huge = ("0123456789" * 25000)
sub_agent(SID_A, "ahuge001", "toolu_huge", "Very long report", [u("Write a very long report"), a([{"type": "text", "text": huge}])], NOW - 3600)
sub_agent(SID_A, "afresh01", "toolu_fresh", "Fresh agent", [u("Check the fixture"),
    a(tool("b1", "Bash", {"command": "make check", "description": "Checking the fixture"}))], None)

# an agent's own background agents: one in the middle of a step, one its parent was told failed
a_lines += launch(SID_A, "toolu_par", "apar0001", "Parent agent", t)
write(f"{PROJ}/{SID_A}.jsonl", a_lines)
told = "[SYSTEM NOTIFICATION - NOT USER INPUT]\n\n<task-notification>\n<task-id>achi0002</task-id>\n<tool-use-id>toolu_ch2</tool-use-id>\n<status>failed</status>\n<summary>Agent \"Child two\" failed</summary>\n</task-notification>"
sub_agent(SID_A, "apar0001", "toolu_par", "Parent agent", [u("Split the work")]
          + launch(SID_A, "toolu_ch1", "achi0001", "Child one", t, True) + launch(SID_A, "toolu_ch2", "achi0002", "Child two", t, True)
          + [u(told)], NOW - 60)
for child, call, desc in (("achi0001", "toolu_ch1", "Child one"), ("achi0002", "toolu_ch2", "Child two")):
    d = f"{PROJ}/{SID_A}/subagents"
    write(f"{d}/agent-{child}.meta.json", [json.dumps({"agentType": "general-purpose", "description": desc, "toolUseId": call,
                                                       "parentAgentId": "apar0001", "spawnDepth": 2})])
    write(f"{d}/agent-{child}.jsonl", [u(desc), a(tool("b1", "Bash", {"command": "make", "description": "Building " + desc}))])

run = f"{PROJ}/{SID_A}/subagents/workflows/wf_fix-001"
real, real_input = real_structured()
if real:
    os.makedirs(run, exist_ok=True)
    shutil.copyfile(real, f"{run}/agent-awf00001.jsonl")
    wf_expect = real_input
    print("workflow agent: a copy of a real transcript,", os.path.getsize(real), "bytes")
else:
    wf_expect = {"status": "done", "summary": "synthetic"}
    write(f"{run}/agent-awf00001.jsonl", [u("Check one thing"), a([{"type": "text", "text": "Working on it."}]),
                                          a(tool("so1", "StructuredOutput", wf_expect)), u(result("so1", "Structured output provided successfully"))])
    print("workflow agent: synthetic (no real one found)")
write(f"{run}/journal.jsonl", [json.dumps({"type": "started", "agentId": "awf00001", "label": "Check one", "phase": "Check"}),
                               json.dumps({"type": "result", "agentId": "awf00001", "result": {"ok": True}})])
write(f"{PROJ}/{SID_A}/workflows/wf_fix-001.json", [json.dumps({"status": "completed", "workflowName": "fixture-flow", "summary": "ok",
                                                                "startTime": int(t * 1000), "durationMs": 1000})])

# session B, held by a process since two hours ago: a long step, a long model call, an older process's
# agent, words written ahead of the rest of their message (no stop_reason), words that closed the turn
b_lines = [rec("user", "Start three agents", t, SID_B)]
b_lines += launch(SID_B, "toolu_step", "astep001", "Long step", t)
b_lines += launch(SID_B, "toolu_think", "athink01", "Long model call", t)
b_lines += launch(SID_B, "toolu_orph", "aorph001", "Earlier process", t)
b_lines += launch(SID_B, "toolu_text", "atext001", "Words first", t)
b_lines += launch(SID_B, "toolu_done", "adone001", "Finished", t)
write(f"{PROJ}/{SID_B}.jsonl", b_lines)
a, u = own(SID_B, t)
sub_agent(SID_B, "astep001", "toolu_step", "Long step", [u("Run the long build"),
    a(tool("b1", "Bash", {"command": "sleep 3000", "description": "Waiting for the build"}))], NOW - 1800)
sub_agent(SID_B, "athink01", "toolu_think", "Long model call", [u("Read it"), a(tool("r1", "Read", {"file_path": "/tmp/x"})),
    u(result("r1", "contents"))], NOW - 1800)
sub_agent(SID_B, "aorph001", "toolu_orph", "Earlier process", [u("Run it"), a(tool("b1", "Bash", {"command": "sleep 3000"}))], NOW - 3 * 3600)
sub_agent(SID_B, "atext001", "toolu_text", "Words first", [u("Write it"), a([{"type": "text", "text": "Writing the file now."}], stop_reason=None)], NOW - 1800)
sub_agent(SID_B, "adone001", "toolu_done", "Finished", [u("Say it"), a([{"type": "text", "text": "All done."}], stop_reason="end_turn")], NOW - 1800)
holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(900)"])
stat = open(f"/proc/{holder.pid}/stat").read()
proc_start = stat.rsplit(")", 1)[1].split()[19]
write(f"{HOME}/.claude/sessions/{holder.pid}.json", [json.dumps({"pid": holder.pid, "sessionId": SID_B, "cwd": CWD,
                                                                 "startedAt": int((NOW - 7200) * 1000), "procStart": proc_start, "kind": "interactive"})])

# sessions C1, C2: BIG_MB each of task-notifications, which the agents scan parses line by line
# (each with many small blocks after it, which make the parse cost more than reading the bytes)
small = [{"type": "x", "n": k} for k in range(100)]
for sid in (SID_C1, SID_C2):
    lines = [rec("user", "start", t, sid)]
    size, i = 0, 0
    while size < BIG_MB * 1024 * 1024:
        note = f"<task-notification>\n<task-id>t{i}</task-id>\n<status>completed</status>\n<summary>s</summary>\n</task-notification>"
        lines.append(rec("user", [{"type": "text", "text": note}] + small, t, sid))
        size += len(lines[-1]) + 1; i += 1
    write(f"{PROJ}/{sid}.jsonl", lines)

# --- the host
sock = f"{RT}/agent-host.sock"
env = dict(os.environ, HOME=HOME, XDG_RUNTIME_DIR=RT, AGENT_HOST_CACHE=CACHE, AGENT_HOST_IDLE="8")
host = subprocess.Popen(["nice", "-n", "10", BIN, "--socket", sock], env=env, stderr=open(f"{SB}/host.err", "w"))

class C:
    def __init__(s):
        s.k = socket.socket(socket.AF_UNIX); s.k.connect(sock); s.q = queue.Queue(); s.f = s.k.makefile(); s.tag = 0
        threading.Thread(target=s.rd, daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put(json.loads(l))
    def send(s, o): s.k.sendall((json.dumps(o) + "\n").encode())
    def until(s, pred, t=120):
        end = time.time() + t
        while time.time() < end:
            try: m = s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): return m
        raise SystemExit("TIMEOUT")
    def ask(s, o, t=120):
        s.tag += 1; tag = s.tag; o["tag"] = tag; s.send(o); return s.until(lambda m: m.get("tag") == tag, t)

def cpu():
    # the host's user + system time in seconds, all its threads
    f = open(f"/proc/{host.pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")

try:
    for _ in range(100):
        if os.path.exists(sock): break
        time.sleep(0.1)
    c = C(); c.until(lambda m: m.get("ev") == "hello")
    c.ask({"op": "hello", "proto": 5})
    t1 = time.time(); rows = c.ask({"op": "history"}, 300).get("sessions", [])
    print(f"index: {len(rows)} sessions in {time.time() - t1:.1f}s")

    # B1, B2, P7
    for f in ("afresh01", "achi0001", "achi0002"):
        os.utime(f"{PROJ}/{SID_A}/subagents/agent-{f}.jsonl")
    r = c.ask({"op": "agents", "id": SID_A})
    ag = {x["agent"]: x for x in r.get("agents", [])}
    so = ag.get("aso00001", {})
    check(so.get("state") == "done", f"B1 an agent that ended on StructuredOutput (no result, no notification, old file) is done: {so.get('state')}")
    d = c.ask({"op": "agent", "id": SID_A, "agent": "aso00001"})
    rep = d.get("report", "")
    try: parsed = json.loads(rep)
    except ValueError: parsed = None
    check(parsed == so_input and rep.startswith("{\n  ") and d.get("reportKind") == "json",
          f"B1 its report is the pretty JSON, reportKind {d.get('reportKind')!r}: {rep[:60]!r}")
    wf = next((w for w in r.get("workflows", []) if w["run"] == "wf_fix-001"), {})
    check(wf.get("state") == "done" and [m["state"] for m in wf.get("agents", [])] == ["done"], f"workflow run {wf.get('state')}, its agent {[m['state'] for m in wf.get('agents', [])]}")
    d = c.ask({"op": "agent", "id": SID_A, "agent": "awf00001"})
    rep = d.get("report", "")
    try: parsed = json.loads(rep)
    except ValueError: parsed = None
    check(parsed == wf_expect and d.get("reportKind") == "json", f"B1 workflow agent ({'real copy' if real else 'synthetic'}) reports its StructuredOutput: {len(rep)} chars, kind {d.get('reportKind')!r}")
    d = c.ask({"op": "agent", "id": SID_A, "agent": "abig0001"})
    check(d.get("report") == big and d.get("reportCut") is False, f"B2 44 000-char report whole: {len(d.get('report', ''))} chars, reportCut {d.get('reportCut')}")
    d = c.ask({"op": "agent", "id": SID_A, "agent": "ahuge001"})
    check(len(d.get("report", "")) == 200001 and d["report"].startswith(huge[:200000]) and d.get("reportCut") is True,
          f"B2 250 000-char report cut at 200 000: {len(d.get('report', ''))} chars, reportCut {d.get('reportCut')}")
    fr = ag.get("afresh01", {})
    check(fr.get("state") == "running" and (fr.get("last") or {}).get("desc") == "Checking the fixture", f"P7 running step: {fr.get('state')} last={fr.get('last')}")

    ch = ag.get("achi0001", {})
    check(ch.get("state") == "running" and (ch.get("last") or {}).get("desc") == "Building Child one",
          f"B3 an agent's background agent mid-step, launched but not ended, is running: {ch.get('state')} last={ch.get('last')}")
    check(ag.get("achi0002", {}).get("state") == "failed", f"B3 its parent's task notification ends it: {ag.get('achi0002', {}).get('state')}")

    # B3
    def states():
        r = c.ask({"op": "agents", "id": SID_B})
        return {x["agent"]: x["state"] for x in r.get("agents", [])}
    s = states()
    check(s.get("astep001") == "running", f"B3 held: a step written 30 min ago, unanswered, is running: {s.get('astep001')}")
    check(s.get("athink01") == "running", f"B3 held: waiting 30 min on its model is running: {s.get('athink01')}")
    check(s.get("aorph001") == "stopped", f"B3 held: an agent last written before the process started is stopped: {s.get('aorph001')}")
    check(s.get("atext001") == "running", f"B3 held: words ahead of the rest of their message (no stop_reason) are running: {s.get('atext001')}")
    check(s.get("adone001") == "done", f"B3 held: words that closed the turn are done: {s.get('adone001')}")
    holder.send_signal(signal.SIGTERM); holder.wait(10)
    s = states()
    check(s.get("astep001") == "stopped" and s.get("athink01") == "stopped", f"B3 not held: both stopped (120 s rule): {s.get('astep001')}, {s.get('athink01')}")

    # B16
    time.sleep(1)
    c0 = cpu(); t1 = time.time()
    one = c.ask({"op": "agents", "id": SID_C1}, 300)
    cpu1, wall1 = cpu() - c0, time.time() - t1
    time.sleep(1)
    c0 = cpu(); t1 = time.time()
    tags = []
    for _ in range(2):
        c.tag += 1; tags.append(c.tag); c.send({"op": "agents", "id": SID_C2, "tag": c.tag})
    got = {}
    while len(got) < 2:
        m = c.until(lambda m: m.get("tag") in tags, 300); got[m["tag"]] = m
    cpu2, wall2 = cpu() - c0, time.time() - t1
    strip = lambda m: {k: v for k, v in m.items() if k not in ("tag", "id")}
    print(f"B16 one cold request: cpu {cpu1:.2f}s wall {wall1:.2f}s; two concurrent cold requests: cpu {cpu2:.2f}s wall {wall2:.2f}s")
    check(cpu1 >= 0.1, f"B16 the cold read is measurable (cpu {cpu1:.2f}s; raise BIG_MB if not)")
    check(cpu2 < cpu1 * 1.5, f"B16 two concurrent cold requests cost one read: {cpu2 / max(cpu1, 0.01):.2f}x")
    check(strip(got[tags[0]]) == strip(got[tags[1]]), "B16 both get the same answer")
finally:
    host.send_signal(signal.SIGTERM)
    try: host.wait(10)
    except subprocess.TimeoutExpired: host.kill()
    if holder.poll() is None:
        holder.kill()
    err = open(f"{SB}/host.err").read().strip()
    if err:
        print("host stderr (last lines):\n  " + "\n  ".join(err.splitlines()[-5:]))
    if os.environ.get("KEEP") != "1":
        shutil.rmtree(SB, ignore_errors=True)

print("PASS" if not fails else f"FAIL ({len(fails)}): " + "; ".join(fails))
sys.exit(1 if fails else 0)
