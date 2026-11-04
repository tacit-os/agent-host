#!/usr/bin/env python3
"""Effort with the real agents (praxis-composer-accounts-plan §6.4), on a private agent-host in a scratch
folder, with one-line prompts:

    python3 tests/effort_e2e.py [--host target/release/agent-host]

- a Sonnet session started at effort low reports effort low in system/init and its summary
- the effort op, then get_settings (the settings op), shows effortLevel
- one Codex turn with effort, when Codex is signed in (skipped otherwise): its rollout records it
Prints the session ids it made.
"""
import argparse, atexit, glob, json, os, queue, shutil, socket, subprocess, sys, tempfile, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--host", default=os.path.join(HERE, "..", "target", "release", "agent-host"))
ARGS = ap.parse_args()
t0 = time.time()


def say(*a):
    print(f"{time.time() - t0:6.1f}s", *a, flush=True)


def check(cond, msg, *evidence):
    if not cond:
        say("FAIL:", msg)
        for e in evidence:
            print("   ", (e if isinstance(e, str) else json.dumps(e))[:1500])
        sys.exit(1)
    say("ok:", msg)


base = f"/tmp/claude-{os.getuid()}" if os.path.isdir(f"/tmp/claude-{os.getuid()}") else None
SB = tempfile.mkdtemp(prefix="effort-e2e-", dir=base)
RT = tempfile.mkdtemp(prefix="ee", dir=base)
os.chmod(RT, 0o700)
real_rt = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
for n in ("bus", "systemd"):
    if os.path.exists(os.path.join(real_rt, n)):
        os.symlink(os.path.join(real_rt, n), os.path.join(RT, n))
CWD = os.path.join(SB, "work")
os.makedirs(CWD)
SOCK = os.path.join(RT, "agent-host.sock")
env = dict(os.environ, XDG_RUNTIME_DIR=RT, AGENT_HOST_IDLE="8", AGENT_HOST_CACHE=os.path.join(SB, "cache"),
           AGENT_HOST_CONFIG=os.path.join(SB, "config"))
err = open(os.path.join(SB, "host.stderr"), "w")
host = subprocess.Popen(["nice", "-n", "10", os.path.abspath(ARGS.host), "--socket", SOCK], env=env, stdout=err, stderr=err)
atexit.register(lambda: host.poll() is None and (host.terminate(), host.wait(10)))
until = time.time() + 10
while True:
    try:
        socket.socket(socket.AF_UNIX).connect(SOCK)
        break
    except OSError:
        if time.time() > until:
            check(False, "host started")
        time.sleep(0.05)
made = []
quiet = open(os.path.join(RT, "notch-quiet-sessions"), "a")


class Conn:
    def __init__(self):
        self.k = socket.socket(socket.AF_UNIX)
        self.k.connect(SOCK)
        self.q, self.tag, self.back = queue.Queue(), 0, []
        threading.Thread(target=self.read, daemon=True).start()
        self.hello = self.wait(lambda m: m.get("ev") == "hello")

    def read(self):
        for line in self.k.makefile():
            self.q.put(json.loads(line))

    def ask(self, o, t=60):
        self.tag += 1
        o = dict(o, tag=self.tag)
        self.k.sendall((json.dumps(o) + "\n").encode())
        return self.wait(lambda m: m.get("tag") == o["tag"], t, keep=True)

    def wait(self, pred, t=60, keep=False):
        kept = []
        while self.back:
            m = self.back.pop(0)
            if pred(m):
                self.back = kept + self.back
                return m
            if keep:
                kept.append(m)
        end = time.time() + t
        while time.time() < end:
            try:
                m = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            if pred(m):
                self.back = kept
                return m
            if keep:
                kept.append(m)
        check(False, "timed out waiting")


def event(sid, kind, sub=None):
    return lambda m: m.get("ev") == "event" and m.get("id") == sid and m["data"].get("type") == kind and (sub is None or m["data"].get("subtype") == sub)


c = Conn()
c.ask({"op": "hello", "proto": 6})
check("effort" in c.hello.get("features", []), "hello names the effort feature", c.hello.get("features"))

# ── Claude Code ──
r = c.ask({"op": "start", "cwd": CWD, "agent": "claude", "model": "sonnet", "effort": "low", "mode": "default", "text": "Reply with just: ok"})
check(r.get("ev") == "started", "a Sonnet session starts at effort low", r)
sid = r["id"]
made.append(("claude", sid))
quiet.write(sid + "\n"); quiet.flush()
c.ask({"op": "watch", "id": sid, "from": 0})
init = c.wait(event(sid, "system", "init"), 90)
res = c.wait(event(sid, "result"), 120)
say("result:", repr(res["data"].get("result"))[:60])
# Claude Code 2.1.289's headless system/init carries no effort field; get_settings' applied effort
# is then what shows the start's --effort took
before = c.ask({"op": "settings", "id": sid})["response"].get("applied") or {}
if "effort" in init["data"]:
    check(init["data"]["effort"] == "low", "system/init reports effort low", {k: init["data"].get(k) for k in ("model", "effort")})
else:
    say("note: system/init has no effort field", sorted(init["data"]))
check(before.get("effort") == "low", "get_settings: the session runs at effort low", before)
s = next(x for x in c.ask({"op": "list"})["sessions"] if x["id"] == sid)
check(s["effort"] == "low" and "sonnet" in s["model"], "the summary carries the model and effort", {k: s[k] for k in ("model", "effort", "account")})
r = c.ask({"op": "effort", "id": sid, "effort": "high"})
check(r.get("ev") == "ok", "the effort op", r)
r = c.ask({"op": "settings", "id": sid})
check(r.get("ev") == "control", "get_settings answers", r)
resp = r["response"]
levels = [v for k, v in (resp.get("effective") or {}).items() if k == "effortLevel"]
check(levels == ["high"] or (resp.get("applied") or {}).get("effort") == "high",
      "get_settings shows effortLevel high after the effort op", {"effective.effortLevel": levels, "applied": resp.get("applied")})
c.ask({"op": "close", "id": sid})

# ── Codex ──
codex = c.hello["agents"].get("codex", {})
if codex.get("signedIn") is not True:
    say("skip: Codex is not signed in here", codex.get("reason"))
else:
    r = c.ask({"op": "start", "cwd": CWD, "agent": "codex", "effort": "low", "mode": "default", "text": "Reply with just: ok"}, 120)
    check(r.get("ev") == "started", "a Codex thread starts with effort low", r)
    tid = r["id"]
    made.append(("codex", tid))
    quiet.write(tid + "\n"); quiet.flush()
    c.ask({"op": "watch", "id": tid, "from": 0})
    res = c.wait(event(tid, "result"), 180)
    say("result:", repr(res["data"].get("result"))[:60])
    home = os.environ.get("CODEX_HOME", os.path.expanduser("~/.codex"))
    files = glob.glob(os.path.join(home, "sessions", "**", f"*{tid}*.jsonl"), recursive=True)
    efforts = []
    for f in files:
        for line in open(f):
            if '"effort"' in line:
                v = json.loads(line)
                p = v.get("payload", {})
                if p.get("effort") is not None:
                    efforts.append(p["effort"])
    check("low" in efforts, "Codex's rollout records the turn's effort low", efforts, files)
    c.ask({"op": "close", "id": tid})

host.terminate()
host.wait(10)
err.close()
say("all passed; sessions made:", made)
shutil.rmtree(RT, ignore_errors=True)
