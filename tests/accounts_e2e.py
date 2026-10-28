#!/usr/bin/env python3
"""Accounts, keys, defaults, sign-ins and installs (praxis-composer-accounts-plan §6.4) against private
agent-hosts in a scratch HOME, with every agent CLI, secret-tool, busctl and curl faked:

    python3 tests/accounts_e2e.py [--host target/release/agent-host] [--keep]

Host A has all four agents (tests/fakes plus wrappers written here) and runs the sign-in, key,
default and session checks; host B has no Node and no pi and runs the installs. It signs nothing in,
installs nothing and touches no keyring: the fake secret-tool keeps values in a scratch file. Each
check prints a line; the first failure exits 1 with the evidence.
"""
import argparse, atexit, json, os, queue, shutil, socket, stat, subprocess, sys, tempfile, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
FAKES = os.path.join(HERE, "fakes")
ap = argparse.ArgumentParser()
ap.add_argument("--host", default=os.path.join(HERE, "..", "target", "release", "agent-host"))
ap.add_argument("--keep", action="store_true", help="leave the scratch folder")
ARGS = ap.parse_args()
HOST = os.path.abspath(ARGS.host)

# the values no output may carry; the "bad" ones are refused by the fake curl
GOOD_ANTHROPIC = "sk-ant-api03-GOODgoodGOODgood0123456789abcdWXYZ"
BAD_ANTHROPIC = "sk-ant-api03-badBADbadBAD0123456789abcdefQRST"
GOOD_OPENROUTER = "sk-or-v1-0123456789abcdef0123456789abcdefOROR"
GOOD_OPENAI = "sk-proj-0123456789abcdefABCDEFopenaiKEY77"
ROUTER_KEY_LATER = "sk-or-v1-fedcba9876543210fedcba9876543210LATR"
LATER_REFUSED = GOOD_ANTHROPIC.replace("GOOD", "Goob")
# what a permission asks to write: key-shaped text the redactor masks for clients, at a key-shaped path
PERM_TEXT = "Authorization: Bearer abcdefghijklmnop0123456789\nexample = sk-test-0123456789abcdefghijklmnop\n"
PERM_FILE = "sk-learn-preprocessing-pipeline-notes.txt"
HOST_ENV_LEAKS = {"OPENAI_API_KEY": "sk-proj-HOSTLEAKhostleak0123456789abcd", "ANTHROPIC_AUTH_TOKEN": "host-auth-token-leak-0123456789",
                  "CLAUDE_CODE_OAUTH_TOKEN": "host-oauth-token-leak-0123456789", "OPENCODE_AUTH_CONTENT": "host-opencode-auth-leak-01234",
                  "SOMETHING_API_KEY": "host-something-leak-0123456789", "CLAUDE_CONFIG_DIR": "/nonexistent/claude",
                  "CODEX_HOME": "/nonexistent/codex", "PI_CODING_AGENT_DIR": "/nonexistent/pi", "PI_CODING_AGENT_SESSION_DIR": "/nonexistent/pis"}
SECRETS = [GOOD_ANTHROPIC, BAD_ANTHROPIC, GOOD_OPENROUTER, GOOD_OPENAI, ROUTER_KEY_LATER, LATER_REFUSED] + \
          [v for k, v in HOST_ENV_LEAKS.items() if not k.endswith(("DIR", "HOME"))]

t0 = time.time()


def say(*a):
    print(f"{time.time() - t0:6.1f}s", *a, flush=True)


def fail(msg, *evidence):
    say("FAIL:", msg)
    for e in evidence:
        print("   ", json.dumps(e)[:2000] if not isinstance(e, str) else e[:2000])
    sys.exit(1)


def check(cond, msg, *evidence):
    if not cond:
        fail(msg, *evidence)
    say("ok:", msg)


# ── scratch ──────────────────────────────────────────────────────────────────────────────────
base = f"/tmp/claude-{os.getuid()}" if os.path.isdir(f"/tmp/claude-{os.getuid()}") else None
SB = tempfile.mkdtemp(prefix="accounts-e2e-", dir=base)
RT = tempfile.mkdtemp(prefix="ae", dir=base)   # short: a unix socket path is at most 107 bytes
os.chmod(RT, 0o700)
real_rt = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
for n in ("bus", "systemd"):   # systemd-run --user finds the user manager here
    if os.path.exists(os.path.join(real_rt, n)):
        os.symlink(os.path.join(real_rt, n), os.path.join(RT, n))
HOME, OUT, BIN, CACHE = (os.path.join(SB, d) for d in ("home", "out", "bin", "cache"))
for d in (HOME, OUT, BIN, CACHE, os.path.join(HOME, ".claude/projects"), os.path.join(HOME, ".claude/skills"),
          os.path.join(HOME, ".codex/sessions"), os.path.join(HOME, ".pi/agent")):
    os.makedirs(d, exist_ok=True)
for f, text in ((".claude/settings.json", "{}"), (".claude/CLAUDE.md", "# personal\n"), (".codex/config.toml", 'model = "x"\n'),
                (".pi/agent/settings.json", "{}")):
    open(os.path.join(HOME, f), "w").write(text)
KEYRING, KEYSTATE = os.path.join(SB, "keyring.json"), os.path.join(SB, "keyring-state")
open(KEYSTATE, "w").write("open")


def write_exe(path, text):
    open(path, "w").write(text)
    os.chmod(path, 0o755)


def mode(agent, m):
    open(os.path.join(SB, f"mode-{agent}"), "w").write(m)


# claude: sessions (-p) here, everything else to tests/fakes/bin/claude with the mode file's FAKE_MODE
write_exe(os.path.join(BIN, "claude"), f'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if "-p" not in args:
    try: os.environ["FAKE_MODE"] = open("{SB}/mode-claude").read().strip()
    except OSError: pass
    os.execv("{FAKES}/bin/claude", ["{FAKES}/bin/claude"] + args)
out = os.environ["FAKE_OUT"]
sid = next((args[i + 1] for i, a in enumerate(args) if a in ("--session-id", "--resume")), "probe")
rec = {{"pid": os.getpid(), "env": sorted(os.environ), "args": args, "cwd": os.getcwd(),
       "config": os.environ.get("CLAUDE_CONFIG_DIR")}}
key = None
fd = os.environ.get("CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR")
if fd:
    try:
        key = os.read(int(fd), 4096).decode()
        rec["fd3"] = len(key)
    except OSError as e:
        rec["fd3err"] = str(e)
json.dump(rec, open(f"{{out}}/session-{{sid}}.json", "w"))
# a careless agent: every key it holds goes to stderr
for v in ([key] if key else []) + [v for k, v in os.environ.items() if k.endswith(("_API_KEY", "_TOKEN"))]:
    print("debug: using key " + v, file=sys.stderr, flush=True)
effort = args[args.index("--effort") + 1] if "--effort" in args else None
settings = {{}}
def send(o):
    sys.stdout.write(json.dumps(o) + "\\n")
    sys.stdout.flush()
for line in sys.stdin:
    m = json.loads(line)
    if m.get("type") == "control_request":
        r, resp = m["request"], {{}}
        if r.get("subtype") == "initialize":
            resp = {{"commands": [{{"name": "fake-cmd", "description": "A fake command", "argumentHint": ""}}],
                    "models": [{{"value": "default", "displayName": "Default", "description": "Fake default"}}]}}
        elif r.get("subtype") == "apply_flag_settings":
            settings.update(r.get("settings", {{}}))
        elif r.get("subtype") == "get_settings":
            resp = {{"effective": dict(settings), "applied": {{"effort": settings.get("effortLevel", effort)}}}}
        send({{"type": "control_response", "response": {{"subtype": "success", "request_id": m["request_id"], "response": resp}}}})
    elif m.get("type") == "control_response" and m["response"].get("request_id") == "perm-write":
        r = m["response"]["response"]
        if r.get("behavior") == "allow":
            open(r["updatedInput"]["file_path"], "w").write(r["updatedInput"]["content"])
        send({{"type": "result", "subtype": "success", "is_error": False, "result": "written", "session_id": sid}})
    elif m.get("type") == "user":
        init = {{"type": "system", "subtype": "init", "session_id": sid, "model": "fake-model", "permissionMode": "default"}}
        if effort:
            init["effort"] = effort
        send(init)
        said = m["message"]["content"]
        said = said if isinstance(said, str) else " ".join(c.get("text", "") for c in said)
        if said.startswith("write"):
            send({{"type": "control_request", "request_id": "perm-write", "request": {{"subtype": "can_use_tool", "tool_name": "Write",
                  "input": {{"file_path": os.path.join(os.getcwd(), {json.dumps(PERM_FILE)}), "content": {json.dumps(PERM_TEXT)}}},
                  "permission_suggestions": []}}}})
            continue
        if key is not None and "bad" in key.lower():
            send({{"type": "result", "subtype": "success", "is_error": True, "result": "Invalid API key · Please run /login", "session_id": sid}})
            continue
        text = "I hold " + key if key else "hello"
        send({{"type": "assistant", "message": {{"role": "assistant", "content": [{{"type": "text", "text": text}}]}}, "parent_tool_use_id": None, "session_id": sid}})
        send({{"type": "result", "subtype": "success", "is_error": False, "result": text, "session_id": sid}})
''')
for agent in ("codex", "opencode"):
    write_exe(os.path.join(BIN, agent), f'''#!/bin/sh
env | cut -d= -f1 > "$FAKE_OUT/{agent}.env.$$"
FAKE_MODE=$(cat "{SB}/mode-{agent}" 2>/dev/null || echo ok) exec "{FAKES}/bin/{agent}" "$@"
''')
os.symlink(os.path.join(FAKES, "pi-pkg/dist/bundle/cli.js"), os.path.join(BIN, "pi"))
os.symlink(os.path.join(FAKES, "bin/xdg-open"), os.path.join(BIN, "xdg-open"))
write_exe(os.path.join(BIN, "secret-tool"), f'''#!/usr/bin/env python3
import json, sys
db = "{KEYRING}"
a = sys.argv[1:]
open("{OUT}/secret-tool.calls", "a").write(json.dumps(a) + "\\n")
try: items = json.load(open(db))
except OSError: items = {{}}
attrs = a[3:] if a[:1] == ["store"] else a[1:]
want = dict(zip(attrs[::2], attrs[1::2]))
if a[0] == "store":
    items[want["key"]] = {{"attrs": want, "label": a[2], "value": sys.stdin.read()}}
    json.dump(items, open(db, "w"))
elif a[0] == "lookup":
    hit = [i for i in items.values() if all(i["attrs"].get(k) == v for k, v in want.items())]
    if not hit: sys.exit(1)
    sys.stdout.write(hit[0]["value"])
elif a[0] == "clear":
    items = {{k: i for k, i in items.items() if not all(i["attrs"].get(k2) == v for k2, v in want.items())}}
    json.dump(items, open(db, "w"))
else:
    sys.exit(3)
''')
write_exe(os.path.join(BIN, "busctl"), f'''#!/bin/sh
case "$(cat "{KEYSTATE}")" in open) echo "b false" ;; locked) echo "b true" ;; *) echo "Failed to get property" >&2; exit 1 ;; esac
''')
write_exe(os.path.join(BIN, "curl"), f'''#!/usr/bin/env python3
import json, sys
a = sys.argv[1:]
cfg = sys.stdin.read() if a == ["--config", "-"] else ""
url = next((l.split('"')[1] for l in cfg.splitlines() if l.startswith("url")), "")
open("{OUT}/curl.calls", "a").write(json.dumps({{"argv": a, "url": url}}) + "\\n")
key = " ".join(l for l in cfg.splitlines() if l.startswith("header"))
print("401" if "bad" in key.lower() else "200")
''')

PATH_A = f"{BIN}:/usr/bin:/bin"
BASE_ENV = {k: v for k, v in os.environ.items() if not k.endswith(("_API_KEY", "_TOKEN")) and k not in HOST_ENV_LEAKS}
BASE_ENV.update(HOME=HOME, XDG_RUNTIME_DIR=RT, AGENT_HOST_CACHE=CACHE, AGENT_HOST_CONFIG=os.path.join(SB, "config"),
                AGENT_HOST_IDLE="8", FAKE_OUT=OUT, FAKE_PI_AUTH="openai-codex:oauth", PATH=PATH_A)
for k in ("XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "FAKE_MODE"):
    BASE_ENV.pop(k, None)


HOSTS = []


@atexit.register
def stop_hosts():   # also after a failure: no private host outlives the test
    for h in HOSTS:
        if h.p.poll() is None:
            h.p.terminate()
            h.p.wait(10)


class Host:
    def __init__(self, name, env):
        self.sock = os.path.join(RT, "agent-host.sock")
        if os.path.exists(self.sock):
            os.unlink(self.sock)
        self.err = open(os.path.join(SB, f"host-{name}.stderr"), "w")
        self.p = subprocess.Popen(["nice", "-n", "10", HOST, "--socket", self.sock], env=env, stdout=self.err, stderr=self.err)
        HOSTS.append(self)
        until = time.time() + 10
        while True:
            try:
                socket.socket(socket.AF_UNIX).connect(self.sock)
                break
            except OSError:
                if time.time() > until:
                    fail("host did not start")
                time.sleep(0.05)

    def stop(self):
        self.p.terminate()
        self.p.wait(10)
        self.err.close()


SEEN = []   # every line any client got


class Conn:
    def __init__(self, host):
        self.k = socket.socket(socket.AF_UNIX)
        self.k.connect(host.sock)
        self.q, self.got, self.tag, self.back = queue.Queue(), [], 0, []
        threading.Thread(target=self.read, daemon=True).start()
        self.hello = self.wait(lambda m: m.get("ev") == "hello", 15)

    def read(self):
        for line in self.k.makefile():
            SEEN.append(line)
            m = json.loads(line)
            self.got.append(m)
            self.q.put(m)

    def send(self, o):
        self.k.sendall((json.dumps(o) + "\n").encode())

    # The reply; pushes that came before it are kept for the next wait.
    def ask(self, o, t=30):
        self.tag += 1
        o = dict(o, tag=self.tag)
        self.send(o)
        return self.wait(lambda m: m.get("tag") == o["tag"], t, o, keep=True)

    # The first message (kept ones first) that `pred` takes; the others before it are dropped,
    # or kept with `keep`.
    def wait(self, pred, t=30, what=None, keep=False):
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
        fail(f"timed out waiting ({what})" if what else "timed out waiting", *[json.dumps(x)[:300] for x in self.got[-8:]])

    def drain(self):
        self.back = []
        while not self.q.empty():
            self.q.get()

    def close(self):
        self.k.close()


def acct(state, aid):
    return next((a for a in state["accounts"] if a["id"] == aid), None)


def settled(w, ids, t=40):
    """The accounts state once none of `ids` reads checking."""
    end = time.time() + t
    while time.time() < end:
        st = w.ask({"op": "accounts"})
        if all(acct(st, i) and acct(st, i)["state"] != "checking" for i in ids):
            return st
        time.sleep(0.3)
    fail("accounts still checking", st)


def steps(w, aid, until=("done", "failed", "cancelled", "busy"), t=30):
    """The signin pushes for `aid` up to and including the last."""
    out = []
    while True:
        m = w.wait(lambda m: m.get("ev") == "signin" and m.get("account") == aid, t)
        out.append(m)
        if m["step"] in until:
            return out


def session_file(sid, t=15):
    p = os.path.join(OUT, f"session-{sid}.json")
    end = time.time() + t
    while time.time() < end:
        if os.path.exists(p):
            try:
                return json.load(open(p))
            except ValueError:
                pass
        time.sleep(0.1)
    fail(f"the fake claude for {sid} never started")


def gone(pid):
    try:
        return open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        return True


def env_file(agent, since):
    files = [f for f in os.listdir(OUT) if f.startswith(f"{agent}.env.") and os.path.getmtime(os.path.join(OUT, f)) >= since]
    return set().union(*[open(os.path.join(OUT, f)).read().split() for f in files]) if files else set()


SCRUBBED = {"OPENAI_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "OPENCODE_AUTH_CONTENT", "SOMETHING_API_KEY",
            "PI_CODING_AGENT_DIR", "PI_CODING_AGENT_SESSION_DIR", "CODEX_HOME"}
quiet = open(os.path.join(RT, "notch-quiet-sessions"), "a")

# ═══ host A ══════════════════════════════════════════════════════════════════════════════════
say("scratch", SB)
host = Host("a", dict(BASE_ENV, **HOST_ENV_LEAKS))
w = Conn(host)                      # watches accounts
old = Conn(host)                    # an old-style client: never says watch
check(w.hello.get("proto") == 6 and w.hello.get("features") == ["accounts", "catalog", "effort"], "hello: protocol 6 with features", w.hello)
w.send({"op": "hello", "proto": 6})
st = w.ask({"op": "accounts", "watch": True})
check([a["id"] for a in st["accounts"]] == ["claude-personal", "codex-personal", "pi-personal", "opencode-personal"],
      "four Personal accounts", st["accounts"])
st = settled(w, ["claude-personal", "codex-personal", "pi-personal", "opencode-personal"])
check(st["keyring"] == "open" and acct(st, "claude-personal")["state"] == "signedOut" and acct(st, "codex-personal")["state"] == "signedOut"
      and acct(st, "pi-personal")["state"] == "ready" and acct(st, "opencode-personal")["state"] == "ready",
      "states read through each agent (claude/codex signed out, pi/opencode ready)", [(a["id"], a["state"]) for a in st["accounts"]])
check(any(f["agent"] == "opencode" and f["provider"] == "OpenRouter" for f in st["found"]), "a key in opencode's own file is listed under found", st["found"])

# ── keys ──
r = w.ask({"op": "key_set", "provider": "anthropic", "label": "Anthropic key (Work)", "secret": BAD_ANTHROPIC})
check(r.get("state") == "refused" and r.get("id") is None, "a refused key is not stored", r)
check(not os.path.exists(KEYRING) or BAD_ANTHROPIC not in open(KEYRING).read(), "the refused key never reached the keyring")
r = w.ask({"op": "key_set", "provider": "anthropic", "label": "Anthropic key (Work)", "secret": GOOD_ANTHROPIC})
check(r.get("state") == "ok" and r.get("last4") == GOOD_ANTHROPIC[-4:] and GOOD_ANTHROPIC not in json.dumps(r), "key_set stores and answers last4 only", r)
k_ant = r["id"]
item = json.load(open(KEYRING))[k_ant]
check(item["value"] == GOOD_ANTHROPIC and item["attrs"] == {"application": "tacit-agents", "kind": "api-key", "provider": "anthropic", "key": k_ant}
      and item["label"] == "Tacit: Anthropic key (Work)", "one keyring item, value from stdin, the plan's attributes", item["attrs"])
k_or = w.ask({"op": "key_set", "provider": "openrouter", "secret": GOOD_OPENROUTER})["id"]
k_oa = w.ask({"op": "key_set", "provider": "openai", "secret": GOOD_OPENAI})["id"]
calls = [json.loads(l) for l in open(os.path.join(OUT, "secret-tool.calls"))]
check(all(c[0] in ("store", "lookup", "clear") for c in calls) and not any(s in json.dumps(calls) for s in SECRETS),
      "secret-tool: store/lookup/clear only, never search, no secret in argv", calls)
curls = [json.loads(l) for l in open(os.path.join(OUT, "curl.calls"))]
check(all(c["argv"] == ["--config", "-"] for c in curls) and not any("key=" in c["url"] for c in curls)
      and {c["url"] for c in curls} >= {"https://api.anthropic.com/v1/models", "https://openrouter.ai/api/v1/key", "https://api.openai.com/v1/models"},
      "key checks: curl --config - with the header on stdin", curls)
r = w.ask({"op": "key_check", "id": k_or})
check(r.get("ev") == "key" and r.get("state") == "ok", "key_check", r)

# ── a Claude key account: the key on fd 3 through systemd-run --scope, nothing in env or argv ──
r = w.ask({"op": "account_add", "agent": "claude", "label": "Work", "use": "key", "key": k_ant})
check(r.get("ev") == "account" and r.get("id") == "claude-work", "account_add (key)", r)
st = w.ask({"op": "accounts"})
check(acct(st, "claude-work")["state"] == "ready" and acct(st, "claude-work")["key"] == k_ant, "the key account is ready", acct(st, "claude-work"))
work_home = os.path.join(HOME, ".local/share/agent-host/accounts/claude-work")
check(stat.S_IMODE(os.stat(work_home).st_mode) == 0o700 and os.readlink(os.path.join(work_home, "settings.json")) == os.path.join(HOME, ".claude/settings.json")
      and os.path.islink(os.path.join(work_home, "projects")) and not os.path.exists(os.path.join(work_home, "agents")),
      "its folder: 0700, Personal's settings, CLAUDE.md, skills and projects linked in (those that exist)", sorted(os.listdir(work_home)))
cwd = os.path.join(SB, "work")
os.makedirs(os.path.join(cwd, "sub"))
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "account": "claude-work", "text": "hi"})
sid = r["id"]
quiet.write(sid + "\n"); quiet.flush()
w.send({"op": "watch", "id": sid, "from": 0})
rec = session_file(sid)
check(rec.get("fd3") == len(GOOD_ANTHROPIC), "the fake claude read the whole key on fd 3 under systemd-run --scope", rec.get("fd3err", ""))
check("CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR" in rec["env"] and not any(k.endswith("_API_KEY") or k in SCRUBBED for k in rec["env"])
      and rec["config"] == work_home, "its env: the fd variable and CLAUDE_CONFIG_DIR, no host key or account variable",
      [k for k in rec["env"] if k.endswith(("_KEY", "_TOKEN", "_DIR", "HOME", "CONTENT"))])
cmdline = open(f"/proc/{rec['pid']}/cmdline", "rb").read().decode(errors="replace")
check(not any(s in cmdline for s in SECRETS), "no secret in /proc/<child>/cmdline", cmdline.replace("\0", " "))
res = w.wait(lambda m: m.get("ev") == "event" and m.get("id") == sid and m["data"].get("type") == "result")
check(res["data"]["result"] == "I hold ••••" + GOOD_ANTHROPIC[-4:], "a key the agent prints is masked in relayed events", res["data"]["result"])
summ = w.ask({"op": "list"})
s = next(x for x in summ["sessions"] if x["id"] == sid)
check(s["account"] == "claude-work", "the session summary names its account", s)

# a refused key ends the session as signed out and marks the key refused
k_bad_later = w.ask({"op": "key_set", "provider": "anthropic", "secret": LATER_REFUSED})["id"]
items = json.load(open(KEYRING))
items[k_bad_later]["value"] = BAD_ANTHROPIC          # the provider stops taking it after it was saved
json.dump(items, open(KEYRING, "w"))
w.ask({"op": "account_add", "agent": "claude", "label": "Old", "use": "key", "key": k_bad_later})
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "account": "claude-old", "text": "hi"})
quiet.write(r["id"] + "\n"); quiet.flush()
ex = w.wait(lambda m: m.get("ev") == "exit" and m.get("id") == r["id"], 20)
check(ex["reason"] == "signedOut", "an auth failure in the result: exit.reason signedOut", ex)
st = w.ask({"op": "accounts"})
check(acct(st, "claude-old")["state"] == "keyRefused", "that account's key is marked refused", acct(st, "claude-old"))

# ── the scrub on a Personal session ──
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "text": "hi"})
quiet.write(r["id"] + "\n"); quiet.flush()
rec = session_file(r["id"])
check(not any(k.endswith("_API_KEY") or k in SCRUBBED or k == "CLAUDE_CONFIG_DIR" or k == "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR" for k in rec["env"]),
      "Personal: every *_API_KEY and account variable of the host's env is gone", [k for k in rec["env"] if "KEY" in k or "TOKEN" in k or "DIR" in k])

# ── a permission allowed with no input (Praxis's card, the notch): the agent gets its own input ──
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "text": "write the notes"})
quiet.write(r["id"] + "\n"); quiet.flush()
w.send({"op": "watch", "id": r["id"], "from": 0})
ask = w.wait(lambda m: m.get("ev") == "event" and m.get("id") == r["id"] and m["data"].get("type") == "control_request", 15)
shown = ask["data"]["request"]["input"]
check(PERM_FILE not in json.dumps(ask) and "abcdefghijklmnop" not in json.dumps(ask) and "••••" in shown["content"],
      "the permission request reaches clients masked", shown)
ans = w.ask({"op": "answer", "id": r["id"], "request": ask["data"]["request_id"], "allow": True})
w.wait(lambda m: m.get("ev") == "event" and m.get("id") == r["id"] and m["data"].get("type") == "result", 15)
written = os.path.join(cwd, PERM_FILE)
check(ans.get("ev") == "ok" and os.path.exists(written) and open(written).read() == PERM_TEXT,
      "allowed with no input: the agent writes its own text at its own path, not the masked copy", ans, sorted(os.listdir(cwd)))
os.remove(written)

# ── resolution: explicit > nearest folder (Home exact only) > everywhere > the agent's own ──
w.ask({"op": "defaults_set", "scope": "everywhere", "agent": "claude", "run": {"claude": {"model": "opus", "effort": "high"}}})
w.ask({"op": "defaults_set", "scope": cwd, "run": {"claude": {"account": "claude-work", "effort": "low"}}})
w.ask({"op": "defaults_set", "scope": HOME, "mode": "plan"})
r = w.ask({"op": "resolve", "cwd": os.path.join(cwd, "sub")})
check((r["agent"], r["account"], r["model"], r["effort"], r["mode"]) == ("claude", "claude-work", "opus", "low", None)
      and r["from"] == {"agent": "everywhere", "account": f"folder:{cwd}", "model": "everywhere", "effort": f"folder:{cwd}", "mode": "agent"},
      "resolve: folder over everywhere, Home's mode not inherited below Home", r)
r = w.ask({"op": "resolve", "cwd": HOME})
check(r["mode"] == "plan" and r["from"]["mode"] == f"folder:{HOME}", "resolve: Home on an exact match", r)
r = w.ask({"op": "resolve", "cwd": os.path.join(SB, "elsewhere"), "agent": "codex"})
check(r["from"]["agent"] == "explicit" and r["account"] == "codex-personal" and r["from"]["account"] == "agent" and r["model"] is None,
      "resolve: an explicit agent, its own account and model", r)
check(acct(w.ask({"op": "accounts"}), "claude-work")["folders"] == [cwd], "the account lists the folder that defaults to it")
r = w.ask({"op": "start", "cwd": os.path.join(cwd, "sub"), "text": "hi"})          # the notch's shape: no agent
quiet.write(r["id"] + "\n"); quiet.flush()
rec = session_file(r["id"])
a = rec["args"]
check(r["tool"] == "claude" and rec["config"] == work_home and a[a.index("--effort") + 1] == "low" and a[a.index("--model") + 1] == "opus"
      and "--permission-mode" not in a, "a start with no agent: Claude, with the folder's account and effort and everywhere's model", a)
r = w.ask({"op": "start", "cwd": os.path.join(cwd, "sub"), "text": "hi", "mode": "auto", "effort": "max", "account": "claude-personal"})
quiet.write(r["id"] + "\n"); quiet.flush()
rec = session_file(r["id"])
a = rec["args"]
check(a[a.index("--permission-mode") + 1] == "auto" and a[a.index("--effort") + 1] == "max" and rec["config"] is None,
      "explicit fields win (mode auto, effort, Personal)", a)
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "account": "codex-personal"})
check(r.get("ev") == "error", "a start with another agent's account is refused", r)

# ── the effort op and the settings read ──
w.ask({"op": "effort", "id": sid, "effort": "high"})
r = w.ask({"op": "settings", "id": sid})
check(r.get("ev") == "control" and r["response"]["effective"].get("effortLevel") == "high", "effort: apply_flag_settings, read back by get_settings", r)
summ = w.ask({"op": "list"})
check(next(x for x in summ["sessions"] if x["id"] == sid)["effort"] == "high", "the summary carries the effort")

# ── Codex key account: CODEX_API_KEY in its app-server's env only ──
w.ask({"op": "account_add", "agent": "codex", "label": "API", "use": "key", "key": k_oa})
since = time.time()
r = w.ask({"op": "start", "cwd": cwd, "agent": "codex", "account": "codex-api", "text": "hi"}, 40)
names = env_file("codex", since - 1)
check("CODEX_API_KEY" in names and not ({"OPENAI_API_KEY", "SOMETHING_API_KEY", "ANTHROPIC_AUTH_TOKEN"} & names),
      "a Codex key account's app-server gets CODEX_API_KEY and none of the host's keys", sorted(n for n in names if "KEY" in n or "TOKEN" in n))
codex_pid = open(os.path.join(OUT, "codex.pid")).read().strip()
try:
    cl = open(f"/proc/{codex_pid}/cmdline", "rb").read().decode(errors="replace")
    check(not any(s in cl for s in SECRETS), "no secret in the app-server's cmdline", cl.replace("\0", " "))
except OSError:
    say("note: the codex app-server had already exited")

# ── pi and opencode: keys by their "Use with" list ──
w.ask({"op": "key_use", "id": k_or, "agent": "opencode", "on": True})
since = time.time()
w.ask({"op": "account_check", "account": "opencode-personal"})
time.sleep(2)
names = env_file("opencode", since - 0.5)
check("OPENROUTER_API_KEY" in names and not ({"OPENAI_API_KEY", "SOMETHING_API_KEY"} & names), "opencode gets the OpenRouter key it is set to use", sorted(n for n in names if "KEY" in n))
r = w.ask({"op": "key_remove", "id": k_ant})
check(r.get("ev") == "error" and r.get("usedBy") == ["claude-work"], "key_remove of a key in use names its accounts", r)

# ── pi: a key in its own file does not make it ready (folders plan Q9); one given here does ──
# pi for this part: its sessions note their env and wait; the rest is the fake with FAKE_PI_AUTH from a file
PI = os.path.join(BIN, "pi")
os.remove(PI)
write_exe(PI, f'''#!/bin/sh
case " $* " in *" --mode rpc "*) env | cut -d= -f1 > "$FAKE_OUT/pi.env.$$"; exec cat > /dev/null ;; esac
FAKE_PI_AUTH=$(cat "{SB}/pi-auth") exec "{FAKES}/pi-pkg/dist/bundle/cli.js" "$@"
''')
open(os.path.join(SB, "pi-auth"), "w").write("openrouter:api_key")


def pi_read(pred, what):
    w.ask({"op": "account_check", "account": "pi-personal"})
    return w.wait(lambda m: m.get("ev") == "accounts" and acct(m, "pi-personal")["state"] != "checking" and pred(acct(m, "pi-personal")), 30, what)


pa = acct(pi_read(lambda a: a["providers"] == [{"id": "openrouter", "via": "file", "state": "ready"}], "pi with a file key"), "pi-personal")
check(pa["state"] == "noProvider", "pi with only a key in its own file: noProvider (the hello's \"Needs a provider\")", pa)
w.ask({"op": "key_use", "id": k_or, "agent": "pi", "on": True})
pa = acct(pi_read(lambda a: any(p["via"] == "key" for p in a["providers"]), "pi with a key from here"), "pi-personal")
check(pa["state"] == "ready", "pi given a key here: ready", pa)

# ── a locked keyring ──
open(KEYSTATE, "w").write("locked")
since = time.time()
r = w.ask({"op": "start", "cwd": cwd, "agent": "pi"})
check(r.get("ev") == "started", "locked: a pi start with a \"Use with\" key still starts, without that key", r)
quiet.write(r["id"] + "\n"); quiet.flush()
end = time.time() + 15
while not env_file("pi", since - 0.5) and time.time() < end:
    time.sleep(0.2)
names = env_file("pi", since - 0.5)
check(names and "OPENROUTER_API_KEY" not in names, "its env holds no OpenRouter key", sorted(n for n in names if "KEY" in n))
w.ask({"op": "close", "id": r["id"]})
r = w.ask({"op": "key_set", "provider": "openrouter", "secret": ROUTER_KEY_LATER})
check(r.get("ev") == "error" and "locked" in r["message"], "key_set with the keyring locked says so", r)
r = w.ask({"op": "start", "cwd": cwd, "agent": "claude", "account": "claude-work", "text": "hi"})
check(r.get("ev") == "error" and r["message"] == "The keyring is locked", "a start that needs a key: \"The keyring is locked\"", r)
w.ask({"op": "account_check"})
st = w.wait(lambda m: m.get("ev") == "accounts" and m["keyring"] == "locked" and acct(m, "claude-work")["state"] == "locked", 30)
check(acct(st, "opencode-personal")["state"] == "ready" and any(p["state"] == "locked" for p in acct(st, "opencode-personal")["providers"]),
      "locked: key accounts read locked, a pi/opencode key provider too", acct(st, "opencode-personal"))
pa = acct(pi_read(lambda a: a["state"] == "locked", "pi locked"), "pi-personal")
check({"id": "openrouter", "via": "key", "state": "locked"} in pa["providers"] and {"id": "openrouter", "via": "file", "state": "ready"} in pa["providers"],
      "locked: pi with its file key and a locked one reads locked, both listed", pa)
open(KEYSTATE, "w").write("open")
w.ask({"op": "key_use", "id": k_or, "agent": "pi", "on": False})
os.remove(PI)
os.symlink(os.path.join(FAKES, "pi-pkg/dist/bundle/cli.js"), PI)
w.ask({"op": "account_check"})
w.wait(lambda m: m.get("ev") == "accounts" and m["keyring"] == "open", 30)

# ── sign-ins, every step ──
mode("claude", "ok")
w.drain()
w.ask({"op": "account_login", "account": "claude-personal"})
ss = steps(w, "claude-personal")
check([m["step"] for m in ss] == ["waiting", "done"] and ss[0]["url"].startswith("https://claude.example/oauth") and ss[-1]["who"]["email"] == "someone@example.com",
      "Claude: waiting (the address) then done with who", ss)
st = w.wait(lambda m: m.get("ev") == "accounts" and acct(m, "claude-personal")["state"] == "ready", 15)
check(acct(st, "claude-personal")["plan"] == "max", "the account reads ready after its sign-in", acct(st, "claude-personal"))

mode("claude", "code")
w.ask({"op": "account_login", "account": "claude-personal"})
m = w.wait(lambda m: m.get("ev") == "signin" and m.get("step") == "waiting", 10)
w.wait(lambda m: m.get("ev") == "accounts" and acct(m, "claude-personal")["state"] == "signingIn" and m["signin"]["account"] == "claude-personal", 10)
w.ask({"op": "account_code", "account": "claude-personal", "code": "not-a-code"})
m = w.wait(lambda m: m.get("ev") == "signin" and m.get("step") in ("code", "done", "failed"), 10)
check(m["step"] == "code" and m["message"].startswith("Invalid code") and m["url"].startswith("https://claude.example"), "a refused code: the code step with Claude's words", m)
w.ask({"op": "account_code", "account": "claude-personal", "code": "abc#state"})
m = w.wait(lambda m: m.get("ev") == "signin" and m.get("step") in ("done", "failed"), 10)
check(m["step"] == "done", "a pasted code#state finishes it", m)

mode("claude", "fail")
w.ask({"op": "account_login", "account": "claude-personal"})
ss = steps(w, "claude-personal")
check(ss[-1]["step"] == "failed" and "403" in ss[-1]["message"], "a failed sign-in says why", ss)

mode("claude", "hang")
w.ask({"op": "account_login", "account": "claude-personal"})
w.wait(lambda m: m.get("ev") == "signin" and m.get("step") == "waiting", 10)
pid = int(open(os.path.join(OUT, "claude.pid")).read())
r = w.ask({"op": "account_login", "account": "codex-personal", "method": "device"})
m = w.wait(lambda m: m.get("ev") == "signin" and m.get("account") == "codex-personal", 10)
check(m["step"] == "busy" and m["holder"] == "claude", "one sign-in at a time: a second one is busy, naming the holder", m)
w.ask({"op": "account_cancel", "account": "claude-personal"})
m = w.wait(lambda m: m.get("ev") == "signin" and m.get("account") == "claude-personal", 10)
time.sleep(0.3)
check(m["step"] == "cancelled" and gone(pid), "cancel ends it and its process", m)

mode("codex", "ok")
w.ask({"op": "account_login", "account": "codex-personal"})
ss = steps(w, "codex-personal")
check([m["step"] for m in ss] == ["waiting", "done"] and ss[0]["url"].startswith("https://auth.example/codex") and ss[-1]["who"]["plan"] == "plus",
      "Codex in the browser: waiting then done", ss)
w.ask({"op": "account_login", "account": "codex-personal", "method": "device"})
ss = steps(w, "codex-personal")
check([m["step"] for m in ss] == ["device", "done"] and ss[0]["userCode"] == "WXYZ-1234", "Codex by device code", ss)
held = socket.socket()
held.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    held.bind(("127.0.0.1", 1455))
    held.listen()
    w.ask({"op": "account_login", "account": "codex-personal"})
    ss = steps(w, "codex-personal")
    check([m["step"] for m in ss] == ["busy"], "port 1455 taken: a ChatGPT sign-in in the browser is busy", ss)
finally:
    held.close()

w.ask({"op": "account_login", "account": "pi-personal", "provider": "openai-codex"})
ss = steps(w, "pi-personal")
check([m["step"] for m in ss] == ["waiting", "done"] and ss[0]["provider"] == "openai-codex", "pi through the helper: waiting then done", ss)

mode("opencode", "ok")
w.ask({"op": "account_login", "account": "opencode-personal", "provider": "openai"})
ss = steps(w, "opencode-personal")
check([m["step"] for m in ss] == ["waiting", "done"], "opencode in the browser: waiting then done", ss)
w.ask({"op": "account_login", "account": "opencode-personal", "provider": "anthropic"})
ss = steps(w, "opencode-personal", until=("code", "done", "failed", "cancelled", "busy"), t=15)
check([m["step"] for m in ss] == ["waiting", "code"] and ss[-1]["url"] == "https://claude.example/oauth",
      "opencode's paste-a-code method: waiting, then the code step with the page's address", ss)
w.ask({"op": "account_code", "account": "opencode-personal", "code": "good#code"})
ss = steps(w, "opencode-personal")
check(ss[-1]["step"] == "done", "opencode: the pasted code finishes it", ss)
r = w.ask({"op": "account_login", "account": "claude-work"})
check(r.get("ev") == "error", "a key account does not sign in", r)
r = w.ask({"op": "account_logout", "account": "codex-personal"})
st = w.ask({"op": "accounts"})
check(r.get("ev") == "ok" and acct(st, "codex-personal")["state"] == "signedOut", "account_logout signs out through the agent", r, acct(st, "codex-personal"))
r = w.ask({"op": "account_logout", "account": "opencode-personal", "provider": "OpenAI"})
check(r.get("ev") == "ok" and open(os.path.join(OUT, "opencode-logout")).read() == "OpenAI", "opencode signs out of one provider", r)

# ── an added plan account, then its removal ──
mode("claude", "ok")
r = w.ask({"op": "account_add", "agent": "claude", "label": "Second", "use": "plan"})
second = os.path.join(HOME, ".local/share/agent-host/accounts", r["id"])
w.ask({"op": "account_login", "account": r["id"]})
ss = steps(w, r["id"])
check(ss[-1]["step"] == "done" and os.path.exists(os.path.join(second, "signed-in")), "an added account signs in into its own folder", ss)
w.ask({"op": "account_set", "account": r["id"], "label": "Second one", "default": True})
st = w.ask({"op": "accounts"})
check(acct(st, r["id"])["label"] == "Second one" and acct(st, r["id"])["default"] and not acct(st, "claude-personal")["default"], "account_set: name and default", acct(st, r["id"]))
open(os.path.join(second, ".credentials.json"), "w").write('{"fake": true}')
r2 = w.ask({"op": "account_remove", "account": r["id"]})
trash = os.path.join(HOME, ".local/share/Trash/files", r["id"])
check(r2.get("ev") == "ok" and not os.path.exists(second) and os.path.isdir(trash) and not os.path.exists(os.path.join(trash, ".credentials.json"))
      and not os.path.exists(os.path.join(trash, "signed-in")), "removal: signed out, credential file deleted outright, folder in the Trash",
      r2, os.listdir(trash) if os.path.isdir(trash) else "no trash")
st = w.ask({"op": "accounts"})
check(acct(st, r["id"]) is None and acct(st, "claude-personal")["default"], "removed from the list; Personal is the default again")
r = w.ask({"op": "account_remove", "account": "claude-personal"})
check(r.get("ev") == "error" and "Personal" in r["message"], "Personal cannot be removed", r)

# ── catalog: from the agent once, then from cache ──
r = w.ask({"op": "catalog", "agent": "claude", "cwd": cwd}, 40)
check(r.get("ev") == "catalog" and r["account"] == "claude-personal" and any(c["name"] == "fake-cmd" for c in r["commands"]), "catalog: read through a probe", r)
r = w.ask({"op": "catalog", "agent": "claude", "cwd": cwd, "refresh": True})
check(r.get("ev") == "catalog", "catalog from cache")
w.wait(lambda m: m.get("ev") == "catalog" and m.get("tag") is None, 30)
check(True, "a refresh is pushed to watchers")

# ── the file ──
cfg = os.path.join(SB, "config", "accounts.json")
text = open(cfg).read()
check(stat.S_IMODE(os.stat(cfg).st_mode) == 0o600 and not any(s in text for s in SECRETS), "accounts.json: 0600, no secret in it")
host.stop()
w.close()
old.close()
errs = open(os.path.join(SB, "host-a.stderr")).read()
check("debug: using key ••••" in errs and not any(s in errs for s in SECRETS), "host stderr: the agent's echo of its keys is masked",
      [l for l in errs.splitlines() if "using key" in l][:6])
pushes = [m for m in old.got if m.get("ev") in ("accounts", "signin", "install", "catalog")]
check(not pushes and len(old.got) >= 1, "a connection that never said watch got no accounts pushes", pushes[:3])
leaks = [l for l in SEEN if any(s in l for s in SECRETS)]
check(not leaks, "no secret in any reply or push", leaks[:3])

# ═══ host B: installs ════════════════════════════════════════════════════════════════════════
HOME_B, BIN_B, NODE_B, SYS_B = (os.path.join(SB, d) for d in ("home-b", "bin-b", "node-b", "sys-b"))
for d in (HOME_B, BIN_B, NODE_B, SYS_B, os.path.join(HOME_B, ".local/bin")):
    os.makedirs(d, exist_ok=True)
for tool in ("sh", "bash", "env", "python3", "ln", "dirname", "mkdir", "chmod", "cat", "cut", "sleep", "kill", "systemd-run", "gio",
             "printf", "rm", "ls", "tr", "head", "nice"):
    p = shutil.which(tool, path="/usr/bin:/bin")
    if p:
        os.symlink(p, os.path.join(SYS_B, tool))
for f in ("busctl", "secret-tool"):
    os.symlink(os.path.join(BIN, f), os.path.join(BIN_B, f))
env_b = dict(BASE_ENV, HOME=HOME_B, AGENT_HOST_CONFIG=os.path.join(SB, "config-b"), FAKE_BIN=NODE_B,
             PATH=f"{BIN_B}:{FAKES}/install:{NODE_B}:{HOME_B}/.local/bin:{SYS_B}")
host = Host("b", env_b)
w = Conn(host)
st = w.ask({"op": "accounts", "watch": True})
check(st["agents"]["pi"]["installed"] is False and st["node"] is False and st["agents"]["claude"]["route"] == "none", "host B: no pi, no claude, no Node", st["agents"])
w.ask({"op": "install", "agent": "pi"})
m = w.wait(lambda m: m.get("ev") == "install", 15)
check(m["step"] == "needsNode", "install pi without Node: needsNode", m)
w.ask({"op": "install", "agent": "pi", "node": True})
got = []
while True:
    m = w.wait(lambda m: m.get("ev") == "install", 30)
    got.append(m)
    if m["step"] != "running":
        break
check(got[-1]["step"] == "done" and got[-1]["version"] == "1.2.3" and any("installing nodejs" in g.get("line", "") for g in got)
      and any("added 1 package" in g.get("line", "") for g in got), "install pi with Node: pacman's and npm's lines, then done", got)
st = w.wait(lambda m: m.get("ev") == "accounts" and m["agents"]["pi"]["installed"] is True, 20)
check(st["agents"]["pi"]["route"] == "user" and st["node"] is True, "found again after the install, as a user install", st["agents"]["pi"])
w.ask({"op": "install", "agent": "claude"})
got = []
while True:
    m = w.wait(lambda m: m.get("ev") == "install", 30)
    got.append(m)
    if m["step"] != "running":
        break
check(got[-1]["step"] == "done" and any("successfully installed" in g.get("line", "") for g in got), "install Claude Code with its script", got)
r = w.ask({"op": "install", "agent": "pi"})
check(r.get("ev") == "error" and "already installed" in r["message"], "a second install of an installed agent is refused", r)
host.stop()
w.close()
leaks = [l for l in SEEN if any(s in l for s in SECRETS)]
check(not leaks, "still no secret anywhere", leaks[:3])
say("all passed")
if not ARGS.keep:
    shutil.rmtree(SB, ignore_errors=True)
    shutil.rmtree(RT, ignore_errors=True)
