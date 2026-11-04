#!/usr/bin/env python3
"""A stand-in for agent-host's accounts, catalog and effort features (praxis-composer-accounts-plan
§3.2), for UI tests. It listens on a unix socket and speaks the line protocol: the hello (with
`features`), an empty history, `accounts {watch}` and its pushes, `resolve`, `catalog` (from
fixture JSON), `defaults_set`, the `account_*` and `key_*` ops with scripted `signin` pushes,
`install` with scripted progress, `effort`, `model` and a minimal `start`. It signs nothing in,
installs nothing and stores no secret.

    fake_accounts_host.py --socket $SB/rt/agent-host.sock --scenario <file.json> --log $SB/requests.log

The scenario is a JSON file (tests/fixtures/fake_accounts/scenarios/). Its keys:
  extends          another scenario file, merged under this one (objects merge, lists replace)
  proto, features  the hello's protocol number and features (default 6 and all three)
  keyring, node, agents, accounts, keys, found, providers, defaults
                   the first `accounts` state, in the shape of §3.2
  patch            {"accounts": {<id>: {fields}}} applied over the inherited accounts
  catalogs         {<agent>: <file>} catalog fixtures (default ../catalog/<agent>.json)
  catalogStale     the `stale` flag of every catalog reply
  catalogCwd       false: catalog replies and pushes leave out `cwd` (default: the folder asked for,
                   none for $HOME)
  signin           [{account?, method?, provider?, steps: [step]}]: the first rule that matches an
                   `account_login` runs its steps; a step is {after: ms, step, url?, userCode?,
                   holder?, message?, who?}. Default: waiting, then done after 1.5 s.
  onCode           the steps an `account_code` runs (default: done after 0.3 s)
  installs         [{agent?, node?, steps: [step]}] for `install`; a step is {after, step, line?,
                   version?}. Default: needsNode for an npm agent without Node, else three
                   running lines and done.
  keyCheck         "ok" or "refused": what `key_set` and `key_check` find
  replies          {<op>: reply} answers an op with this reply (plus the tag) instead
  on               {<op>: [message]} untagged messages sent to the asking connection after the reply
  pushes           [{after: ms, msg}] untagged messages sent to each connection after it connects
  history          the rows of the `history` reply (default none)

Every request line is written to the log as {"t": ms, "conn": n, "req": {...}}, with `secret` and
`code` values cut to their last four.
"""
import argparse
import copy
import json
import os
import socket
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "fixtures", "fake_accounts")
NPM_AGENTS = ("codex", "pi", "opencode")
T0 = time.monotonic()


def ms():
    return int((time.monotonic() - T0) * 1000)


def merge(base, over):
    if isinstance(base, dict) and isinstance(over, dict):
        out = dict(base)
        for k, v in over.items():
            out[k] = merge(base.get(k), v) if k in base else copy.deepcopy(v)
        return out
    return copy.deepcopy(over)


def load_scenario(path):
    with open(path) as f:
        sc = json.load(f)
    if "extends" in sc:
        base = load_scenario(os.path.join(os.path.dirname(path), sc.pop("extends")))
        sc = merge(base, sc)
    for aid, fields in sc.pop("patch", {}).get("accounts", {}).items():
        for a in sc.get("accounts", []):
            if a["id"] == aid:
                a.update(fields)
    sc.setdefault("_dir", os.path.dirname(os.path.abspath(path)))
    return sc


def redact(req):
    r = dict(req)
    for k in ("secret", "code"):
        if isinstance(r.get(k), str):
            r[k] = "••••" + r[k][-4:]
    return r


class Conn:
    def __init__(self, cid, sock):
        self.cid = cid
        self.sock = sock
        self.watch = False
        self.history_sent = False
        self.lock = threading.Lock()

    def send(self, msg):
        line = (json.dumps(msg) + "\n").encode()
        with self.lock:
            try:
                self.sock.sendall(line)
            except OSError:
                pass


class Fake:
    def __init__(self, sc, log_path):
        self.sc = sc
        self.lock = threading.RLock()
        self.conns = {}
        self.next_cid = 0
        self.next_id = 0
        self.log = open(log_path, "a", buffering=1) if log_path else None
        self.timers = {}          # account or "install:<agent>" -> [Timer]
        self.before_login = {}    # account -> its state before the sign-in began
        self.sessions = {}
        self.catalog_cache = {}
        st = {k: copy.deepcopy(sc.get(k)) for k in ("agents", "accounts", "keys", "found", "providers", "defaults")}
        self.state = {
            "keyring": sc.get("keyring", "open"),
            "agents": st["agents"] or {},
            "node": sc.get("node", True),
            "accounts": st["accounts"] or [],
            "keys": st["keys"] or [],
            "found": st["found"] or [],
            "providers": st["providers"] or [],
            "defaults": st["defaults"] or {"everywhere": {}, "folders": {}},
            "signin": None,
            "install": None,
        }

    # ── sending ──
    def accounts_msg(self):
        return dict({"ev": "accounts"}, **copy.deepcopy(self.state))

    def push(self, msg):
        for c in list(self.conns.values()):
            if c.watch:
                c.send(msg)

    def push_state(self):
        self.push(self.accounts_msg())

    def to_all(self, msg):
        for c in list(self.conns.values()):
            c.send(msg)

    def later(self, key, delay_ms, fn):
        t = threading.Timer(delay_ms / 1000.0, self.locked(fn))
        t.daemon = True
        self.timers.setdefault(key, []).append(t)
        t.start()

    def locked(self, fn):
        def run():
            with self.lock:
                fn()
        return run

    def stop_timers(self, key):
        for t in self.timers.pop(key, []):
            t.cancel()

    # ── reading the state ──
    def account(self, aid):
        return next((a for a in self.state["accounts"] if a["id"] == aid), None)

    def default_account(self, agent):
        l = [a for a in self.state["accounts"] if a["agent"] == agent]
        return next((a for a in l if a.get("default")), None) or next((a for a in l if a.get("personal")), None) or (l[0] if l else None)

    def hello_agents(self):
        out = {}
        for agent, ag in self.state["agents"].items():
            a = self.default_account(agent)
            installed = ag.get("installed", False)
            signed = installed and a is not None and a.get("state") == "ready"
            out[agent] = {"installed": installed, "version": ag.get("version", ""), "signedIn": signed,
                          "reason": "" if signed else ("Not installed" if not installed else "Not signed in"),
                          "caps": {"fork": True, "rename": True, "delete": True, "rewind": agent == "claude",
                                   "rewindFiles": agent == "claude", "aside": agent == "claude",
                                   "tasks": agent == "claude", "steer": agent in ("codex", "pi")}}
        return out

    def hello(self):
        return {"ev": "hello", "version": "fake", "proto": self.sc.get("proto", 6),
                "sessions": list(self.sessions.values()), "agents": self.hello_agents(),
                "features": self.sc.get("features", ["accounts", "catalog", "effort"])}

    def catalog_for(self, agent):
        if agent not in self.catalog_cache:
            name = self.sc.get("catalogs", {}).get(agent) or os.path.join("..", "catalog", f"{agent}.json")
            path = name if os.path.isabs(name) else os.path.join(self.sc["_dir"], name)
            try:
                with open(path) as f:
                    self.catalog_cache[agent] = json.load(f)
            except OSError:
                self.catalog_cache[agent] = {"models": [], "commands": []}
        return self.catalog_cache[agent]

    # ── sign-in ──
    def signin_steps(self, req):
        for rule in self.sc.get("signin", []):
            if all(rule.get(k) in (None, req.get(k)) for k in ("account", "method", "provider")):
                return rule["steps"]
        return [{"after": 200, "step": "waiting", "url": "https://example.invalid/oauth/authorize?fake=1"},
                {"after": 1500, "step": "done", "who": {"email": "fake@example.invalid", "plan": "max"}}]

    def run_signin(self, aid, provider, steps):
        at = 0
        for s in steps:
            at += s.get("after", 0)
            self.later(aid, at, lambda s=s: self.signin_step(aid, provider, s))

    def signin_step(self, aid, provider, s):
        a = self.account(aid)
        if a is None:
            return
        step = s["step"]
        msg = {"ev": "signin", "account": aid, "step": step}
        if provider:
            msg["provider"] = provider
        for k in ("url", "userCode", "holder", "message", "who"):
            if k in s:
                msg[k] = s[k]
        if step in ("waiting", "code", "device"):
            self.before_login.setdefault(aid, a.get("state"))
            a["state"] = "signingIn"
            self.state["signin"] = {k: v for k, v in msg.items() if k != "ev"}
        elif step == "done":
            who = s.get("who", {})
            a["state"] = "ready"
            for k in ("email", "plan"):
                if k in who:
                    a[k] = who[k]
            if provider:
                ps = [p for p in a.get("providers", []) if p["id"] != provider]
                a["providers"] = ps + [{"id": provider, "via": "oauth", "state": "ready"}]
            self.before_login.pop(aid, None)
            self.state["signin"] = None
        else:   # failed, cancelled, busy
            if aid in self.before_login:
                a["state"] = self.before_login.pop(aid)
            self.state["signin"] = None
        self.push(msg)
        self.push_state()

    # ── install ──
    def install_steps(self, agent, node):
        for rule in self.sc.get("installs", []):
            if rule.get("agent") in (None, agent) and rule.get("node") in (None, node):
                return rule["steps"]
        if agent in NPM_AGENTS and not self.state["node"] and not node:
            return [{"after": 100, "step": "needsNode"}]
        pkg = {"claude": "claude.ai/install.sh", "codex": "@openai/codex", "pi": "@earendil-works/pi-coding-agent",
               "opencode": "opencode-ai"}.get(agent, agent)
        lines = ([f"pacman: installing nodejs, npm"] if node else []) + [
            f"npm http fetch GET 200 https://registry.npmjs.org/{pkg} 412ms (cache miss)",
            f"added 12 packages in 3s"]
        return [{"after": 300, "step": "running", "line": l} for l in lines] + [{"after": 400, "step": "done", "version": "1.0.0-fake"}]

    def install_step(self, agent, s, with_node):
        step = s["step"]
        msg = {"ev": "install", "agent": agent, "step": step}
        for k in ("line", "version"):
            if k in s:
                msg[k] = s[k]
        if step == "running":
            self.state["install"] = {k: v for k, v in msg.items() if k != "ev"}
        else:
            self.state["install"] = None
        if step == "done":
            ag = self.state["agents"].setdefault(agent, {})
            ag.update({"installed": True, "version": s.get("version", "1.0.0-fake"), "route": "user"})
            if with_node:
                self.state["node"] = True
        self.push(msg)
        self.push_state()

    # ── resolve and defaults ──
    def resolve(self, cwd, agent):
        d = self.state["defaults"]
        ev = d.get("everywhere", {}) or {}
        home = os.path.expanduser("~")
        best = None
        for path, f in (d.get("folders", {}) or {}).items():
            if path == cwd or (path != home and cwd.startswith(path.rstrip("/") + "/")):
                if best is None or len(path) > len(best[0]):
                    best = (path, f)
        chain = ([(f"folder:{best[0]}", best[1])] if best else []) + [("everywhere", ev)]
        out = {"ev": "resolved", "from": {}}
        if agent:
            out["agent"], out["from"]["agent"] = agent, "explicit"
        else:
            src = next(((n, f["agent"]) for n, f in chain if f.get("agent")), ("agent", "claude"))
            out["agent"], out["from"]["agent"] = src[1], src[0]
        ag = out["agent"]
        for field in ("account", "model", "effort"):
            src = next(((n, f["run"][ag][field]) for n, f in chain if (f.get("run", {}).get(ag) or {}).get(field)), None)
            if src:
                out[field], out["from"][field] = src[1], src[0]
            else:
                da = self.default_account(ag) if field == "account" else None
                out[field], out["from"][field] = (da["id"] if da else None), "agent"
        src = next(((n, f["mode"]) for n, f in chain if f.get("mode")), None)
        out["mode"], out["from"]["mode"] = (src[1], src[0]) if src else (None, "agent")
        return out

    def defaults_set(self, req):
        d = self.state["defaults"]
        scope = req.get("scope")
        if scope == "everywhere":
            target = d.setdefault("everywhere", {})
        else:
            target = d.setdefault("folders", {}).setdefault(scope, {})
        for k in ("agent", "mode"):
            if k in req:
                if req[k] is None:
                    target.pop(k, None)
                else:
                    target[k] = req[k]
        for ag, fields in (req.get("run") or {}).items():
            run = target.setdefault("run", {})
            if fields is None:
                run.pop(ag, None)
                continue
            entry = run.setdefault(ag, {})
            for k, v in fields.items():
                if v is None:
                    entry.pop(k, None)
                else:
                    entry[k] = v
            if not entry:
                run.pop(ag, None)
        if "run" in target and not target["run"]:
            target.pop("run")
        if scope != "everywhere" and not target:
            d["folders"].pop(scope, None)

    # ── ops ──
    def handle(self, conn, req):
        op = req.get("op", "")
        if op in self.sc.get("replies", {}):
            return copy.deepcopy(self.sc["replies"][op])
        ok = {"ev": "ok"}
        st = self.state
        if op == "hello":
            return {"ev": "ok", "proto": self.sc.get("proto", 6)}
        if op == "list":
            return {"ev": "sessions", "sessions": list(self.sessions.values())}
        if op == "history":
            if req.get("ifChanged") and conn.history_sent:
                return {"ev": "history", "unchanged": True}
            conn.history_sent = True
            return {"ev": "history", "sessions": copy.deepcopy(self.sc.get("history", []))}
        if op == "search":
            return {"ev": "search", "q": req.get("q", ""), "results": []}
        if op == "accounts":
            if req.get("watch"):
                conn.watch = True
            return self.accounts_msg()
        if op == "resolve":
            return self.resolve(req.get("cwd", ""), req.get("agent"))
        if op == "catalog":
            agent = req.get("agent", "claude")
            da = self.default_account(agent)
            account = req.get("account") or (da["id"] if da else "")
            c = self.catalog_for(agent)
            msg = {"ev": "catalog", "agent": agent, "account": account, "at": int(time.time() * 1000),
                   "stale": bool(self.sc.get("catalogStale", False)), "models": c["models"], "commands": c["commands"]}
            if req.get("cwd") and self.sc.get("catalogCwd", True):
                msg["cwd"] = req["cwd"]
            if req.get("refresh"):
                fresh = dict(msg, stale=False)
                self.later("catalog", 300, lambda: self.push(dict(fresh, at=int(time.time() * 1000))))
            return msg
        if op == "defaults_set":
            self.defaults_set(req)
            self.push_state()
            return ok
        if op in ("effort", "model", "send", "answer", "interrupt", "mode", "unwatch", "rename"):
            if req.get("id") and req["id"] in self.sessions:
                s = self.sessions[req["id"]]
                if op in ("effort", "model"):
                    s[op] = req.get(op)
                    self.to_all({"ev": "state", "session": s})
            return ok
        if op == "account_check":
            self.push_state()
            return ok
        if op == "account_add":
            base = f"{req.get('agent')}-{''.join(ch for ch in req.get('label', 'account').lower() if ch.isalnum()) or 'account'}"
            aid, n = base, 1
            while self.account(aid):
                n += 1
                aid = f"{base}{n}"
            a = {"id": aid, "agent": req.get("agent"), "label": req.get("label", ""), "personal": False, "default": False,
                 "use": req.get("use", "plan"), "state": "signedOut", "folders": []}
            if req.get("key"):
                a["key"] = req["key"]
            st["accounts"].append(a)
            self.push_state()
            return {"ev": "account", "id": aid}
        if op == "account_login":
            aid = req.get("account")
            if not self.account(aid):
                return {"ev": "error", "message": f"no such account: {aid}"}
            self.stop_timers(aid)
            self.run_signin(aid, req.get("provider"), self.signin_steps(req))
            return ok
        if op == "account_code":
            aid = req.get("account")
            steps = self.sc.get("onCode", [{"after": 300, "step": "done", "who": {"email": "fake@example.invalid", "plan": "max"}}])
            self.stop_timers(aid)
            self.run_signin(aid, (st["signin"] or {}).get("provider"), steps)
            return ok
        if op == "account_cancel":
            aid = req.get("account")
            self.stop_timers(aid)
            if (st["signin"] or {}).get("account") == aid or aid in self.before_login:
                self.signin_step(aid, (st["signin"] or {}).get("provider"), {"step": "cancelled"})
            return ok
        if op == "account_logout":
            a = self.account(req.get("account"))
            if a is None:
                return {"ev": "error", "message": "no such account"}
            if req.get("provider"):
                a["providers"] = [p for p in a.get("providers", []) if p["id"] != req["provider"]]
                if not a["providers"]:
                    a["state"] = "noProvider" if a["agent"] in ("pi", "opencode") else "signedOut"
            else:
                a["state"] = "signedOut"
                a.pop("email", None)
                a.pop("plan", None)
            self.push_state()
            return ok
        if op == "account_remove":
            a = self.account(req.get("account"))
            if a is None:
                return {"ev": "error", "message": "no such account"}
            if a.get("personal"):
                return {"ev": "error", "message": "Personal can't be removed."}
            st["accounts"].remove(a)
            self.push_state()
            return ok
        if op == "account_set":
            a = self.account(req.get("account"))
            if a is None:
                return {"ev": "error", "message": "no such account"}
            for k in ("label", "use", "key"):
                if k in req:
                    a[k] = req[k]
            if req.get("default"):
                for b in st["accounts"]:
                    if b["agent"] == a["agent"]:
                        b["default"] = b is a
            self.push_state()
            return ok
        if op == "key_set":
            secret = req.get("secret") or ""
            last4 = secret[-4:]
            if self.sc.get("keyCheck", "ok") == "refused":
                return {"ev": "key", "id": None, "last4": last4, "state": "refused"}
            self.next_id += 1
            kid = f"key-{self.next_id}"
            prov = req.get("provider", "")
            st["keys"].append({"id": kid, "provider": prov, "label": req.get("label") or f"{prov} key", "last4": last4,
                               "use": [], "state": "ok", "checked": int(time.time() * 1000)})
            self.push_state()
            return {"ev": "key", "id": kid, "last4": last4, "state": "ok"}
        if op in ("key_use", "key_check", "key_remove"):
            k = next((x for x in st["keys"] if x["id"] == req.get("id")), None)
            if k is None:
                return {"ev": "error", "message": "no such key"}
            if op == "key_use":
                use = [a for a in k["use"] if a != req.get("agent")]
                k["use"] = use + ([req["agent"]] if req.get("on") else [])
                self.push_state()
                return ok
            if op == "key_check":
                k["state"] = self.sc.get("keyCheck", "ok")
                k["checked"] = int(time.time() * 1000)
                self.push_state()
                return {"ev": "key", "id": k["id"], "last4": k["last4"], "state": k["state"]}
            used = [a["id"] for a in st["accounts"] if a.get("key") == k["id"]]
            if used and not req.get("force"):
                return {"ev": "error", "message": "This key is used by an account.", "usedBy": used}
            st["keys"].remove(k)
            self.push_state()
            return ok
        if op == "install":
            agent = req.get("agent")
            key = f"install:{agent}"
            self.stop_timers(key)
            at = 0
            for s in self.install_steps(agent, bool(req.get("node"))):
                at += s.get("after", 0)
                self.later(key, at, lambda s=s: self.install_step(agent, s, bool(req.get("node"))))
            return ok
        if op == "install_cancel":
            agent = req.get("agent")
            self.stop_timers(f"install:{agent}")
            self.install_step(agent, {"step": "cancelled"}, False)
            return ok
        if op == "start":
            self.next_id += 1
            sid = f"fake-{self.next_id:04d}"
            tool = req.get("agent") or "claude"
            s = {"id": sid, "tool": tool, "cwd": req.get("cwd", ""), "status": "idle", "account": req.get("account"),
                 "model": req.get("model"), "effort": req.get("effort"), "mode": req.get("mode"),
                 "started": int(time.time() * 1000)}
            self.sessions[sid] = s
            self.to_all({"ev": "state", "session": s})
            return {"ev": "started", "id": sid, "tool": tool}
        if op == "watch":
            return {"ev": "watching", "id": req.get("id"), "seq": 0, "items": []}
        if op == "close":
            if self.sessions.pop(req.get("id"), None):
                self.to_all({"ev": "exit", "id": req["id"], "code": 0, "reason": "closed", "stderr": []})
            return ok
        if op == "transcript":
            return {"ev": "transcript", "id": req.get("id"), "tool": "claude", "total": 0, "start": 0, "offset": 0, "items": []}
        if op == "agents":
            return {"ev": "agents", "id": req.get("id"), "agents": [], "workflows": [], "shells": [], "items": []}
        return {"ev": "error", "message": f"fake host: no op {op}"}

    def serve(self, sock):
        with self.lock:
            self.next_cid += 1
            conn = Conn(self.next_cid, sock)
            self.conns[conn.cid] = conn
            conn.send(self.hello())
            at = 0
            for p in self.sc.get("pushes", []):
                at += p.get("after", 0)
                self.later(f"conn:{conn.cid}", at, lambda p=p: conn.send(p["msg"]))
        buf = b""
        try:
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    try:
                        req = json.loads(line)
                    except ValueError:
                        conn.send({"ev": "error", "message": "not JSON"})
                        continue
                    if self.log:
                        self.log.write(json.dumps({"t": ms(), "conn": conn.cid, "req": redact(req)}) + "\n")
                    with self.lock:
                        reply = self.handle(conn, req)
                        if "tag" in req:
                            reply["tag"] = req["tag"]
                        conn.send(reply)
                        for m in self.sc.get("on", {}).get(req.get("op"), []):
                            conn.send(m)
        except OSError:
            pass
        finally:
            with self.lock:
                self.conns.pop(conn.cid, None)
                self.stop_timers(f"conn:{conn.cid}")
            sock.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--socket", required=True)
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--log")
    args = ap.parse_args()
    fake = Fake(load_scenario(args.scenario), args.log)
    try:
        os.unlink(args.socket)
    except FileNotFoundError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(args.socket)
    os.chmod(args.socket, 0o600)
    srv.listen(8)
    print(f"fake agent-host: {os.path.basename(args.scenario)} at {args.socket}", flush=True)
    try:
        while True:
            s, _ = srv.accept()
            threading.Thread(target=fake.serve, args=(s,), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()
        try:
            os.unlink(args.socket)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
