//! Codex sessions: threads on one `codex app-server` (JSON-RPC lines on stdio) per account,
//! started on first use and closed once no thread is open on it and it has had no call for
//! AGENT_HOST_IDLE. Its notifications are relayed in Claude Code's shape, and its approval
//! requests are raised as can_use_tool. Following a thread reads its rollout file (history.rs);
//! thread/read is never called.

use crate::{agent_command, ask, in_scope, own, path_list, relay, released, sent, start_cwd, take_requests, write_in, Host, Session, Shared};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::{process, thread};

pub struct Server {
    key: String, // the account ("": the agent's own sign-in)
    gen: u64,
    // taken when the server is closed: with no thread open, this is the last sender to the writer,
    // whose end closes the app-server's stdin (the reader threads keep the Server itself alive)
    tx: Mutex<Option<Sender<String>>>,
    waiting: Mutex<HashMap<u64, Sender<Result<Value, String>>>>,
    next: AtomicU64,
    last: Mutex<Instant>, // when its last call was made or answered
    stderr: Mutex<VecDeque<String>>,
    done: AtomicBool, // its process has exited
}

// Never locked while the host is waited for: the reaper takes it (try_lock) with the host locked.
fn servers() -> &'static Mutex<HashMap<String, Arc<Server>>> {
    static S: OnceLock<Mutex<HashMap<String, Arc<Server>>>> = OnceLock::new();
    S.get_or_init(Default::default)
}

// Servers told to close that may not have exited yet: one still holds its threads' rollouts, and a
// new server cannot open them until it has gone.
fn closing() -> &'static Mutex<Vec<Arc<Server>>> {
    static S: OnceLock<Mutex<Vec<Arc<Server>>>> = OnceLock::new();
    S.get_or_init(Default::default)
}

static GEN: AtomicU64 = AtomicU64::new(1 << 32); // apart from the gens of Claude Code processes

impl Server {
    fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        *self.last.lock().unwrap() = Instant::now();
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        self.waiting.lock().unwrap().insert(n, tx);
        // a server that has already exited answered its waiting calls before this one was added
        if self.done.load(Ordering::Relaxed) {
            self.waiting.lock().unwrap().remove(&n);
            return Err("codex app-server has stopped".into());
        }
        self.to()?.send(format!("{}\n", json!({"jsonrpc": "2.0", "id": n, "method": method, "params": params})))
            .map_err(|_| "codex app-server has stopped".to_string())?;
        let r = rx.recv_timeout(Duration::from_secs(90)).map_err(|_| format!("codex did not answer {method}"));
        self.waiting.lock().unwrap().remove(&n);
        *self.last.lock().unwrap() = Instant::now();
        r?
    }

    fn to(&self) -> Result<Sender<String>, String> {
        self.tx.lock().unwrap().clone().ok_or("codex app-server has stopped".into())
    }
}

// A request whose answer nobody waits for (string ids are never matched to a caller).
fn tell(to: &Sender<String>, method: &str, params: Value) -> Result<(), String> {
    static N: AtomicU64 = AtomicU64::new(0);
    let id = format!("host-{}", N.fetch_add(1, Ordering::Relaxed));
    to.send(format!("{}\n", json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})))
        .map_err(|_| "codex app-server has stopped".to_string())
}

fn server(shared: &Shared, account: Option<&str>) -> Result<Arc<Server>, String> {
    let key = account.unwrap_or("").to_string();
    let mut all = servers().lock().unwrap();
    if let Some(s) = all.get(&key) {
        *s.last.lock().unwrap() = Instant::now();
        return Ok(s.clone());
    }
    let until = Instant::now() + Duration::from_secs(5);
    while closing().lock().unwrap().iter().any(|s| s.key == key && !s.done.load(Ordering::Relaxed)) && Instant::now() < until {
        thread::sleep(Duration::from_millis(50));
    }
    closing().lock().unwrap().retain(|s| !s.done.load(Ordering::Relaxed));
    let gen = GEN.fetch_add(1, Ordering::Relaxed);
    let mut cmd = agent_command("codex", account)?;
    // notify: no desktop notification per turn; the host reports turns itself
    cmd.args(["app-server", "-c", "notify=[]"]).current_dir(crate::home());
    let srv = launch(shared, &key, gen, in_scope(&cmd, &format!("agent-codex-{}-{gen}", process::id())))?;
    all.insert(key, srv.clone());
    Ok(srv)
}

// `cmd` started as the app-server of `key` and initialized; server() calls it with the servers lock held.
fn launch(shared: &Shared, key: &str, gen: u64, mut cmd: Command) -> Result<Arc<Server>, String> {
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start codex: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let srv = Arc::new(Server { key: key.to_string(), gen, tx: Mutex::new(Some(write_in(child.stdin.take().unwrap(), "codex-ap"))), waiting: Mutex::new(HashMap::new()),
                                next: AtomicU64::new(1), last: Mutex::new(Instant::now()), stderr: Mutex::new(VecDeque::new()),
                                done: AtomicBool::new(false) });
    let (a, b) = (srv.clone(), srv.clone());
    let sh = shared.clone();
    let (said, heard) = channel::<()>();
    thread::spawn(move || read_server(sh, a, stdout, child, heard));
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let line = crate::keys::redact(&line).into_owned();
            eprintln!("[codex] {line}");
            let mut e = b.stderr.lock().unwrap();
            e.push_back(line);
            if e.len() > crate::STDERR_LINES {
                e.pop_front();
            }
        }
        drop(said); // all of it is in: read_server reports the exit with it
    });
    if let Err(e) = srv.call("initialize", json!({"clientInfo": {"name": "agent-host", "version": env!("CARGO_PKG_VERSION")}})) {
        // a server that exits at start says why on stderr; one that hangs is let go of
        srv.tx.lock().unwrap().take();
        let said: Vec<String> = srv.stderr.lock().unwrap().iter().filter(|l| !l.trim().is_empty()).cloned().collect();
        return Err(match said.len() {
            0 => e,
            n => format!("{e}. Codex says: {}", said[n.saturating_sub(5)..].join("\n")),
        });
    }
    srv.to()?.send(format!("{}\n", json!({"jsonrpc": "2.0", "method": "initialized"}))).map_err(|e| e.to_string())?;
    Ok(srv)
}

fn read_server(shared: Shared, srv: Arc<Server>, out: ChildStdout, mut child: Child, stderr_end: Receiver<()>) {
    for line in BufReader::new(out).lines() {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        match (v.get("id"), v.get("method")) {
            (Some(_), Some(_)) => server_request(&mut shared.lock().unwrap(), &srv, v),
            (Some(id), None) => {
                if let Some(tx) = id.as_u64().and_then(|n| srv.waiting.lock().unwrap().remove(&n)) {
                    let _ = tx.send(match v.get("error") {
                        Some(e) => Err(e["message"].as_str().unwrap_or("codex refused").to_string()),
                        None => Ok(v["result"].clone()),
                    });
                }
            }
            _ => notification(&mut shared.lock().unwrap(), srv.gen, v),
        }
    }
    let code = child.wait().ok().and_then(|s| s.code());
    let _ = stderr_end.recv_timeout(Duration::from_secs(1));
    srv.done.store(true, Ordering::Relaxed);
    // answered before the servers lock is taken: server() holds it while it waits for initialize
    for (_, tx) in srv.waiting.lock().unwrap().drain() {
        let _ = tx.send(Err("codex app-server has stopped".into()));
    }
    {
        let mut all = servers().lock().unwrap();
        if all.get(&srv.key).is_some_and(|s| Arc::ptr_eq(s, &srv)) {
            all.remove(&srv.key);
        }
    }
    // the threads still open on it end with it
    let stderr: Vec<String> = srv.stderr.lock().unwrap().iter().cloned().collect();
    let mut h = shared.lock().unwrap();
    let open: Vec<String> = h.sessions.values().filter(|s| s.tool == "codex" && s.gen == srv.gen && s.to_child.is_some()).map(|s| s.id.clone()).collect();
    for id in open {
        released(&mut h, &id, if code == Some(0) { "exited" } else { "crashed" }, stderr.clone());
    }
}

// A thread's mode as Codex's approval policy, sandbox and reviewer. `turn`: in turn/start's shape,
// without the sandbox: turn() sends one only when the mode changes its kind (sandbox_change).
fn policy(mode: Option<&str>, turn: bool) -> Value {
    let (approval, sandbox, reviewer) = match mode {
        Some("manual" | "default") => ("untrusted", "workspace-write", "user"),
        Some("acceptEdits") => ("on-request", "workspace-write", "user"),
        Some("auto") => ("on-request", "workspace-write", "auto_review"),
        Some("plan") => ("on-request", "read-only", "user"),
        _ => return json!({}),
    };
    let mut p = json!({"approvalPolicy": approval});
    if !turn {
        p["sandbox"] = json!(sandbox);
    }
    // set every time: the user's own config may name another reviewer
    p["approvalsReviewer"] = json!(reviewer);
    p
}

// The kind of sandbox a turn of the thread must change to for its mode, as turn/start names it; None
// when it runs in that kind already. A turn's sandboxPolicy replaces the whole of the thread's, which
// Codex built from the person's [sandbox_workspace_write] (network access, extra writable folders),
// so one sent on every turn turned those off from the first turn on.
fn sandbox_change(s: &Session) -> Option<&'static str> {
    let kind = match policy(s.mode_req.as_deref(), false)["sandbox"].as_str()? {
        "read-only" => "readOnly",
        _ => "workspaceWrite",
    };
    (s.native.get("sandbox").and_then(|p| p["type"].as_str()) != Some(kind)).then_some(kind)
}

// A sandbox of `kind` as Codex builds one from the person's config for `cwd`.
fn sandbox_policy(srv: &Server, kind: &str, cwd: &str) -> Value {
    let mut p = json!({"type": kind});
    if kind == "workspaceWrite" {
        match srv.call("config/read", json!({"cwd": cwd})) {
            Ok(r) => {
                let w = &r["config"]["sandbox_workspace_write"];
                for (from, to) in [("network_access", "networkAccess"), ("writable_roots", "writableRoots"),
                                   ("exclude_slash_tmp", "excludeSlashTmp"), ("exclude_tmpdir_env_var", "excludeTmpdirEnvVar")] {
                    if !w[from].is_null() {
                        p[to] = w[from].clone();
                    }
                }
            }
            Err(e) => eprintln!("[codex] config/read: {e}"),
        }
    }
    p
}

fn with(mut a: Value, b: Value) -> Value {
    for (k, v) in b.as_object().into_iter().flatten() {
        a[k] = v.clone();
    }
    a
}

// Opens a thread on the server as a host session, from the reply to thread/start, resume or fork.
fn opened(shared: &Shared, srv: &Server, r: &Value, cwd: &str, req: &Value, base: Option<(Option<String>, Option<String>)>) -> Result<String, String> {
    let id = r["thread"]["id"].as_str().ok_or("codex gave no thread id")?.to_string();
    let to = srv.to()?;
    let mut h = shared.lock().unwrap();
    let s = h.sessions.entry(id.clone()).or_insert_with(|| Session::new(&id, cwd, "codex"));
    if let Some((mode, model)) = base {
        (s.mode_req, s.model_req) = (mode, model);
    }
    take_requests(s, req);
    s.to_child = Some(to);
    s.gen = srv.gen;
    s.used = true;
    s.closing = None;
    s.model = r["model"].as_str().unwrap_or("").into();
    s.mode = s.mode_req.clone().unwrap_or_default();
    // the sandbox Codex runs the thread in, from its config: kept until a mode changes its kind
    s.native.insert("sandbox".into(), r["sandbox"].clone());
    s.last = Instant::now();
    h.announce(&id);
    Ok(id)
}

// Codex reopens a thread (resume, fork) through its thread history database; when that database
// does not open, the reply says so before Codex's own words.
fn reopen_error(what: &str, e: String) -> String {
    if !e.contains("thread history") {
        return e;
    }
    format!("Codex cannot {what} this session: it cannot open its thread history database. Codex says: {e}")
}

// The thread open on the server, resumed when it is not.
fn load(shared: &Shared, id: &str, cwd: &str, req: &Value) -> Result<Arc<Server>, String> {
    let (account, open, mode) = {
        let h = shared.lock().unwrap();
        match h.sessions.get(id) {
            Some(s) => (s.account.clone(), s.to_child.is_some(), s.mode_req.clone()),
            None => (req["account"].as_str().map(str::to_string), false, None),
        }
    };
    let srv = server(shared, account.as_deref())?;
    if !open {
        let mode = req["mode"].as_str().map(str::to_string).or(mode);
        let r = srv.call("thread/resume", with(json!({"threadId": id, "cwd": cwd, "excludeTurns": true}), policy(mode.as_deref(), false)))
            .map_err(|e| reopen_error("continue", e))?;
        opened(shared, &srv, &r, cwd, req, None)?;
    }
    Ok(srv)
}

pub fn start(shared: &Shared, req: &Value) -> Result<Value, String> {
    let cwd = start_cwd(req)?;
    let (id, srv) = match req["resume"].as_str() {
        Some(id) => (id.to_string(), load(shared, id, cwd, req)?),
        None => {
            let srv = server(shared, req["account"].as_str())?;
            let mut p = with(json!({"cwd": cwd}), policy(req["mode"].as_str(), false));
            if let Some(m) = req["model"].as_str() {
                p["model"] = json!(m);
            }
            let r = srv.call("thread/start", p)?;
            let id = opened(shared, &srv, &r, cwd, req, None)?;
            own(&id);
            (id, srv)
        }
    };
    skills(srv.clone(), shared.lock().unwrap().sessions.get(&id).and_then(|s| s.account.clone()), cwd);
    if let Some(t) = req["text"].as_str() {
        turn(shared, &srv, &id, t, &path_list(req, "images"), &path_list(req, "files"))?;
    }
    Ok(json!({"ev": "started", "id": id, "tool": "codex"}))
}

pub fn send(shared: &Shared, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("send needs an id")?;
    let text = req["text"].as_str().ok_or("send needs text")?;
    let cwd = shared.lock().unwrap().sessions.get(id).map(|s| s.cwd.clone()).ok_or(format!("no such session: {id}"))?;
    let srv = load(shared, id, &cwd, req)?;
    turn(shared, &srv, id, text, &path_list(req, "images"), &path_list(req, "files"))?;
    Ok(json!({"ev": "ok"}))
}

// A message as a new turn, or into the running one (turn/steer), on the server the caller loaded the
// thread on: looked up here, the servers lock was waited for with the host locked, behind a start.
// Files are named in the text, as @-mentions (Codex's mention input is for apps and plugins); images
// go as localImage.
fn turn(shared: &Shared, srv: &Server, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    let mut input = vec![json!({"type": "text", "text": crate::with_mentions(text, files)})];
    input.extend(images.iter().map(|p| json!({"type": "localImage", "path": p})));
    let (running, mut params, change, cwd) = {
        let mut h = shared.lock().unwrap();
        let s = h.sessions.get(id).ok_or("no such session")?;
        let running = s.turn.clone();
        let (change, cwd) = (sandbox_change(s), s.cwd.clone());
        let mut p = with(json!({"threadId": id, "input": input}), policy(s.mode_req.as_deref(), true));
        if let Some(m) = &s.model_req {
            p["model"] = json!(m);
        }
        if let Some(e) = &s.effort_req {
            p["effort"] = json!(e);
        }
        // marked working before the turn starts: a turn that ends at once must find it so
        sent(&mut h, id, text);
        h.announce(id);
        (running, p, change, cwd)
    };
    let mut sandbox = None;
    let r = if running.is_empty() {
        if let Some(kind) = change {
            let p = sandbox_policy(srv, kind, &cwd);
            params["sandboxPolicy"] = p.clone();
            sandbox = Some(p);
        }
        srv.call("turn/start", params)
    } else {
        srv.call("turn/steer", json!({"threadId": id, "expectedTurnId": running, "input": params["input"]}))
    };
    let mut h = shared.lock().unwrap();
    match r {
        Ok(r) => {
            // a turn that has already ended (its result clears turn_at) is not taken as running
            if let (Some(s), Some(t)) = (h.sessions.get_mut(id), r["turn"]["id"].as_str()) {
                if s.turn_at != 0 && s.turn.is_empty() {
                    s.turn = t.to_string();
                }
            }
            if let (Some(s), Some(p)) = (h.sessions.get_mut(id), sandbox) {
                s.native.insert("sandbox".into(), p);
            }
            Ok(())
        }
        Err(e) => {
            if let Some(s) = h.sessions.get_mut(id) {
                s.state = "idle";
                s.turn_at = 0;
            }
            h.announce(id);
            Err(e)
        }
    }
}

pub fn answer(h: &mut Host, id: &str, native: Value, allow: bool, always: bool) -> Result<(), String> {
    let decision = match (allow, always) {
        (true, true) => "acceptForSession",
        (true, false) => "accept",
        _ => "decline",
    };
    let s = h.sessions.get(id).ok_or("no such session")?;
    let to = s.to_child.as_ref().ok_or("session is not running")?;
    to.send(format!("{}\n", json!({"jsonrpc": "2.0", "id": native["id"], "result": {"decision": decision}})))
        .map_err(|_| "codex app-server has stopped".to_string())
}

pub fn interrupt(h: &mut Host, id: &str) -> Result<(), String> {
    let s = h.sessions.get(id).ok_or("no such session")?;
    let to = s.to_child.as_ref().ok_or("session is not running")?;
    if s.turn.is_empty() {
        return Err("nothing is running".into());
    }
    tell(to, "turn/interrupt", json!({"threadId": id, "turnId": s.turn}))
}

// The thread is let go of on the server, a running turn stopped first; the caller reports it
// (released).
pub fn unload(h: &mut Host, id: &str) {
    if let Some(s) = h.sessions.get(id) {
        if let Some(to) = &s.to_child {
            if !s.turn.is_empty() {
                let _ = tell(to, "turn/interrupt", json!({"threadId": id, "turnId": s.turn}));
            }
            let _ = tell(to, "thread/unsubscribe", json!({"threadId": id}));
        }
    }
}

// The folder's skills, for the catalog's `/` list (read on a thread; nothing waits for them).
fn skills(srv: Arc<Server>, account: Option<String>, cwd: &str) {
    let cwd = cwd.to_string();
    thread::spawn(move || match srv.call("skills/list", json!({"cwds": [cwd]})) {
        Ok(r) => crate::accounts::note_commands("codex", account.as_deref(), &cwd, r),
        Err(e) => eprintln!("[codex] skills/list: {e}"),
    });
}

// Servers with no thread open on them are closed now: after a sign-in, so the next thread starts
// one that reads the new auth.json, and when an account is removed.
pub fn drop_idle(h: &mut Host) {
    reap(h, Duration::ZERO);
}

// Servers with no thread open on them and no call for `idle` are closed: dropping the last sender
// closes the app-server's stdin, and it exits.
pub fn reap(h: &mut Host, idle: Duration) {
    let Ok(mut all) = servers().try_lock() else { return };
    let busy: Vec<u64> = h.sessions.values().filter(|s| s.tool == "codex" && s.to_child.is_some()).map(|s| s.gen).collect();
    all.retain(|_, s| {
        // a call under way (a thread loading) keeps it too
        let keep = busy.contains(&s.gen) || !s.waiting.lock().unwrap().is_empty() || s.last.lock().unwrap().elapsed() < idle;
        if !keep {
            s.tx.lock().unwrap().take();
            closing().lock().unwrap().push(s.clone());
        }
        keep
    });
}

fn session_server(shared: &Shared, id: &str) -> Result<Arc<Server>, String> {
    let account = shared.lock().unwrap().sessions.get(id).and_then(|s| s.account.clone());
    server(shared, account.as_deref())
}

pub fn rename(shared: &Shared, id: &str, title: &str) -> Result<(), String> {
    session_server(shared, id)?.call("thread/name/set", json!({"threadId": id, "name": title})).map(|_| ())
}

pub fn fork(shared: &Shared, from: &str, cwd: &str, req: &Value) -> Result<String, String> {
    let (account, mode, model) = shared.lock().unwrap().sessions.get(from)
        .map(|s| (s.account.clone(), s.mode_req.clone(), s.model_req.clone())).unwrap_or_default();
    let srv = server(shared, req["account"].as_str().or(account.as_deref()))?;
    let m = req["mode"].as_str().map(str::to_string).or(mode.clone());
    let r = srv.call("thread/fork", with(json!({"threadId": from, "cwd": cwd, "excludeTurns": true}), policy(m.as_deref(), false)))
        .map_err(|e| reopen_error("fork", e))?;
    opened(shared, &srv, &r, cwd, req, Some((mode, model)))
}

// Codex's own archive: the rollout moves to ~/.codex/archived_sessions and can be brought back.
pub fn archive(shared: &Shared, id: &str) -> Result<(), String> {
    match session_server(shared, id)?.call("thread/archive", json!({"threadId": id})) {
        // a thread nothing was sent to was never written
        Err(e) if e.starts_with("no rollout found") => Ok(()),
        r => r.map(|_| ()),
    }
}

// "/usr/bin/bash -lc 'touch ok.txt'" as the command it runs.
fn shell_text(cmd: &str) -> String {
    for flag in [" -lc ", " -c "] {
        if let Some((prog, rest)) = cmd.split_once(flag) {
            if ["bash", "sh", "zsh"].iter().any(|s| prog.ends_with(s)) {
                let r = rest.trim();
                if let Some(inner) = r.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')) {
                    return inner.replace(r"'\''", "'");
                }
                return r.to_string();
            }
        }
    }
    cmd.to_string()
}

// A unified diff as the text it replaces and the text it puts there.
pub(crate) fn split_diff(diff: &str) -> (String, String) {
    let (mut old, mut new) = (String::new(), String::new());
    for l in diff.lines() {
        if l.starts_with("---") || l.starts_with("+++") || l.starts_with("@@") {
            continue;
        }
        match l.as_bytes().first() {
            Some(b'-') => old += &format!("{}\n", &l[1..]),
            Some(b'+') => new += &format!("{}\n", &l[1..]),
            _ => {
                let t = l.strip_prefix(' ').unwrap_or(l);
                old += &format!("{t}\n");
                new += &format!("{t}\n");
            }
        }
    }
    (old, new)
}

// A fileChange item as Claude Code's Write (a new file) or Edit.
fn change_input(item: &Value) -> (&'static str, Value) {
    let changes = item["changes"].as_array().cloned().unwrap_or_default();
    let Some(c) = changes.first() else { return ("Edit", json!({})) };
    let path = c["path"].as_str().unwrap_or("");
    let diff = c["diff"].as_str().unwrap_or("");
    let mut out = if c["kind"]["type"] == "add" {
        ("Write", json!({"file_path": path, "content": diff}))
    } else {
        let (old, new) = split_diff(diff);
        ("Edit", json!({"file_path": path, "old_string": old, "new_string": new}))
    };
    if changes.len() > 1 {
        out.1["files"] = changes.iter().filter_map(|c| c["path"].as_str()).collect();
    }
    out
}

// A fileChange item as the patch apply_patch takes, which the app draws as Codex's edits.
fn patch_of(item: &Value) -> String {
    let mut patch = String::new();
    for c in item["changes"].as_array().into_iter().flatten() {
        let path = c["path"].as_str().unwrap_or("");
        let diff = c["diff"].as_str().unwrap_or("");
        match c["kind"]["type"].as_str().unwrap_or("") {
            "add" => {
                patch += &format!("*** Add File: {path}\n");
                patch += &diff.lines().map(|l| format!("+{l}\n")).collect::<String>();
            }
            "delete" => patch += &format!("*** Delete File: {path}\n"),
            _ => {
                patch += &format!("*** Update File: {path}\n");
                patch += &diff.lines().filter(|l| !l.starts_with("+++") && !l.starts_with("---")).map(|l| format!("{l}\n")).collect::<String>();
            }
        }
    }
    patch
}

fn assistant(id: &str, content: Value) -> Value {
    json!({"type": "assistant", "message": {"role": "assistant", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

fn user(id: &str, content: Value) -> Value {
    json!({"type": "user", "message": {"role": "user", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

fn delta(id: &str, d: Value) -> Value {
    json!({"type": "stream_event", "event": {"type": "content_block_delta", "index": 0, "delta": d}, "parent_tool_use_id": null, "session_id": id})
}

fn block_start(id: &str, kind: &str) -> Value {
    let block = if kind == "thinking" { json!({"type": "thinking", "thinking": ""}) } else { json!({"type": "text", "text": ""}) };
    json!({"type": "stream_event", "event": {"type": "content_block_start", "index": 0, "content_block": block}, "parent_tool_use_id": null, "session_id": id})
}

fn thread_of(p: &Value) -> Option<&str> {
    p["threadId"].as_str().or(p["thread"]["id"].as_str())
}

fn notification(h: &mut Host, gen: u64, v: Value) {
    let p = &v["params"];
    let Some(id) = thread_of(p).map(str::to_string) else { return };
    let Some(s) = h.sessions.get_mut(&id).filter(|s| s.tool == "codex" && s.gen == gen) else { return };
    let item = &p["item"];
    let iid = item["id"].as_str().unwrap_or("").to_string();
    let data = match v["method"].as_str().unwrap_or("") {
        "turn/started" => {
            s.turn = p["turn"]["id"].as_str().unwrap_or("").into();
            return;
        }
        "item/started" => match item["type"].as_str().unwrap_or("") {
            "commandExecution" => {
                s.native.insert(iid.clone(), item.clone());
                assistant(&id, json!([{"type": "tool_use", "id": iid, "name": "Bash",
                                       "input": {"command": shell_text(item["command"].as_str().unwrap_or(""))}}]))
            }
            "fileChange" => {
                s.native.insert(iid.clone(), item.clone());
                assistant(&id, json!([{"type": "tool_use", "id": iid, "name": "apply_patch", "input": {"input": patch_of(item)}}]))
            }
            "agentMessage" => block_start(&id, "text"),
            "reasoning" => block_start(&id, "thinking"),
            _ => return,
        },
        "item/agentMessage/delta" => delta(&id, json!({"type": "text_delta", "text": p["delta"]})),
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => delta(&id, json!({"type": "thinking_delta", "thinking": p["delta"]})),
        "item/completed" => {
            s.native.remove(&iid);
            match item["type"].as_str().unwrap_or("") {
                "agentMessage" => assistant(&id, json!([{"type": "text", "text": item["text"]}])),
                "reasoning" => {
                    let parts: Vec<&str> = item["summary"].as_array().filter(|a| !a.is_empty()).or(item["content"].as_array())
                        .into_iter().flatten().filter_map(Value::as_str).collect();
                    if parts.is_empty() {
                        return;
                    }
                    assistant(&id, json!([{"type": "thinking", "thinking": parts.join("\n\n")}]))
                }
                "commandExecution" => {
                    let failed = item["exitCode"].as_i64().is_some_and(|c| c != 0) || matches!(item["status"].as_str(), Some("failed" | "declined"));
                    user(&id, json!([{"type": "tool_result", "tool_use_id": iid, "content": item["aggregatedOutput"].as_str().unwrap_or(""), "is_error": failed}]))
                }
                "fileChange" => {
                    let failed = matches!(item["status"].as_str(), Some("failed" | "declined"));
                    user(&id, json!([{"type": "tool_result", "tool_use_id": iid, "content": item["status"], "is_error": failed}]))
                }
                "userMessage" => {
                    let text: Vec<&str> = item["content"].as_array().into_iter().flatten().filter(|c| c["type"] == "text").filter_map(|c| c["text"].as_str()).collect();
                    user(&id, json!(text.join("\n")))
                }
                _ => return,
            }
        }
        "turn/completed" => {
            s.turn.clear();
            let t = &p["turn"];
            let last = t["items"].as_array().into_iter().flatten().rev().find(|i| i["type"] == "agentMessage").and_then(|i| i["text"].as_str()).unwrap_or("");
            let (subtype, error) = match t["status"].as_str().unwrap_or("") {
                "completed" => ("success", false),
                "interrupted" => ("error_during_execution", false),
                _ => ("error_during_execution", true),
            };
            let text = if error { t["error"]["message"].as_str().unwrap_or("The turn failed.") } else { last };
            json!({"type": "result", "subtype": subtype, "is_error": error, "result": text, "session_id": id})
        }
        // an approval the server settled itself (the turn ended, or a reviewer answered)
        "serverRequest/resolved" => {
            let rid = &p["requestId"];
            let gone: Vec<String> = s.native.iter().filter(|(k, n)| k.starts_with("req:") && n["id"] == *rid).map(|(k, _)| k[4..].to_string()).collect();
            if gone.is_empty() {
                return;
            }
            for r in &gone {
                s.native.remove(&format!("req:{r}"));
                s.pending.retain(|x| x["request"] != json!(r));
            }
            if s.pending.is_empty() && s.state == "waiting" {
                s.state = "working";
            }
            h.announce(&id);
            return;
        }
        _ => return,
    };
    relay(h, &id, gen, data, Some(v.clone()));
}

fn server_request(h: &mut Host, srv: &Server, v: Value) {
    let p = &v["params"];
    let method = v["method"].as_str().unwrap_or("");
    let open = thread_of(p).and_then(|id| h.sessions.get(id)).filter(|s| s.tool == "codex" && s.gen == srv.gen);
    let asked = match (method, open) {
        ("item/commandExecution/requestApproval", Some(_)) => {
            let mut input = json!({"command": shell_text(p["command"].as_str().unwrap_or(""))});
            if let Some(r) = p["reason"].as_str() {
                input["description"] = json!(r);
            }
            Some(("Bash", input))
        }
        ("item/fileChange/requestApproval", Some(s)) => {
            let item = s.native.get(p["itemId"].as_str().unwrap_or("")).cloned().unwrap_or(Value::Null);
            Some(change_input(&item))
        }
        _ => None,
    };
    match asked {
        Some((tool, input)) => {
            let id = thread_of(p).unwrap_or("").to_string();
            ask(h, &id, srv.gen, tool, input, json!({"id": v["id"], "method": method}), v.clone());
        }
        // questions, MCP prompts and the like are not relayed: answered rather than left hanging
        None => {
            let _ = srv.to().map(|to| to.send(format!("{}\n", json!({"jsonrpc": "2.0", "id": v["id"], "error": {"code": -32601, "message": "not handled by agent-host"}}))));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Host;

    fn host() -> Shared {
        Arc::new(Mutex::new(Host::default()))
    }

    // A server as server() makes one, whose writes go to the returned receiver.
    fn fake_server(key: &str) -> (Arc<Server>, std::sync::mpsc::Receiver<String>) {
        let (to, written) = channel();
        let srv = Server { key: key.into(), gen: GEN.fetch_add(1, Ordering::Relaxed), tx: Mutex::new(Some(to)), waiting: Mutex::new(HashMap::new()),
                           next: AtomicU64::new(1), last: Mutex::new(Instant::now()), stderr: Mutex::new(VecDeque::new()), done: AtomicBool::new(false) };
        (Arc::new(srv), written)
    }

    // An app-server that exits at start (a -c value it cannot read, a crash, a codex systemd-run cannot find) fails
    // its start at once and says why, although server() holds the servers lock throughout: the exit's reader took
    // that lock before answering the waiting initialize, so the start waited 90 s and said only that it got no answer.
    #[test]
    fn a_server_that_exits_at_start_fails_at_once_with_why() {
        let shared = host();
        let (tx, rx) = channel();
        thread::spawn(move || {
            let _all = servers().lock().unwrap();
            let mut cmd = Command::new("sh");
            cmd.args(["-c", "echo 'Error: invalid type: string \"[\", expected a sequence' >&2; echo 'in `notify`' >&2; exit 1"]);
            let _ = tx.send(launch(&shared, "exits-at-start", GEN.fetch_add(1, Ordering::Relaxed), cmd).map(|_| ()));
        });
        let r = rx.recv_timeout(Duration::from_secs(20)).expect("the start still waits for an answer after its server exited");
        let e = r.unwrap_err();
        assert!(e.starts_with("codex app-server has stopped") && e.ends_with("expected a sequence\nin `notify`"), "{e}");
    }

    // A message to a running thread never waits on the servers lock with the host locked: a Codex start holds that
    // lock for up to 95 s (a closing server, then initialize), and the turn that waited for it under the host lock
    // stalled every session's reader and every client of the host with it.
    #[test]
    fn a_turn_does_not_wait_on_a_start_with_the_host_locked() {
        let id = "019a0000-0000-7000-8000-00000000c001";
        let shared = host();
        let (srv, written) = fake_server("");
        {
            let mut h = shared.lock().unwrap();
            let mut s = Session::new(id, "/", "codex");
            (s.gen, s.to_child) = (srv.gen, srv.to().ok());
            h.sessions.insert(id.into(), s);
        }
        // Codex answers turn/start
        let codex = srv.clone();
        thread::spawn(move || {
            for l in written {
                let v: Value = serde_json::from_str(&l).unwrap();
                if let Some(tx) = v["id"].as_u64().and_then(|n| codex.waiting.lock().unwrap().remove(&n)) {
                    let _ = tx.send(Ok(json!({"turn": {"id": "turn-1"}})));
                }
            }
        });
        // a start under way for another account
        let (held, holding) = channel();
        let (end, ended) = channel::<()>();
        thread::spawn(move || {
            let _all = servers().lock().unwrap();
            let _ = held.send(());
            let _ = ended.recv_timeout(Duration::from_secs(30));
        });
        holding.recv().unwrap();
        let (sh, s2, (tx, rx)) = (shared.clone(), srv.clone(), channel());
        thread::spawn(move || {
            let _ = tx.send(turn(&sh, &s2, id, "and then?", &[], &[]));
        });
        let r = rx.recv_timeout(Duration::from_secs(10)).expect("the turn waited on the servers lock with the host locked");
        let _ = end.send(());
        assert_eq!(r, Ok(()));
        assert_eq!(shared.lock().unwrap().sessions[id].turn, "turn-1");
    }

    // A turn keeps the sandbox Codex built for the thread from the person's config (network access, extra writable
    // folders): every turn/start sent a bare {"type": "workspaceWrite"}, which replaces it for that turn and the
    // next. A mode that changes the sandbox's kind still changes it, a workspace-write one filled from the config.
    #[test]
    fn a_turn_keeps_the_sandbox_codex_made_from_the_config() {
        let id = "019a0000-0000-7000-8000-00000000c002";
        let shared = host();
        let (srv, written) = fake_server("sandbox-test");
        let configured = json!({"type": "workspaceWrite", "networkAccess": true, "writableRoots": ["/srv/extra"],
                                "excludeSlashTmp": false, "excludeTmpdirEnvVar": false});
        // Codex answers config/read with the person's [sandbox_workspace_write] and notes every turn/start
        let (codex, (starts, started)) = (srv.clone(), channel());
        thread::spawn(move || {
            for l in written {
                let v: Value = serde_json::from_str(&l).unwrap();
                let answer = match v["method"].as_str().unwrap_or("") {
                    "config/read" => json!({"config": {"sandbox_workspace_write": {"network_access": true, "writable_roots": ["/srv/extra"],
                                                                                   "exclude_slash_tmp": false, "exclude_tmpdir_env_var": false}}}),
                    "turn/start" => {
                        let _ = starts.send(v["params"].clone());
                        json!({"turn": {"id": "turn-1"}})
                    }
                    _ => json!({}),
                };
                if let Some(tx) = v["id"].as_u64().and_then(|n| codex.waiting.lock().unwrap().remove(&n)) {
                    let _ = tx.send(Ok(answer));
                }
            }
        });
        let reply = json!({"thread": {"id": id}, "model": "gpt-5", "sandbox": configured});
        opened(&shared, &srv, &reply, "/srv/w", &json!({"mode": "acceptEdits"}), None).unwrap();
        let send = |mode: &str| {
            {
                let mut h = shared.lock().unwrap();
                let s = h.sessions.get_mut(id).unwrap();
                s.mode_req = Some(mode.into());
                s.turn.clear(); // the last turn has ended
            }
            turn(&shared, &srv, id, "go on", &[], &[]).unwrap();
            started.recv_timeout(Duration::from_secs(5)).unwrap()
        };
        let p = send("acceptEdits");
        assert_eq!(p["approvalPolicy"], "on-request");
        assert!(p.get("sandboxPolicy").is_none(), "a turn replaced the thread's sandbox: {p}");
        assert_eq!(send("plan")["sandboxPolicy"], json!({"type": "readOnly"}));
        assert!(send("plan").get("sandboxPolicy").is_none());
        assert_eq!(send("acceptEdits")["sandboxPolicy"], configured, "back to edits without the config's network and folders");
        assert!(send("auto").get("sandboxPolicy").is_none(), "auto runs in the same kind");
    }
}
