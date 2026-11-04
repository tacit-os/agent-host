//! Codex sessions: threads on one `codex app-server` (JSON-RPC lines on stdio) per account,
//! started on first use and closed once no thread is open on it and it has had no call for
//! AGENT_HOST_IDLE. Its notifications are relayed in Claude Code's shape, and its approval
//! requests are raised as can_use_tool. Following a thread reads its rollout file (history.rs);
//! thread/read is never called.

use crate::{agent_command, ask, in_scope, own, path_list, relay, released, sent, start_cwd, take_requests, write_in, Host, Session, Shared};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
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
    let mut cmd = in_scope(&cmd, &format!("agent-codex-{}-{gen}", process::id()));
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start codex: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let srv = Arc::new(Server { key: key.clone(), gen, tx: Mutex::new(Some(write_in(child.stdin.take().unwrap(), "codex-ap"))), waiting: Mutex::new(HashMap::new()),
                                next: AtomicU64::new(1), last: Mutex::new(Instant::now()), stderr: Mutex::new(VecDeque::new()),
                                done: AtomicBool::new(false) });
    let (a, b) = (srv.clone(), srv.clone());
    let sh = shared.clone();
    thread::spawn(move || read_server(sh, a, stdout, child));
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
    });
    srv.call("initialize", json!({"clientInfo": {"name": "agent-host", "version": env!("CARGO_PKG_VERSION")}}))?;
    srv.to()?.send(format!("{}\n", json!({"jsonrpc": "2.0", "method": "initialized"}))).map_err(|e| e.to_string())?;
    all.insert(key, srv.clone());
    Ok(srv)
}

fn read_server(shared: Shared, srv: Arc<Server>, out: ChildStdout, mut child: Child) {
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
    srv.done.store(true, Ordering::Relaxed);
    {
        let mut all = servers().lock().unwrap();
        if all.get(&srv.key).is_some_and(|s| Arc::ptr_eq(s, &srv)) {
            all.remove(&srv.key);
        }
    }
    for (_, tx) in srv.waiting.lock().unwrap().drain() {
        let _ = tx.send(Err("codex app-server has stopped".into()));
    }
    // the threads still open on it end with it
    let stderr: Vec<String> = srv.stderr.lock().unwrap().iter().cloned().collect();
    let mut h = shared.lock().unwrap();
    let open: Vec<String> = h.sessions.values().filter(|s| s.tool == "codex" && s.gen == srv.gen && s.to_child.is_some()).map(|s| s.id.clone()).collect();
    for id in open {
        released(&mut h, &id, if code == Some(0) { "exited" } else { "crashed" }, stderr.clone());
    }
}

// A thread's mode as Codex's approval policy, sandbox and reviewer. `turn`: in turn/start's shape
// (sandboxPolicy) rather than thread/start's (sandbox).
fn policy(mode: Option<&str>, turn: bool) -> Value {
    let (approval, sandbox, reviewer) = match mode {
        Some("manual" | "default") => ("untrusted", "workspace-write", "user"),
        Some("acceptEdits") => ("on-request", "workspace-write", "user"),
        Some("auto") => ("on-request", "workspace-write", "auto_review"),
        Some("plan") => ("on-request", "read-only", "user"),
        _ => return json!({}),
    };
    let mut p = json!({"approvalPolicy": approval});
    if turn {
        p["sandboxPolicy"] = json!({"type": if sandbox == "read-only" { "readOnly" } else { "workspaceWrite" }});
    } else {
        p["sandbox"] = json!(sandbox);
    }
    // set every time: the user's own config may name another reviewer
    p["approvalsReviewer"] = json!(reviewer);
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
    skills(srv, shared.lock().unwrap().sessions.get(&id).and_then(|s| s.account.clone()), cwd);
    if let Some(t) = req["text"].as_str() {
        turn(shared, &id, t, &path_list(req, "images"), &path_list(req, "files"))?;
    }
    Ok(json!({"ev": "started", "id": id, "tool": "codex"}))
}

pub fn send(shared: &Shared, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("send needs an id")?;
    let text = req["text"].as_str().ok_or("send needs text")?;
    let cwd = shared.lock().unwrap().sessions.get(id).map(|s| s.cwd.clone()).ok_or(format!("no such session: {id}"))?;
    load(shared, id, &cwd, req)?;
    turn(shared, id, text, &path_list(req, "images"), &path_list(req, "files"))?;
    Ok(json!({"ev": "ok"}))
}

// A message as a new turn, or into the running one (turn/steer). Files are named in the text, as
// @-mentions (Codex's mention input is for apps and plugins); images go as localImage.
fn turn(shared: &Shared, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    let mut input = vec![json!({"type": "text", "text": crate::with_mentions(text, files)})];
    input.extend(images.iter().map(|p| json!({"type": "localImage", "path": p})));
    let (srv, running, params) = {
        let mut h = shared.lock().unwrap();
        let s = h.sessions.get(id).ok_or("no such session")?;
        let running = s.turn.clone();
        let mut p = with(json!({"threadId": id, "input": input}), policy(s.mode_req.as_deref(), true));
        if let Some(m) = &s.model_req {
            p["model"] = json!(m);
        }
        if let Some(e) = &s.effort_req {
            p["effort"] = json!(e);
        }
        let key = s.account.clone().unwrap_or_default();
        // marked working before the turn starts: a turn that ends at once must find it so
        sent(&mut h, id, text);
        h.announce(id);
        (servers().lock().unwrap().get(&key).cloned(), running, p)
    };
    let srv = srv.ok_or("codex app-server has stopped")?;
    let r = if running.is_empty() {
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
