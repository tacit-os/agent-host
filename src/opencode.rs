//! opencode sessions: sessions on one `opencode serve` per account, on 127.0.0.1 at a free port
//! behind a random password, started on demand and stopped once no session is open on it and it
//! has had no call for AGENT_HOST_IDLE. HTTP/1.1 is spoken on std::net. Its event stream
//! (/global/event, every folder on one connection) is relayed in Claude Code's shape, and its
//! permission requests are raised as can_use_tool. Following a session reads opencode's database
//! (history.rs).

use crate::codex::split_diff;
use crate::{agent_command, ask, in_scope, now_ms, own, path_list, relay, released, sent, start_cwd, take_requests, Host, Session, Shared};
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::{fs, process, thread};

pub struct Server {
    gen: u64,
    port: u16,
    auth: String, // the Authorization header's value; never logged
    pid: u32,
    // requests sent without waiting (prompts, answers, aborts), made one at a time in order by the
    // server's poster thread; taken when the server is stopped
    tx: Mutex<Option<Sender<String>>>,
    last: Mutex<Instant>, // when its last request ended
    calls: AtomicU64,     // requests under way: a server is not idle while one runs
    done: AtomicBool,
    v2: bool,             // opencode 2's API (opencode_v2.rs), read from the program's version
}

fn servers() -> &'static Mutex<HashMap<String, Arc<Server>>> {
    static S: OnceLock<Mutex<HashMap<String, Arc<Server>>>> = OnceLock::new();
    S.get_or_init(Default::default)
}

static GEN: AtomicU64 = AtomicU64::new(2 << 32);

// The servers that speak 2, by generation: read with the host's lock held, so never behind servers(), which a
// starting server holds for as long as it takes to start.
fn v2_gens() -> &'static Mutex<std::collections::HashSet<u64>> {
    static G: OnceLock<Mutex<std::collections::HashSet<u64>>> = OnceLock::new();
    G.get_or_init(Default::default)
}

// A path's query: the folder a session belongs to.
fn dir_query(cwd: &str) -> String {
    format!("?directory={}", escaped(cwd))
}

// An attached file as opencode reads it back (fileURLToPath): a '#', '?' or '%' left in the path cut it short or failed.
fn file_url(path: &str) -> String {
    format!("file://{}", escaped(path))
}

// `s` percent-encoded, but for letters, digits, '/' and -_.~
fn escaped(s: &str) -> String {
    let mut q = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            q.push(b as char);
        } else {
            q += &format!("%{b:02X}");
        }
    }
    q
}

impl Server {
    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let r = http(self.port, &self.auth, method, path, body);
        *self.last.lock().unwrap() = Instant::now();
        self.calls.fetch_sub(1, Ordering::Relaxed);
        r
    }
}

// One request on a connection of its own. The reply is read to its length (or last chunk): the
// server does not always close the connection after it, Connection: close notwithstanding.
pub(crate) fn http(port: u16, auth: &str, method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
    let err = |e: std::io::Error| format!("opencode: {e}");
    let mut s = TcpStream::connect(("127.0.0.1", port)).map_err(err)?;
    let _ = s.set_read_timeout(Some(Duration::from_secs(60)));
    let body = body.map(Value::to_string).unwrap_or_default();
    write!(s, "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: {auth}\r\nConnection: close\r\n\
               Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).map_err(err)?;
    let mut r = BufReader::new(s);
    let mut line = String::new();
    r.read_line(&mut line).map_err(err)?;
    let status: u16 = line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).ok_or("opencode sent no status")?;
    let (mut length, mut chunked) = (None, false);
    loop {
        line.clear();
        if r.read_line(&mut line).map_err(err)? == 0 || line == "\r\n" {
            break;
        }
        let l = line.to_ascii_lowercase();
        if let Some(n) = l.strip_prefix("content-length:") {
            length = n.trim().parse::<usize>().ok();
        }
        chunked |= l.starts_with("transfer-encoding:") && l.contains("chunked");
    }
    let mut bytes = Vec::new();
    if chunked {
        loop {
            line.clear();
            r.read_line(&mut line).map_err(err)?;
            let n = usize::from_str_radix(line.trim(), 16).unwrap_or(0);
            if n == 0 {
                break;
            }
            let mut chunk = vec![0u8; n + 2];
            r.read_exact(&mut chunk).map_err(err)?;
            bytes.extend_from_slice(&chunk[..n]);
        }
    } else if let Some(n) = length {
        bytes.resize(n, 0);
        r.read_exact(&mut bytes).map_err(err)?;
    } else {
        r.read_to_end(&mut bytes).map_err(err)?;
    }
    let v: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    if !(200..300).contains(&status) {
        let msg = v["data"]["message"].as_str().or(v["message"].as_str()).or(v["error"].as_str()).map(str::to_string)
            .unwrap_or_else(|| format!("opencode answered {status}"));
        return Err(msg);
    }
    Ok(v)
}

fn server(shared: &Shared, account: Option<&str>) -> Result<Arc<Server>, String> {
    let key = account.unwrap_or("").to_string();
    let mut all = servers().lock().unwrap();
    if let Some(s) = all.get(&key).filter(|s| !s.done.load(Ordering::Relaxed)) {
        *s.last.lock().unwrap() = Instant::now();
        return Ok(s.clone());
    }
    let gen = GEN.fetch_add(1, Ordering::Relaxed);
    let port = TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map_err(|e| e.to_string())?.port();
    let mut secret = [0u8; 18];
    fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut secret)).map_err(|e| e.to_string())?;
    let password = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}")));
    let mut cmd = agent_command("opencode", account)?;
    let v2 = crate::opencode_v2::major(&cmd) >= 2;
    cmd.args(["serve", "--hostname", "127.0.0.1", "--port", &port.to_string()])
        .env("OPENCODE_SERVER_PASSWORD", &password)
        .current_dir(crate::home());
    let mut cmd = in_scope(&cmd, &format!("agent-opencode-{}-{gen}", process::id()));
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start opencode: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let (ready_tx, ready) = channel::<()>();
    thread::spawn(move || crate::each_line(stdout, |line| {
        if line.contains("listening on") {
            let _ = ready_tx.send(());
        }
    }));
    thread::spawn(move || crate::each_line(stderr, |line| eprintln!("[opencode] {}", crate::keys::redact(&line))));
    let (tx, rx) = channel::<String>();
    if v2 {
        v2_gens().lock().unwrap().insert(gen);
    }
    let srv = Arc::new(Server { gen, port, auth, pid: child.id(), tx: Mutex::new(Some(tx)),
                                last: Mutex::new(Instant::now()), calls: AtomicU64::new(0), done: AtomicBool::new(false), v2 });
    let mut failed = |e: &str| {
        let _ = child.kill();
        let _ = child.wait();
        Err(e.to_string())
    };
    if ready.recv_timeout(Duration::from_secs(30)).is_err() {
        return failed("opencode serve did not start");
    }
    // events before anything is sent: a prompt's first events must not be missed
    let (on_tx, on) = channel::<()>();
    let (sh, s2) = (shared.clone(), srv.clone());
    thread::spawn(move || read_events(sh, s2, on_tx));
    if on.recv_timeout(Duration::from_secs(15)).is_err() {
        return failed("opencode's event stream did not open");
    }
    let (sh, s2) = (shared.clone(), srv.clone());
    thread::spawn(move || poster(sh, s2, rx));
    let (sh, s2) = (shared.clone(), srv.clone());
    thread::spawn(move || {
        let code = child.wait().ok().and_then(|s| s.code());
        s2.done.store(true, Ordering::Relaxed);
        s2.tx.lock().unwrap().take();
        let mut h = sh.lock().unwrap();
        let open: Vec<String> = h.sessions.values().filter(|s| s.tool == "opencode" && s.gen == s2.gen && s.to_child.is_some()).map(|s| s.id.clone()).collect();
        for id in open {
            released(&mut h, &id, if code == Some(0) { "exited" } else { "crashed" }, Vec::new());
        }
    });
    all.insert(key, srv.clone());
    Ok(srv)
}

// The requests sessions send without waiting, in order. A prompt opencode refuses ends the turn
// with its error, as Claude Code's result would.
fn poster(shared: Shared, srv: Arc<Server>, rx: Receiver<String>) {
    for line in rx {
        let Ok(r) = serde_json::from_str::<Value>(&line) else { continue };
        let body = if r["body"].is_null() { None } else { Some(&r["body"]) };
        if let Err(e) = srv.request(r["method"].as_str().unwrap_or("POST"), r["path"].as_str().unwrap_or("/"), body) {
            eprintln!("[opencode] {} {}: {e}", r["method"], r["path"]);
            if r["what"] == "prompt" {
                let id = r["session"].as_str().unwrap_or("");
                let mut h = shared.lock().unwrap();
                relay(&mut h, id, srv.gen, json!({"type": "result", "subtype": "error_during_execution", "is_error": true, "result": e, "session_id": id}), None);
            }
        }
    }
}

// The server's event stream: SSE, in a chunked body or not. 1.x: /global/event, each event in a `payload`;
// 2: /api/event, the event itself.
fn read_events(shared: Shared, srv: Arc<Server>, on: Sender<()>) {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", srv.port)) else { return };
    let path = if srv.v2 { "/api/event" } else { "/global/event" };
    if write!(s, "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAuthorization: {}\r\nAccept: text/event-stream\r\n\r\n", srv.port, srv.auth).is_err() {
        return;
    }
    let mut r = BufReader::new(s);
    let mut line = String::new();
    let mut chunked = false;
    loop {
        line.clear();
        if r.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        if line == "\r\n" {
            break;
        }
        let l = line.to_ascii_lowercase();
        chunked |= l.starts_with("transfer-encoding:") && l.contains("chunked");
    }
    let mut buf: Vec<u8> = Vec::new();
    loop {
        line.clear();
        if r.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if chunked {
            let Ok(n) = usize::from_str_radix(line.trim(), 16) else { continue };
            if n == 0 {
                break;
            }
            let mut chunk = vec![0u8; n + 2];
            if r.read_exact(&mut chunk).is_err() {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        } else {
            buf.extend_from_slice(line.as_bytes());
        }
        while let Some(eol) = buf.iter().position(|&b| b == b'\n') {
            let l: Vec<u8> = buf.drain(..=eol).collect();
            let Some(data) = l.strip_prefix(b"data: ") else { continue };
            let Ok(v) = serde_json::from_slice::<Value>(data) else { continue };
            let p = if srv.v2 { &v } else { &v["payload"] };
            match p["type"].as_str().unwrap_or("") {
                "server.connected" => {
                    let _ = on.send(());
                }
                // the stream's own bookkeeping, and catalogue noise at startup
                "sync" | "plugin.added" | "catalog.updated" | "" => {}
                _ if srv.v2 => crate::opencode_v2::event(&mut shared.lock().unwrap(), srv.gen, p),
                _ => event(&mut shared.lock().unwrap(), srv.gen, p),
            }
        }
    }
}

pub(crate) fn assistant(id: &str, content: Value) -> Value {
    json!({"type": "assistant", "message": {"role": "assistant", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

pub(crate) fn user(id: &str, content: Value) -> Value {
    json!({"type": "user", "message": {"role": "user", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

pub(crate) fn stream(id: &str, e: Value) -> Value {
    json!({"type": "stream_event", "event": e, "parent_tool_use_id": null, "session_id": id})
}

// opencode's tools under Claude Code's names and input keys.
fn tool_of(name: &str, i: &Value) -> (String, Value) {
    match name {
        "bash" => ("Bash".into(), json!({"command": i["command"], "description": i["description"]})),
        "read" => ("Read".into(), json!({"file_path": i["filePath"]})),
        "edit" => ("Edit".into(), json!({"file_path": i["filePath"], "old_string": i["oldString"], "new_string": i["newString"]})),
        "write" => ("Write".into(), json!({"file_path": i["filePath"], "content": i["content"]})),
        "grep" => ("Grep".into(), json!({"pattern": i["pattern"], "path": i["path"]})),
        "glob" => ("Glob".into(), json!({"pattern": i["pattern"], "path": i["path"]})),
        "webfetch" => ("WebFetch".into(), json!({"url": i["url"]})),
        "todowrite" => ("TodoWrite".into(), json!({"todos": i["todos"]})),
        "task" => ("Agent".into(), json!({"description": i["description"], "prompt": i["prompt"], "subagent_type": i["subagent_type"]})),
        _ => (name.to_string(), i.clone()),
    }
}

// A permission request as the tool Claude Code would have asked about.
fn permission_input(p: &Value) -> (String, Value) {
    let m = &p["metadata"];
    let first = p["patterns"][0].as_str().unwrap_or("");
    match p["permission"].as_str().unwrap_or("") {
        "bash" => ("Bash".into(), json!({"command": m["command"].as_str().unwrap_or(first)})),
        "edit" => {
            let path = m["filepath"].as_str().or(m["filePath"].as_str()).unwrap_or(first);
            match m["diff"].as_str() {
                Some(d) => {
                    // from its first hunk: the "Index:" and "====" lines before it are no file's text
                    let (old, new) = split_diff(d.find("\n@@").map(|i| &d[i + 1..]).unwrap_or(""));
                    ("Edit".into(), json!({"file_path": path, "old_string": old, "new_string": new}))
                }
                None => ("Write".into(), json!({"file_path": path, "content": m["content"]})),
            }
        }
        other => (other.to_string(), m.clone()),
    }
}

fn event(h: &mut Host, gen: u64, p: &Value) {
    let pr = &p["properties"];
    let part = &pr["part"];
    let Some(id) = pr["sessionID"].as_str().or(part["sessionID"].as_str()).or(pr["info"]["sessionID"].as_str()).map(str::to_string) else { return };
    let Some(s) = h.sessions.get_mut(&id).filter(|s| s.tool == "opencode" && s.gen == gen && s.to_child.is_some()) else { return };
    let ty = p["type"].as_str().unwrap_or("");
    let mut out: Vec<Value> = Vec::new();
    match ty {
        "message.updated" => {
            let info = &pr["info"];
            if let (Some(m), Some(r)) = (info["id"].as_str(), info["role"].as_str()) {
                s.native.insert(format!("msg:{m}"), json!(r));
            }
            if let Some(e) = info["error"].as_object() {
                s.native.insert("error".into(), json!(e));
            }
        }
        "message.part.updated" => {
            let pid = part["id"].as_str().unwrap_or("").to_string();
            let kind = part["type"].as_str().unwrap_or("");
            let role = s.native.get(&format!("msg:{}", part["messageID"].as_str().unwrap_or(""))).and_then(|r| r.as_str()).unwrap_or("assistant").to_string();
            let fresh = s.native.insert(format!("part:{pid}"), json!(kind)).is_none();
            let ended = !part["time"]["end"].is_null();
            match (role.as_str(), kind) {
                ("user", "text") if fresh && part["synthetic"] != true => out.push(user(&id, part["text"].clone())),
                ("user", _) => {}
                (_, "step-start") => out.push(stream(&id, json!({"type": "message_start", "message": {}}))),
                (_, "text" | "reasoning") => {
                    let thinking = kind == "reasoning";
                    if fresh {
                        let block = if thinking { json!({"type": "thinking", "thinking": ""}) } else { json!({"type": "text", "text": ""}) };
                        out.push(stream(&id, json!({"type": "content_block_start", "index": 0, "content_block": block})));
                    }
                    if ended && s.native.insert(format!("done:{pid}"), json!(true)).is_none() {
                        let t = part["text"].as_str().unwrap_or("");
                        if !t.trim().is_empty() {
                            if !thinking {
                                s.native.insert("last".into(), json!(t));
                            }
                            out.push(assistant(&id, if thinking { json!([{"type": "thinking", "thinking": t}]) } else { json!([{"type": "text", "text": t}]) }));
                        }
                    }
                }
                (_, "tool") => {
                    let call = part["callID"].as_str().unwrap_or(&pid).to_string();
                    let st = &part["state"];
                    let status = st["status"].as_str().unwrap_or("");
                    if status != "pending" && s.native.insert(format!("call:{call}"), json!(true)).is_none() {
                        let (name, input) = tool_of(part["tool"].as_str().unwrap_or(""), &st["input"]);
                        out.push(assistant(&id, json!([{"type": "tool_use", "id": call, "name": name, "input": input}])));
                    }
                    if matches!(status, "completed" | "error") && s.native.insert(format!("result:{call}"), json!(true)).is_none() {
                        let text = st["output"].as_str().or(st["error"].as_str()).unwrap_or("");
                        out.push(user(&id, json!([{"type": "tool_result", "tool_use_id": call, "content": text, "is_error": status == "error"}])));
                    }
                }
                _ => {}
            }
        }
        "message.part.delta" => {
            let pid = pr["partID"].as_str().unwrap_or("");
            let d = pr["delta"].as_str().unwrap_or("");
            match s.native.get(&format!("part:{pid}")).and_then(|k| k.as_str()) {
                Some("reasoning") => out.push(stream(&id, json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": d}}))),
                Some("text") | None if pr["field"] == "text" => out.push(stream(&id, json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": d}}))),
                _ => {}
            }
        }
        "permission.asked" => {
            let (tool, input) = permission_input(pr);
            ask(h, &id, gen, &tool, input, json!({"id": pr["id"]}), p.clone());
            return;
        }
        // answered somewhere else, or settled by opencode itself
        "permission.replied" => {
            let rid = &pr["requestID"];
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
        "session.error" => {
            s.native.insert("error".into(), pr["error"].clone());
        }
        "session.idle" => {
            if s.state == "idle" {
                return;
            }
            let err = s.native.remove("error").unwrap_or(Value::Null);
            let last = s.native.remove("last").and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
            s.native.retain(|k, _| k.starts_with("req:"));
            let aborted = err["name"] == "MessageAbortedError";
            let failed = !err.is_null() && !aborted;
            let text = if failed { err["data"]["message"].as_str().or(err["message"].as_str()).unwrap_or("The turn failed.").to_string() } else { last };
            let mut r = json!({"type": "result", "subtype": if err.is_null() { "success" } else { "error_during_execution" },
                               "is_error": failed, "result": text, "session_id": id});
            // its provider refused the sign-in or key: the host ends the session as signed out
            if err["name"] == "ProviderAuthError" {
                r["authError"] = json!(true);
            }
            out.push(r);
        }
        _ => {}
    }
    for data in out {
        relay(h, &id, gen, data, Some(p.clone()));
    }
}

// A mode as opencode's permission rules, and the agent it runs with.
fn rules(mode: Option<&str>) -> (Value, &'static str) {
    let r = |bash: &str, edit: &str| json!([{"permission": "bash", "pattern": "*", "action": bash}, {"permission": "edit", "pattern": "*", "action": edit}]);
    match mode {
        Some("plan") => (r("ask", "deny"), "plan"),
        // opencode has no judge of its own: Auto lets edits through and asks before commands
        Some("acceptEdits" | "auto") => (r("ask", "allow"), "build"),
        _ => (r("ask", "ask"), "build"),
    }
}

// A mode's rules on a session: one opencode made elsewhere or forked has none of the host's, and a
// mode change must reach them. opencode adds them after the rules it has; the last match wins.
fn set_rules(srv: &Server, id: &str, cwd: &str, mode: Option<&str>) -> Result<(), String> {
    if srv.v2 {
        return srv.request("PATCH", &format!("/api/session/{id}"), Some(&json!({"permissions": crate::opencode_v2::rules(mode).0}))).map(|_| ());
    }
    srv.request("PATCH", &format!("/session/{id}{}", dir_query(cwd)), Some(&json!({"permission": rules(mode).0}))).map(|_| ())
}

// Opens a session on the server as a host session.
fn opened(shared: &Shared, srv: &Server, id: &str, cwd: &str, req: &Value, base: Option<(Option<String>, Option<String>)>) -> Result<(), String> {
    let to = srv.tx.lock().unwrap().clone().ok_or("opencode has stopped")?;
    let mut h = shared.lock().unwrap();
    let s = h.sessions.entry(id.to_string()).or_insert_with(|| Session::new(id, cwd, "opencode"));
    if let Some((mode, model)) = base {
        (s.mode_req, s.model_req) = (mode, model);
    }
    take_requests(s, req);
    s.to_child = Some(to);
    s.gen = srv.gen;
    s.used = true;
    s.closing = None;
    s.mode = s.mode_req.clone().unwrap_or_default();
    s.model = s.model_req.clone().unwrap_or_default();
    s.last = Instant::now();
    h.announce(id);
    Ok(())
}

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
        set_rules(&srv, id, cwd, req["mode"].as_str().or(mode.as_deref()))?;
        opened(shared, &srv, id, cwd, req, None)?;
    }
    Ok(srv)
}

pub fn start(shared: &Shared, req: &Value) -> Result<Value, String> {
    let cwd = start_cwd(req)?;
    let (id, srv) = match req["resume"].as_str() {
        Some(id) => (id.to_string(), load(shared, id, cwd, req)?),
        None => {
            let srv = server(shared, req["account"].as_str())?;
            let r = if srv.v2 {
                let (perm, agent) = crate::opencode_v2::rules(req["mode"].as_str());
                let mut body = json!({"location": {"directory": cwd}, "permissions": perm, "agent": agent});
                if let Some(m) = req["model"].as_str().and_then(|m| crate::opencode_v2::model_ref(m, req["effort"].as_str())) {
                    body["model"] = m;
                }
                srv.request("POST", "/api/session", Some(&body))?["data"].take()
            } else {
                srv.request("POST", &format!("/session{}", dir_query(cwd)), Some(&json!({"permission": rules(req["mode"].as_str()).0})))?
            };
            let id = r["id"].as_str().ok_or("opencode gave no session id")?.to_string();
            opened(shared, &srv, &id, cwd, req, None)?;
            own(&id);
            (id, srv)
        }
    };
    commands(srv, shared.lock().unwrap().sessions.get(&id).and_then(|s| s.account.clone()), cwd);
    if let Some(t) = req["text"].as_str() {
        prompt(shared, &id, t, &path_list(req, "images"), &path_list(req, "files"))?;
    }
    Ok(json!({"ev": "started", "id": id, "tool": "opencode"}))
}

// The folder's commands, for the catalog's `/` list (read on a thread; nothing waits for them).
fn commands(srv: Arc<Server>, account: Option<String>, cwd: &str) {
    let cwd = cwd.to_string();
    let path = if srv.v2 { format!("/api/command{}", crate::opencode_v2::loc(&cwd)) } else { format!("/command{}", dir_query(&cwd)) };
    thread::spawn(move || {
        let mut r = srv.request("GET", &path, None);
        // 2 answers a folder it has not loaded yet with none, and has them a moment later
        if srv.v2 && r.as_ref().is_ok_and(|v| v["data"].as_array().is_some_and(|a| a.is_empty())) {
            thread::sleep(Duration::from_secs(2));
            r = srv.request("GET", &path, None);
        }
        match r {
            Ok(mut r) => {
                let list = if srv.v2 { r["data"].take() } else { r };
                crate::accounts::note_commands("opencode", account.as_deref(), &cwd, list)
            }
            Err(e) => eprintln!("[opencode] GET {path}: {e}"),
        }
    });
}

pub fn send(shared: &Shared, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("send needs an id")?;
    let text = req["text"].as_str().ok_or("send needs text")?;
    let cwd = shared.lock().unwrap().sessions.get(id).map(|s| s.cwd.clone()).ok_or(format!("no such session: {id}"))?;
    load(shared, id, &cwd, req)?;
    prompt(shared, id, text, &path_list(req, "images"), &path_list(req, "files"))?;
    Ok(json!({"ev": "ok"}))
}

fn mime(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("pdf") => "application/pdf",
        _ => "text/plain",
    }
}

// A message as prompt_async: text, then images and files as file parts. 2: the session's agent and model are
// switched first (its prompt names neither), in order on the same poster.
fn prompt(shared: &Shared, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    if v2_for(&shared.lock().unwrap(), id) {
        return prompt_v2(shared, id, text, images, files);
    }
    let mut parts = vec![json!({"type": "text", "text": text})];
    for p in images.iter().chain(files) {
        let m = crate::image_type_of(p).unwrap_or_else(|| mime(p));
        parts.push(json!({"type": "file", "mime": m, "filename": p.rsplit('/').next().unwrap_or(p), "url": file_url(p)}));
    }
    let mut h = shared.lock().unwrap();
    let s = h.sessions.get(id).ok_or("no such session")?;
    let mut body = json!({"parts": parts, "agent": rules(s.mode_req.as_deref()).1});
    if let Some((provider, model)) = s.model_req.as_deref().and_then(|m| m.split_once('/')) {
        body["model"] = json!({"providerID": provider, "modelID": model});
    }
    if let Some(e) = &s.effort_req {
        body["variant"] = json!(e);
    }
    let line = json!({"method": "POST", "path": format!("/session/{id}/prompt_async{}", dir_query(&s.cwd)), "body": body, "what": "prompt", "session": id});
    let to = s.to_child.clone().ok_or("session is not open")?;
    sent(&mut h, id, text);
    h.announce(id);
    to.send(line.to_string()).map_err(|_| "opencode has stopped".to_string())
}

fn prompt_v2(shared: &Shared, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    let attached: Vec<Value> = images.iter().chain(files).map(|p| json!({"uri": file_url(p), "name": p.rsplit('/').next().unwrap_or(p)})).collect();
    let mut h = shared.lock().unwrap();
    let s = h.sessions.get(id).ok_or("no such session")?;
    let to = s.to_child.clone().ok_or("session is not open")?;
    let mut lines = vec![json!({"method": "POST", "path": format!("/api/session/{id}/agent"), "body": {"agent": crate::opencode_v2::rules(s.mode_req.as_deref()).1}})];
    if let Some(m) = s.model_req.as_deref().and_then(|m| crate::opencode_v2::model_ref(m, s.effort_req.as_deref())) {
        lines.push(json!({"method": "POST", "path": format!("/api/session/{id}/model"), "body": {"model": m}}));
    }
    let mut body = json!({"text": text});
    if !attached.is_empty() {
        body["files"] = json!(attached);
    }
    lines.push(json!({"method": "POST", "path": format!("/api/session/{id}/prompt"), "body": body, "what": "prompt", "session": id}));
    sent(&mut h, id, text);
    h.announce(id);
    for l in lines {
        to.send(l.to_string()).map_err(|_| "opencode has stopped".to_string())?;
    }
    Ok(())
}

// Whether the session's server speaks 2.
fn v2_for(h: &Host, id: &str) -> bool {
    h.sessions.get(id).is_some_and(|s| v2_gens().lock().unwrap().contains(&s.gen))
}

fn post_for(h: &Host, id: &str, line: Value) -> Result<(), String> {
    let s = h.sessions.get(id).ok_or("no such session")?;
    s.to_child.as_ref().ok_or("session is not open")?.send(line.to_string()).map_err(|_| "opencode has stopped".to_string())
}

// A mode change on an open session, in order with its prompts and answers.
pub fn set_mode(h: &Host, id: &str, mode: &str) -> Result<(), String> {
    if v2_for(h, id) {
        return post_for(h, id, json!({"method": "PATCH", "path": format!("/api/session/{id}"), "body": {"permissions": crate::opencode_v2::rules(Some(mode)).0}}));
    }
    let cwd = h.sessions.get(id).map(|s| s.cwd.clone()).unwrap_or_default();
    post_for(h, id, json!({"method": "PATCH", "path": format!("/session/{id}{}", dir_query(&cwd)), "body": {"permission": rules(Some(mode)).0}}))
}

pub fn answer(h: &mut Host, id: &str, native: Value, allow: bool, always: bool, message: Option<&str>) -> Result<(), String> {
    let reply = match (allow, always) {
        (true, true) => "always",
        (true, false) => "once",
        _ => "reject",
    };
    let mut body = json!({"reply": reply});
    if let (false, Some(m)) = (allow, message) {
        body["message"] = json!(m);
    }
    let cwd = h.sessions.get(id).map(|s| s.cwd.clone()).unwrap_or_default();
    let rid = native["id"].as_str().unwrap_or("");
    if v2_for(h, id) {
        body["decision"] = body["reply"].take();
        body.as_object_mut().map(|b| b.remove("reply"));
        return post_for(h, id, json!({"method": "POST", "path": format!("/api/session/{id}/permission/{rid}/reply"), "body": body}));
    }
    post_for(h, id, json!({"method": "POST", "path": format!("/permission/{rid}/reply{}", dir_query(&cwd)), "body": body}))
}

pub fn interrupt(h: &mut Host, id: &str) -> Result<(), String> {
    if v2_for(h, id) {
        return post_for(h, id, json!({"method": "POST", "path": format!("/api/session/{id}/interrupt")}));
    }
    let cwd = h.sessions.get(id).map(|s| s.cwd.clone()).unwrap_or_default();
    post_for(h, id, json!({"method": "POST", "path": format!("/session/{id}/abort{}", dir_query(&cwd))}))
}

// The host stops relaying the session; a turn still running is stopped first.
pub fn unload(h: &mut Host, id: &str) {
    if h.sessions.get(id).is_some_and(|s| s.state != "idle") {
        let _ = interrupt(h, id);
    }
}

// Servers with no session open on them and no call for `idle` are stopped (by their exact pid).
pub fn reap(h: &mut Host, idle: Duration) {
    let Ok(mut all) = servers().try_lock() else { return };
    let busy: Vec<u64> = h.sessions.values().filter(|s| s.tool == "opencode" && s.to_child.is_some()).map(|s| s.gen).collect();
    all.retain(|_, s| {
        let keep = !s.done.load(Ordering::Relaxed)
            && (busy.contains(&s.gen) || s.calls.load(Ordering::Relaxed) > 0 || s.last.lock().unwrap().elapsed() < idle);
        if !keep && !s.done.load(Ordering::Relaxed) {
            s.tx.lock().unwrap().take();
            let _ = Command::new("kill").arg(s.pid.to_string()).status();
        }
        keep
    });
}

// The host is going away: every `opencode serve` is stopped (by its exact pid). Unlike the other
// agents it has no stdin to see the host leave by, and it runs in a scope of its own, so neither a
// dead host nor the host's service stopping reaches it.
pub fn stop_all() {
    let all = servers().lock().unwrap_or_else(|e| e.into_inner());
    for s in all.values().filter(|s| !s.done.load(Ordering::Relaxed)) {
        let _ = Command::new("kill").arg(s.pid.to_string()).status();
    }
}

// Servers a host that died without stopping them (SIGKILL, the OOM killer) left running: their
// scopes are named for the host's pid, so those whose host is gone are stopped.
pub fn sweep_orphans() {
    let Ok(out) = Command::new("systemctl").args(["--user", "list-units", "--plain", "--no-legend", "--all", "agent-opencode-*.scope"]).output() else { return };
    for unit in String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| l.split_whitespace().next()) {
        let pid = unit.strip_prefix("agent-opencode-").and_then(|r| r.split('-').next()).and_then(|p| p.parse::<u32>().ok());
        if pid.is_some_and(|p| !std::path::Path::new(&format!("/proc/{p}")).exists()) {
            let _ = Command::new("systemctl").args(["--user", "stop", unit]).status();
        }
    }
}

// The server for a session, and the folder its requests name.
fn session_server(shared: &Shared, id: &str) -> Result<(Arc<Server>, String), String> {
    let (account, cwd) = shared.lock().unwrap().sessions.get(id).map(|s| (s.account.clone(), s.cwd.clone())).unwrap_or_default();
    let srv = server(shared, account.as_deref())?;
    let cwd = if !cwd.is_empty() {
        cwd
    } else if srv.v2 {
        srv.request("GET", &format!("/api/session/{id}"), None)?["data"]["location"]["directory"].as_str().unwrap_or("").to_string()
    } else {
        srv.request("GET", &format!("/session/{id}"), None)?["directory"].as_str().unwrap_or("").to_string()
    };
    Ok((srv, cwd))
}

// 2 has no rename in its API: the title goes in the session's metadata with the session's id (a fork copies the
// metadata, and keeps its own title), which the history reads first (sources.rs); opencode's own lists keep the
// title it made.
pub fn rename(shared: &Shared, id: &str, title: &str) -> Result<(), String> {
    let (srv, cwd) = session_server(shared, id)?;
    if srv.v2 {
        let mut meta = srv.request("GET", &format!("/api/session/{id}"), None)?["data"]["metadata"].take();
        if !meta.is_object() {
            meta = json!({});
        }
        meta["renamed"] = json!({"id": id, "title": title});
        return srv.request("PATCH", &format!("/api/session/{id}"), Some(&json!({"metadata": meta}))).map(|_| ());
    }
    srv.request("PATCH", &format!("/session/{id}{}", dir_query(&cwd)), Some(&json!({"title": title}))).map(|_| ())
}

pub fn fork(shared: &Shared, from: &str, cwd: &str, req: &Value) -> Result<String, String> {
    let (mode, model) = shared.lock().unwrap().sessions.get(from).map(|s| (s.mode_req.clone(), s.model_req.clone())).unwrap_or_default();
    let (srv, _) = session_server(shared, from)?;
    let r = if srv.v2 {
        srv.request("POST", &format!("/api/session/{from}/fork"), Some(&json!({})))?["data"].take()
    } else {
        srv.request("POST", &format!("/session/{from}/fork{}", dir_query(cwd)), Some(&json!({})))?
    };
    let id = r["id"].as_str().ok_or("opencode gave no session id")?.to_string();
    // opencode's fork does not copy the session's rules
    set_rules(&srv, &id, cwd, req["mode"].as_str().or(mode.as_deref()))?;
    opened(shared, &srv, &id, cwd, req, Some((mode, model)))?;
    Ok(id)
}

// opencode's own archive: hidden from its lists, kept in its database.
pub fn archive(shared: &Shared, id: &str) -> Result<(), String> {
    let (srv, cwd) = session_server(shared, id)?;
    if srv.v2 {
        return srv.request("PATCH", &format!("/api/session/{id}"), Some(&json!({"archived": true}))).map(|_| ());
    }
    srv.request("PATCH", &format!("/session/{id}{}", dir_query(&cwd)), Some(&json!({"time": {"archived": now_ms()}}))).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Attached files whose names hold '#', '?', '%' or a space reach opencode as file URLs it reads back whole, on
    // both 1's prompt_async and 2's prompt.
    #[test]
    fn attached_paths_are_escaped_in_their_urls() {
        let id = "ses_0123456789abcdefghijklmnop";
        let files = ["/home/me/C# notes.md".to_string(), "/home/me/what?.txt".to_string(), "/home/me/100%.txt".to_string()];
        let want = ["file:///home/me/C%23%20notes.md", "file:///home/me/what%3F.txt", "file:///home/me/100%25.txt"];
        for (gen, v2) in [(u64::MAX - 1, false), (u64::MAX - 2, true)] {
            if v2 {
                v2_gens().lock().unwrap().insert(gen);
            }
            let (tx, rx) = channel::<String>();
            let mut s = Session::new(id, "/home/me", "opencode");
            (s.gen, s.to_child) = (gen, Some(tx));
            let shared: Shared = Arc::new(Mutex::new(Host::default()));
            shared.lock().unwrap().sessions.insert(id.into(), s);
            prompt(&shared, id, "read these", &[], &files).unwrap();
            let sent: Vec<Value> = rx.try_iter().map(|l| serde_json::from_str(&l).unwrap()).collect();
            let urls: Vec<&str> = if v2 {
                sent.last().unwrap()["body"]["files"].as_array().unwrap().iter().map(|f| f["uri"].as_str().unwrap()).collect()
            } else {
                sent[0]["body"]["parts"].as_array().unwrap().iter().filter_map(|p| p["url"].as_str()).collect()
            };
            assert_eq!(urls, want, "opencode {}", if v2 { 2 } else { 1 });
        }
    }
}
