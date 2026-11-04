//! agent-host: keeps the desktop's agent sessions running and relays them to the Claude app, the
//! notch and the palette.
//!
//! A Claude Code session is one `claude -p` process in stream-json mode, started in its own systemd
//! scope with a memory cap. Codex (codex.rs), opencode (opencode.rs) and pi (pi.rs) sessions are
//! relayed in Claude Code's dialect. Clients connect to a unix socket and exchange JSON lines:
//!
//!   → {"op":"hello","proto":5}   (this connection also gets pi and opencode history rows, with the protocol 5 fields)
//!   → {"op":"list"}   {"op":"info","id":ID}
//!   → {"op":"start","cwd":DIR,"text":?,"resume":?ID,"id":?ID,"model":?,"mode":?,"agent":?TOOL,"files":?[PATH],"images":?[PATH]}
//!   → {"op":"fork","id":ID}   {"op":"delete","id":ID}   ← {"ev":"forked","id":NEW,"from":ID,"tool":T}   {"ev":"deleted","id":ID,"how":"trash"|"archive"}
//!   → {"op":"send","id":ID,"text":TEXT,"images":?[PATH]}
//!   → {"op":"answer","id":ID,"request":RID,"allow":BOOL,"input":?,"permissions":?,"message":?}
//!   → {"op":"stop_task","id":ID,"task":TASK}   (one background agent, workflow run or command)
//!   → {"op":"rewind","id":ID,"message":UUID,"lastSeen":?UUID}   {"op":"rewind_files","id":ID,"message":UUID,"dry":?BOOL}
//!   → {"op":"side_question","id":ID,"question":Q,"history":?[{question,response}]}
//!   ← {"ev":"control","id":ID,"response":{..}} as the reply to those once Claude Code answers
//!   → {"op":"agents","id":ID}   {"op":"agent","id":ID,"agent":AGENT}   (history.rs: sub-agents, workflow runs, background commands)
//!   → {"op":"interrupt"|"close","id":ID}   {"op":"mode","id":ID,"mode":M}   {"op":"model","id":ID,"model":M}
//!   → {"op":"watch","id":ID,"from":SEQ}     {"op":"unwatch","id":ID}
//!   → {"op":"history","ifChanged":?BOOL}   {"op":"search","q":TEXT}   {"op":"transcript","id":ID,"limit":?N,"beforeByte":?BYTE,"before":?N,"after":?BYTE}   {"op":"changes","id":ID,"before":?UUID}   {"op":"rename","id":ID,"title":T}   (history.rs)
//!   ← {"ev":"history","unchanged":true}   (ifChanged: the rows are the ones last sent on this connection)
//!   ← {"ev":"items","id":ID,"items":[..]}   (watchers: the running process's messages in transcript item form)
//!   ← {"ev":"hello","version":V,"proto":N,"sessions":[..],"agents":{TOOL:{installed,version,signedIn,reason,caps}}}   {"ev":"sessions","sessions":[..]}   {"ev":"state","session":{..}}   {"ev":"started","id":ID,"tool":T}
//!   ← {"ev":"event","id":ID,"seq":N,"data":<claude's own JSON line, or another agent's in Claude Code's shape>,"native":?<that agent's own message>}   (watchers only)
//!   ← {"ev":"exit","id":ID,"code":N,"reason":"idle"|"closed"|"exited"|"crashed"|"signedOut"|"expired","stderr":[..]}   {"ev":"ok"}   {"ev":"error","message":..}
//! Protocol 6 (hello `features`: accounts, catalog, effort; accounts.rs):
//!   → {"op":"start",..,"account":?ID,"effort":?E}   (omitted fields from the folder's defaults, then everywhere's, then the agent's own)
//!   → {"op":"effort","id":ID,"effort":E}   {"op":"model","id":ID,"model":"default"}   {"op":"settings","id":ID}   (Claude Code's get_settings)
//!   → {"op":"accounts","watch":?BOOL}   {"op":"resolve","cwd":DIR,"agent":?}   {"op":"catalog","agent":A,"account":?,"cwd":?,"refresh":?}
//!   → {"op":"defaults_set",..}   {"op":"account_check"|"account_add"|"account_login"|"account_code"|"account_cancel"|"account_logout"|"account_remove"|"account_set",..}
//!   → {"op":"key_set"|"key_use"|"key_check"|"key_remove",..}   {"op":"install"|"install_cancel","agent":A,"node":?BOOL}
//!   ← {"ev":"accounts",..}   {"ev":"signin",..}   {"ev":"install",..}   {"ev":"catalog",..}   (pushed to connections that watch accounts only)
//! Results (hello feature "results"; results.rs):
//!   → {"op":"results","id":ID}   {"op":"read_reports","id":ID}   {"op":"ask","id":ID,"q":TEXT}
//!   ← {"ev":"results",..}   {"ev":"read_reports","id":ID,"queued":N}   {"ev":"ask",..}
//! A request's "tag" is echoed on its reply.
//!
//! A session whose turn has ended and that has been quiet for AGENT_HOST_IDLE seconds (default
//! 600) has its process closed; the next message resumes it with `--resume`, so an idle session
//! holds no memory. Finished turns live in Claude's own transcript; the host keeps the last few
//! thousand events of a running process so a client that reconnects mid-turn can catch up.

mod accounts;
mod catalog;
mod codex;
mod history;
#[allow(dead_code)] // Install::pid and Detected::path serve its tests
mod install;
mod keys;
mod opencode;
mod opencode_v2;
mod pi;
mod results;
#[allow(dead_code)] // Login::pid and active() serve its tests
mod signin;

use history::History;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::net::Shutdown;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs, process, thread};

const KEEP_EVENTS: usize = 5000;
const STDERR_LINES: usize = 20;

struct Session {
    id: String,
    tool: &'static str, // "claude" | "codex" | "opencode" | "pi"
    cwd: String,
    account: Option<String>,   // None: the agent's own sign-in
    model_req: Option<String>, // passed on every (re)start
    effort_req: Option<String>,
    mode_req: Option<String>,
    fork_from: Option<String>, // Claude: the session this one forks, until its first message is sent
    turn_at: u64,              // when the running turn was sent (ms): what it made is swept at its result
    turn: String,              // codex: the running turn's id
    gate: bool,                // pi: its permission gate has said it is loaded
    spawned: Instant,          // pi: when its process started (the gate has GATE_WAIT from then)
    native: HashMap<String, Value>, // codex items and opencode parts by id, for approvals and deltas
    held: Vec<String>,         // lines held back until the process can take them (pi: until its gate loads)
    model: String, // as the running process reports it
    mode: String,
    effort: String,
    tasks: HashMap<String, String>, // Claude Code: its process's running tasks -> the tool_use that started each
    stops: HashMap<String, (Value, Option<String>)>, // task -> its stop record, and the task's state when it was first shown
    stopping: HashMap<String, String>, // stop_task request id -> task
    auth: Option<&'static str>, // its process said its sign-in is gone ("signedOut" | "expired")
    state: &'static str, // "working" | "waiting" | "idle"
    used: bool,          // has a transcript, so a restart must --resume
    to_child: Option<Sender<String>>, // lines for the process's stdin: its writer thread has it
    pid: u32, // 0 once the process has exited
    gen: u64, // which process the reader threads belong to
    seq: u64,
    events: VecDeque<(u64, String)>, // ready-to-send lines
    pending: Vec<Value>,              // permission requests nobody has answered
    info: Value,                      // the initialize reply: commands, models, account
    started: u64,
    last: Instant,
    last_ms: u64,
    stderr: VecDeque<String>,
    closing: Option<&'static str>, // why the host closed stdin: "idle" | "closed"
    live: Vec<Value>,               // this process's messages as transcript items (history::claude_item)
    prompt: String,                 // the first message sent through the host: a title before the transcript has one
    calls: HashMap<String, usize>,  // tool_use id -> index in `live`, for attaching results
    ctl: HashMap<String, (&'static str, String)>, // our control requests: field asked for, value reported before
}

impl Session {
    fn new(id: &str, cwd: &str, tool: &'static str) -> Self {
        Session {
            id: id.into(),
            tool,
            cwd: cwd.into(),
            account: None,
            model_req: None,
            effort_req: None,
            mode_req: None,
            fork_from: None,
            turn_at: 0,
            turn: String::new(),
            gate: false,
            spawned: Instant::now(),
            native: HashMap::new(),
            held: Vec::new(),
            model: String::new(),
            mode: String::new(),
            effort: String::new(),
            tasks: HashMap::new(),
            stops: HashMap::new(),
            stopping: HashMap::new(),
            auth: None,
            state: "idle",
            used: false,
            to_child: None,
            pid: 0,
            gen: 0,
            seq: 0,
            events: VecDeque::new(),
            pending: Vec::new(),
            info: Value::Null,
            started: now_ms(),
            last: Instant::now(),
            last_ms: now_ms(),
            stderr: VecDeque::new(),
            closing: None,
            live: Vec::new(),
            prompt: String::new(),
            calls: HashMap::new(),
            ctl: HashMap::new(),
        }
    }

    fn summary(&self) -> Value {
        let mut s = json!({"id": self.id, "tool": self.tool, "cwd": self.cwd, "state": self.state, "alive": self.to_child.is_some(),
               "model": self.model, "mode": self.mode, "pending": self.pending, "prompt": self.prompt,
               "started": self.started, "last": self.last_ms, "seq": self.seq, "caps": caps(self.tool),
               "account": accounts::account_id(self.tool, self.account.as_deref()), "effort": self.effort});
        if let Some(f) = &self.fork_from {
            s["from"] = json!(f);
        }
        s
    }
}

// What the host can do with each agent's sessions.
fn caps(tool: &str) -> Value {
    let claude = tool == "claude";
    // Claude also takes bypassPermissions ("Full access"): its sessions start able to switch into it
    let modes: &[&str] = if claude { &["auto", "manual", "acceptEdits", "plan", "bypassPermissions"] } else { &["auto", "manual", "acceptEdits", "plan"] };
    json!({"fork": true, "rename": true, "delete": true, "rewind": claude, "rewindFiles": claude, "aside": claude,
           "tasks": claude, "steer": matches!(tool, "codex" | "pi"), "modes": modes, "effort": true})
}

struct Client {
    out: UnixStream,
    watching: HashSet<String>,
    proto: u64,   // what the client said it speaks (hello op); 4 until it does
    history: u64, // a hash of the history rows last sent to it
    accounts: bool, // sent accounts {watch:true}: gets the accounts, signin, install and catalog pushes
}

#[derive(Default)]
struct Host {
    sessions: HashMap<String, Session>,
    clients: HashMap<u64, Client>,
    next_client: u64,
    next_gen: u64,
    next_req: u64,
    // control requests whose answer goes back to the client that asked (rewind, side_question):
    // request id -> (client, its tag, session)
    replies: HashMap<String, (u64, Value, String)>,
}

type Shared = Arc<Mutex<Host>>;
type Past = Arc<Mutex<History>>;

impl Host {
    fn list(&self) -> Vec<Value> {
        let mut all: Vec<&Session> = self.sessions.values().collect();
        all.sort_by_key(|s| s.started);
        all.into_iter().map(Session::summary).collect()
    }

    fn to_all(&mut self, msg: &Value) {
        let line = format!("{msg}\n");
        self.clients.retain(|_, c| deliver(c, line.as_bytes()));
    }

    // An untagged accounts, signin, install or catalog event: to the connections that watch accounts.
    fn push(&mut self, msg: &Value) {
        let line = format!("{msg}\n");
        self.clients.retain(|_, c| !c.accounts || deliver(c, line.as_bytes()));
    }

    fn to_watchers(&mut self, id: &str, line: &str) {
        self.clients.retain(|_, c| !c.watching.contains(id) || deliver(c, line.as_bytes()));
    }

    fn to_client(&mut self, cid: u64, text: &str) {
        let failed = self.clients.get_mut(&cid).is_some_and(|c| !deliver(c, text.as_bytes()));
        if failed {
            self.clients.remove(&cid);
        }
    }

    fn announce(&mut self, id: &str) {
        if let Some(s) = self.sessions.get(id) {
            let msg = json!({"ev": "state", "session": s.summary()});
            self.to_all(&msg);
        }
    }

    // Handed to the session's writer thread rather than written here: a message with an image
    // in it is base64, far more than a pipe holds, and blocking on it under this lock would stop
    // the reader thread draining the process's output - which is what keeps the pipe moving.
    fn write(&mut self, id: &str, v: &Value) -> Result<(), String> {
        let s = self.sessions.get_mut(id).ok_or("no such session")?;
        let to_child = s.to_child.as_ref().ok_or("session is not running")?;
        to_child.send(format!("{v}\n")).map_err(|_| "claude is not taking input".to_string())
    }

    fn control(&mut self, id: &str, request: Value) -> Result<String, String> {
        self.next_req += 1;
        let rid = format!("host-{}", self.next_req);
        self.write(id, &json!({"type": "control_request", "request_id": rid, "request": request}))?;
        Ok(rid)
    }
}

// A client that cannot take a line within the write timeout is dropped, and its connection is
// shut down with it: serve() would otherwise go on reading requests on a socket whose replies
// to_client throws away, leaving the client waiting for answers that can never come.
fn deliver(c: &mut Client, bytes: &[u8]) -> bool {
    if c.out.write_all(bytes).is_ok() {
        return true;
    }
    let _ = c.out.shutdown(Shutdown::Both);
    false
}

fn home() -> PathBuf {
    PathBuf::from(env::var("HOME").unwrap_or_default())
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn new_id() -> String {
    fs::read_to_string("/proc/sys/kernel/random/uuid").expect("kernel uuid").trim().to_string()
}

// A Claude Code or pi session id (a UUID), or an opencode one (ses_ and 26 letters and digits).
fn valid_id(id: &str) -> bool {
    (id.len() == 36 && id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'))
        || (id.len() == 30 && id.starts_with("ses_") && id[4..].chars().all(|c| c.is_ascii_alphanumeric()))
}

// Every agent process the host starts is built here. `account`: None is the agent's own sign-in.
// Err: a key the account needs cannot be read (the keyring is locked).
fn agent_command(agent: &str, account: Option<&str>) -> Result<Command, String> {
    accounts::command(agent, account)
}

// `cmd` run in a systemd scope of its own with a memory cap, so one runaway agent cannot take the
// machine with it.
fn in_scope(cmd: &Command, unit: &str) -> Command {
    let mut s = Command::new("systemd-run");
    s.args(["--user", "--scope", "--quiet", "--collect", "-p", "MemoryHigh=3G", "-p", "MemoryMax=5G"])
        .arg(format!("--unit={unit}"))
        .arg("--")
        .arg(cmd.get_program())
        .args(cmd.get_args());
    for (k, v) in cmd.get_envs() {
        match v {
            Some(v) => s.env(k, v),
            None => s.env_remove(k),
        };
    }
    if let Some(d) = cmd.get_current_dir() {
        s.current_dir(d);
    }
    s
}

fn spawn(shared: &Shared, h: &mut Host, id: &str) -> Result<(), String> {
    match h.sessions.get(id).map(|s| s.tool) {
        Some("pi") => return pi::spawn(shared, h, id),
        Some("claude") => {}
        Some(t) => return Err(format!("a {t} session has no process of its own")),
        None => return Err("no such session".into()),
    }
    h.next_gen += 1;
    let gen = h.next_gen;
    let s = h.sessions.get_mut(id).ok_or("no such session")?;
    let (mut cmd, key) = accounts::command_with("claude", s.account.as_deref())?;
    cmd.args(["-p", "--verbose", "--input-format", "stream-json", "--output-format", "stream-json",
              "--include-partial-messages", "--replay-user-messages", "--permission-prompt-tool", "stdio",
              // bypassPermissions as a choice, not the default: without it a live switch to Full access is refused
              "--allow-dangerously-skip-permissions",
              // the words of thinking and of narration (the prose between tool calls, which the API can send as a
              // thinking block): without this `-p` writes both empty, so that prose never reaches the screen
              "--thinking-display", "summarized"]);
    match &s.fork_from {
        // a fork is written under its own id at its first message; until then it starts as one again
        Some(from) if !s.used => cmd.args(["--resume", from.as_str(), "--fork-session", "--session-id", id]),
        _ => cmd.arg(if s.used { "--resume" } else { "--session-id" }).arg(id),
    };
    if let Some(m) = &s.model_req {
        cmd.args(["--model", m.as_str()]);
    }
    if let Some(m) = &s.mode_req {
        cmd.args(["--permission-mode", m.as_str()]);
    }
    if let Some(e) = &s.effort_req {
        cmd.args(["--effort", e.as_str()]);
    }
    // Started from inside a Claude Code terminal, these would tell the child it is nested.
    // the file snapshots the interactive CLI keeps by default: a rewind can put files back
    cmd.env("CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING", "1");
    // the task tools (TaskCreate/TaskUpdate) a terminal session has; headless sessions lack them
    // otherwise, and the app's pinned plan is built from them
    cmd.env("CLAUDE_CODE_ENABLE_TODO_TOOLS", "1");
    cmd.env_remove("CLAUDECODE")
        .env_remove("CLAUDE_CODE_ENTRYPOINT")
        .current_dir(&s.cwd);
    let mut cmd = in_scope(&cmd, &format!("agent-{}-{}-{gen}", &id[..8], process::id()));
    if let Some(k) = key {
        keys::pass_fd3(&mut cmd, &k).map_err(|e| format!("pass the key: {e}"))?;
    }
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start claude: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    s.to_child = Some(write_in(child.stdin.take().unwrap(), id));
    s.pid = child.id();
    s.gen = gen;
    s.state = "idle";
    s.last = Instant::now();
    s.stderr.clear();
    s.closing = None;
    s.live.clear();
    s.calls.clear();
    s.ctl.clear();
    s.tasks.clear();
    s.stopping.clear();
    s.auth = None;
    // The app gives each background agent, workflow and command its own Stop (stop_task), so the
    // turn's interrupt is declared to spare them: without this it kills them all.
    let init = json!({"type": "control_request", "request_id": format!("host-init-{gen}"),
                      "request": {"subtype": "initialize", "perTaskStopAffordance": true}});
    h.write(id, &init)?;
    let (a, b) = (shared.clone(), shared.clone());
    let (ida, idb) = (id.to_string(), id.to_string());
    thread::spawn(move || read_out(a, ida, gen, stdout, child));
    thread::spawn(move || read_err(b, idb, gen, stderr));
    Ok(())
}

// The only owner of a process's stdin. Dropping the sender closes it, which is how the host
// ends a process: claude sees EOF and exits.
fn write_in(mut stdin: ChildStdin, id: &str) -> Sender<String> {
    let (tx, rx) = channel::<String>();
    let id = id[..8].to_string();
    thread::spawn(move || {
        for line in rx {
            if let Err(e) = stdin.write_all(line.as_bytes()).and_then(|_| stdin.flush()) {
                eprintln!("[{id}] write to claude: {e}");
                return; // dropping stdin here is the EOF claude exits on
            }
        }
    });
    tx
}

fn read_out<R: std::io::Read>(shared: Shared, id: String, gen: u64, out: R, mut child: Child) {
    for line in BufReader::new(out).lines() {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        on_event(&mut shared.lock().unwrap(), &id, gen, v);
    }
    let code = child.wait().ok().and_then(|s| s.code());
    let mut h = shared.lock().unwrap();
    // The exit code alone misleads: after an interrupted turn, closing stdin exits with 1.
    let (reason, stderr): (&str, Vec<String>) = match h.sessions.get_mut(&id) {
        Some(s) if s.gen == gen => {
            if s.closing.is_none() {
                if let Some(r) = s.auth {
                    accounts::refused(s.tool, s.account.as_deref(), r);
                }
            }
            s.to_child = None;
            s.pid = 0;
            s.state = "idle";
            s.pending.clear();
            s.native.clear();
            // the transcript has it all now, and items run to a few KB each: kept, they would sit
            // in memory and in every roster message for as long as the host runs
            s.events.clear();
            s.live.clear();
            s.calls.clear();
            s.tasks.clear();
            s.stopping.clear();
            match (s.closing.take(), s.auth.take()) {
                (Some(r), _) => (r, Vec::new()),
                (None, Some(r)) => (r, s.stderr.iter().cloned().collect()),
                (None, None) if code == Some(0) => ("exited", Vec::new()),
                (None, None) => ("crashed", s.stderr.iter().cloned().collect()),
            }
        }
        _ => return,
    };
    h.to_all(&json!({"ev": "exit", "id": id, "code": code, "reason": reason, "stderr": stderr}));
    // what the session still owed a client is answered, so nobody waits for it
    let owed: Vec<String> = h.replies.iter().filter(|(_, r)| r.2 == id).map(|(k, _)| k.clone()).collect();
    for rid in owed {
        if let Some((cid, tag, _)) = h.replies.remove(&rid) {
            let line = json!({"ev": "error", "message": "The session stopped before it answered.", "tag": tag});
            h.to_client(cid, &format!("{line}\n"));
        }
    }
    h.announce(&id);
}

fn read_err(shared: Shared, id: String, gen: u64, err: ChildStderr) {
    for line in BufReader::new(err).lines() {
        let Ok(line) = line else { break };
        let line = keys::redact(&line).into_owned();
        eprintln!("[{}] {line}", &id[..8]);
        let mut h = shared.lock().unwrap();
        if let Some(s) = h.sessions.get_mut(&id).filter(|s| s.gen == gen) {
            if let Some(r) = accounts::auth_failure(s.tool, &line) {
                s.auth = Some(r);
            }
            s.stderr.push_back(line);
            if s.stderr.len() > STDERR_LINES {
                s.stderr.pop_front();
            }
        }
    }
}

// A line of a session's own process: Claude Code's are relayed as they are, pi's translated.
fn on_event(h: &mut Host, id: &str, gen: u64, v: Value) {
    if h.sessions.get(id).is_some_and(|s| s.tool == "pi") {
        return pi::event(h, id, gen, v);
    }
    relay(h, id, gen, v, None);
}

// One line of a session's process, in Claude Code's shape; `native`: the other agent's own message
// it was made from, passed on beside it.
fn relay(h: &mut Host, id: &str, gen: u64, mut v: Value, mut native: Option<Value>) {
    // Claude Code's permission input as it asked: an answer with no input of its own allows this,
    // not the masked copy clients see
    let raw_input = (v["type"] == "control_request" && v["request"]["subtype"] == "can_use_tool").then(|| v["request"]["input"].clone());
    keys::redact_value(&mut v);
    if let Some(n) = native.as_mut() {
        keys::redact_value(n);
    }
    let watched = h.clients.values().any(|c| c.watching.contains(id));
    let Some(s) = h.sessions.get_mut(id) else { return };
    if s.gen != gen {
        return;
    }
    let mut changed = false;
    let mut items = false;
    let mut answer_to: Option<(u64, String)> = None;
    let mut auth: Option<&'static str> = None;
    match v["type"].as_str().unwrap_or("") {
        "control_request" if v["request"]["subtype"] == "can_use_tool" => {
            let r = &v["request"];
            s.pending.push(json!({"request": v["request_id"], "tool": r["tool_name"], "input": r["input"],
                                  "suggestions": r["permission_suggestions"]}));
            if let (true, Some(rid), Some(raw)) = (s.tool == "claude", v["request_id"].as_str(), raw_input) {
                s.native.insert(format!("input:{rid}"), raw);
            }
            s.state = "waiting";
            changed = true;
        }
        "control_request" => {
            // Hooks and MCP servers are not registered through the host; answer rather than hang.
            let reply = json!({"type": "control_response", "response": {"subtype": "error",
                               "request_id": v["request_id"], "error": "not handled by agent-host"}});
            if let Some(to_child) = &s.to_child {
                let _ = to_child.send(format!("{reply}\n"));
            }
        }
        "control_response" if v["response"]["request_id"].as_str().is_some_and(|r| r.starts_with("host-init-")) => {
            s.info = v["response"]["response"].clone();
            accounts::note_claude_init(s.account.as_deref(), &s.cwd, s.info.clone());
        }
        // the answer to a control request a client is waiting on goes back to it, as its reply
        "control_response" if h.replies.contains_key(v["response"]["request_id"].as_str().unwrap_or("")) => {
            let r = &v["response"];
            let (cid, tag, _) = h.replies.remove(r["request_id"].as_str().unwrap_or("")).unwrap();
            let ok = r["subtype"] == "success";
            if let Some(task) = s.stopping.remove(r["request_id"].as_str().unwrap_or("")) {
                if !ok {
                    s.stops.insert(task, (json!({"state": "refused", "message": r["error"]}), None));
                }
            }
            answer_to = Some((cid, format!("{}\n", json!({"ev": if ok { "control" } else { "error" }, "id": id, "response": r["response"],
                                                           "message": r["error"], "tag": tag}))));
        }
        "control_response" => {
            let r = &v["response"];
            match s.ctl.remove(r["request_id"].as_str().unwrap_or("")) {
                // Mode and model are reported as changed the moment they are asked for. A refusal
                // used to be dropped here, so the control went on showing a value the process
                // never applied for the rest of its life.
                Some((field, was)) if r["subtype"] != "success" => {
                    eprintln!("[{}] {field} refused: {}", &id[..8], r["error"]);
                    match field {
                        "mode" => s.mode = was,
                        "effort" => s.effort = was,
                        _ => s.model = was,
                    }
                    changed = true;
                }
                _ if r["subtype"] != "success" => eprintln!("[{}] control refused: {}", &id[..8], r["error"]),
                _ => {}
            }
        }
        "system" if v["subtype"] == "init" => {
            s.model = v["model"].as_str().unwrap_or("").into();
            s.mode = v["permissionMode"].as_str().unwrap_or("").into();
            s.effort = v["effort"].as_str().map(str::to_string).or(s.effort_req.clone()).unwrap_or_default();
            changed = true;
        }
        // the background agents, workflows and commands its process runs (history::LiveTasks)
        "system" if matches!(v["subtype"].as_str(), Some("task_started" | "task_progress")) => {
            if let Some(t) = v["task_id"].as_str() {
                let call = s.tasks.entry(t.to_string()).or_default();
                if let Some(c) = v["tool_use_id"].as_str() {
                    *call = c.to_string();
                }
            }
        }
        "system" if v["subtype"] == "task_notification"
            || (v["subtype"] == "task_updated" && matches!(v["patch"]["status"].as_str(), Some("completed" | "failed" | "killed" | "stopped"))) => {
            if let Some(t) = v["task_id"].as_str() {
                s.tasks.remove(t);
                s.stops.remove(t);
            }
        }
        "system" if v["subtype"] == "commands_changed" => {
            accounts::note_commands("claude", s.account.as_deref(), &s.cwd, v["commands"].clone());
        }
        "result" => {
            if s.pending.is_empty() {
                s.state = "idle";
            }
            // a sign-in that no longer works ends the session with why (exit.reason)
            if v["is_error"] == true {
                auth = accounts::auth_failure(s.tool, v["result"].as_str().unwrap_or(""))
                    .or((v["authError"] == true).then_some("signedOut"));
            }
            // what the turn made beside its tool-named writes (a script's output, a download)
            if s.turn_at > 0 {
                let (sid, cwd, since) = (s.id.clone(), s.cwd.clone(), s.turn_at);
                thread::spawn(move || {
                    let made = history::sweep_made(&cwd, since);
                    if !made.is_empty() {
                        let _ = history::made_store(&sid, &made);
                    }
                });
                s.turn_at = 0;
            }
            changed = true;
        }
        // Claude asking to drop a permission request it no longer waits for (an interrupt): the card goes, and
        // a state left at "waiting" kept the turn open and every queued message unsent
        "control_cancel_request" => {
            let rid = v["request_id"].clone();
            s.pending.retain(|p| p["request"] != rid);
            if s.pending.is_empty() && s.state == "waiting" {
                s.state = "working";
            }
            changed = true;
        }
        "assistant" | "user" if v["parent_tool_use_id"].is_string() => {}
        "assistant" | "user" => {
            history::claude_item(&v, &mut s.live, &mut s.calls, true);
            items = true;
        }
        _ => {}
    }
    // a turn Claude starts itself (a background task's report, a Stop hook sending it back) is working too:
    // left idle, the app wiped the reply it was streaming at every read of the transcript
    // (a reply's start, not any stray event after a turn's result)
    let starts = v["type"] == "assistant" || (v["type"] == "stream_event" && v["event"]["type"] == "message_start");
    if s.state == "idle" && starts && !v["parent_tool_use_id"].is_string() {
        s.state = "working";
        changed = true;
    }
    s.seq += 1;
    s.last = Instant::now();
    s.last_ms = now_ms();
    let mut ev = json!({"ev": "event", "id": id, "seq": s.seq, "data": v});
    if let Some(n) = native {
        ev["native"] = n;
    }
    let line = format!("{ev}\n");
    s.events.push_back((s.seq, line.clone()));
    if s.events.len() > KEEP_EVENTS {
        s.events.pop_front();
    }
    // The whole list, serialised again for every message of a turn: not built at all when there
    // is nobody watching this session to send it to.
    let items_line = (items && watched).then(|| format!("{}\n", json!({"ev": "items", "id": id, "items": s.live})));
    h.to_watchers(id, &line);
    if let Some(l) = items_line {
        h.to_watchers(id, &l);
    }
    if let Some((cid, l)) = answer_to {
        h.to_client(cid, &l);
    }
    if changed {
        h.announce(id);
    }
    if let Some(r) = auth {
        let (tool, account) = (h.sessions[id].tool, h.sessions[id].account.clone());
        accounts::refused(tool, account.as_deref(), r);
        close(h, id, r);
        h.announce(id);
    }
}

// An image's media type from its first bytes, as the models check it; None: not one they take.
fn image_type(bytes: &[u8]) -> Option<&'static str> {
    match bytes {
        [0x89, b'P', b'N', b'G', ..] => Some("image/png"),
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
        [b'G', b'I', b'F', b'8', ..] => Some("image/gif"),
        [b'R', b'I', b'F', b'F', _, _, _, _, b'W', b'E', b'B', b'P', ..] => Some("image/webp"),
        _ => None,
    }
}

// The media type of the image at `path`, from its first bytes.
fn image_type_of(path: &str) -> Option<&'static str> {
    let mut head = [0u8; 12];
    let n = fs::File::open(path).and_then(|mut f| std::io::Read::read(&mut f, &mut head)).ok()?;
    image_type(&head[..n])
}

// A user message: the text, with any attached images (paths) as base64 blocks before it.
fn user_content(text: &str, images: &[String]) -> Value {
    if images.is_empty() {
        return Value::String(text.to_string());
    }
    let mut blocks: Vec<Value> = images.iter().filter_map(|p| {
        let bytes = fs::read(p).ok()?;
        let mime = image_type(&bytes)?;
        use base64::Engine;
        Some(json!({"type": "image", "source": {"type": "base64", "media_type": mime,
                    "data": base64::engine::general_purpose::STANDARD.encode(bytes)}}))
    }).collect();
    blocks.push(json!({"type": "text", "text": text}));
    Value::Array(blocks)
}

// Ends a session's process (Claude Code and pi: EOF on its stdin, and read_out reports the exit),
// or lets go of its thread on a shared server (Codex and opencode: reported here, as nothing exits).
fn close(h: &mut Host, id: &str, why: &'static str) {
    let Some((tool, alive)) = h.sessions.get(id).map(|s| (s.tool, s.to_child.is_some())) else { return };
    if !alive {
        return;
    }
    match tool {
        "codex" => {
            codex::unload(h, id);
            released(h, id, why, Vec::new());
        }
        "opencode" => {
            opencode::unload(h, id);
            released(h, id, why, Vec::new());
        }
        _ => {
            let s = h.sessions.get_mut(id).unwrap();
            s.to_child = None;
            s.closing = Some(why);
        }
    }
}

// A session on a shared server no longer has it: as read_out reports a process that exited.
fn released(h: &mut Host, id: &str, reason: &str, stderr: Vec<String>) {
    let Some(s) = h.sessions.get_mut(id) else { return };
    s.to_child = None;
    s.state = "idle";
    s.pending.clear();
    s.events.clear();
    s.live.clear();
    s.calls.clear();
    s.native.clear();
    s.turn.clear();
    h.to_all(&json!({"ev": "exit", "id": id, "code": null, "reason": reason, "stderr": stderr}));
    let owed: Vec<String> = h.replies.iter().filter(|(_, r)| r.2 == id).map(|(k, _)| k.clone()).collect();
    for rid in owed {
        if let Some((cid, tag, _)) = h.replies.remove(&rid) {
            let line = json!({"ev": "error", "message": "The session stopped before it answered.", "tag": tag});
            h.to_client(cid, &format!("{line}\n"));
        }
    }
    h.announce(id);
}

// A session closed while idle is started again (resumed) for anything that needs its process.
fn wake(shared: &Shared, h: &mut Host, id: &str) -> Result<(), String> {
    let s = h.sessions.get(id).ok_or("no such session")?;
    if s.to_child.is_none() {
        if s.pid != 0 {
            return Err("session is still closing; try again".into());
        }
        spawn(shared, h, id)?;
    }
    Ok(())
}

// Files given with a message, as Claude Code's @-mentions after its text.
fn with_mentions(text: &str, files: &[String]) -> String {
    if files.is_empty() {
        return text.to_string();
    }
    let at: Vec<String> = files.iter().map(|f| if f.chars().any(char::is_whitespace) { format!("@\"{f}\"") } else { format!("@{f}") }).collect();
    if text.trim().is_empty() { at.join(" ") } else { format!("{text}\n\n{}", at.join(" ")) }
}

fn send_message(shared: &Shared, h: &mut Host, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    match h.sessions.get(id).ok_or("no such session")?.tool {
        "pi" => pi::send(shared, h, id, text, images, files)?,
        "claude" => {
            wake(shared, h, id)?;
            h.write(id, &json!({"type": "user", "message": {"role": "user", "content": user_content(&with_mentions(text, files), images)},
                                "parent_tool_use_id": null, "session_id": id}))?;
        }
        t => return Err(format!("{t} messages are sent on a worker")),
    }
    sent(h, id, text);
    Ok(())
}

// A message is on its way to the session's agent.
fn sent(h: &mut Host, id: &str, text: &str) {
    let Some(s) = h.sessions.get_mut(id) else { return };
    if s.prompt.is_empty() {
        s.prompt = text.lines().next().unwrap_or("").chars().take(120).collect();
    }
    s.state = "working";
    s.used = true;
    s.fork_from = None;
    s.last = Instant::now();
    if s.turn_at == 0 {
        s.turn_at = now_ms();
    }
}

fn tool_name(t: &str) -> Option<&'static str> {
    ["claude", "codex", "opencode", "pi"].into_iter().find(|x| *x == t)
}

// The agent a request is for: start names it, or resumes a session of it (claude when neither
// says); other ops act on a session the host runs. None: not a session the host runs.
fn tool_for(shared: &Shared, req: &Value) -> Result<Option<&'static str>, String> {
    if req["op"] == "start" {
        if let Some(a) = req["agent"].as_str() {
            return tool_name(a).map(Some).ok_or(format!("unknown agent: {a}"));
        }
        let Some(r) = req["resume"].as_str() else { return Ok(Some("claude")) };
        if let Some(t) = shared.lock().unwrap().sessions.get(r).map(|s| s.tool) {
            return Ok(Some(t));
        }
        return Ok(Some(history::tool_of(r).unwrap_or("claude")));
    }
    let id = req["id"].as_str().unwrap_or("");
    Ok(shared.lock().unwrap().sessions.get(id).map(|s| s.tool))
}

// A session the user started here: headless like an agent's, but shown in the app.
fn own(id: &str) {
    let own = history::own_file();
    let _ = fs::create_dir_all(own.parent().unwrap());
    let _ = fs::OpenOptions::new().create(true).append(true).open(&own).and_then(|mut f| writeln!(f, "{id}"));
}

// The cwd a start names, if it is a folder here.
fn start_cwd(req: &Value) -> Result<&str, String> {
    let cwd = req["cwd"].as_str().ok_or("start needs cwd")?;
    if !Path::new(cwd).is_dir() {
        return Err(format!("no such folder: {cwd}"));
    }
    Ok(cwd)
}

// The model, effort and mode a start or a fork asks for.
fn take_requests(s: &mut Session, req: &Value) {
    if let Some(m) = req["model"].as_str() {
        s.model_req = Some(m.into());
    }
    if let Some(m) = req["effort"].as_str() {
        s.effort_req = Some(m.into());
    }
    if let Some(m) = req["mode"].as_str() {
        s.mode_req = Some(m.into());
    }
    if let Some(a) = req["account"].as_str() {
        s.account = Some(a.into());
    }
}

// Claude Code and pi: one process per session.
fn start(shared: &Shared, h: &mut Host, req: &Value, tool: &'static str) -> Result<Value, String> {
    let cwd = start_cwd(req)?;
    let resume = req["resume"].as_str();
    let id = resume.or(req["id"].as_str()).map(str::to_string).unwrap_or_else(new_id);
    if !valid_id(&id) {
        return Err(format!("not a session id: {id}"));
    }
    let s = h.sessions.entry(id.clone()).or_insert_with(|| Session::new(&id, cwd, tool));
    if s.tool != tool {
        return Err(format!("{id} is a {} session", s.tool));
    }
    if resume.is_some() {
        s.used = true;
    }
    take_requests(s, req);
    if resume.is_none() && req["id"].as_str().is_none() {
        own(&id);
    }
    if s.to_child.is_none() && s.pid == 0 {
        spawn(shared, h, &id)?;
    }
    if let Some(t) = req["text"].as_str() {
        send_message(shared, h, &id, t, &path_list(req, "images"), &path_list(req, "files"))?;
    }
    h.announce(&id);
    Ok(json!({"ev": "started", "id": id, "tool": tool}))
}

fn path_list(req: &Value, key: &str) -> Vec<String> {
    req[key].as_array().into_iter().flatten().filter_map(|v| v.as_str().map(str::to_string)).collect()
}

fn answer(h: &mut Host, id: &str, req: &Value) -> Result<(), String> {
    let rid = req["request"].as_str().ok_or("answer needs request")?;
    let s = h.sessions.get_mut(id).unwrap();
    let at = s.pending.iter().position(|p| p["request"] == rid).ok_or("no such request")?;
    let p = s.pending.remove(at);
    let raw = s.native.remove(&format!("input:{rid}"));
    if s.tool != "claude" {
        if s.pending.is_empty() {
            s.state = "working";
        }
        // "always": the client passes back the suggestion the host offered (see pending_from)
        let allow = req["allow"].as_bool() == Some(true);
        let always = allow && req["permissions"].as_array().is_some_and(|a| !a.is_empty());
        let native = s.native.remove(&format!("req:{rid}")).ok_or("that request was already answered")?;
        return match s.tool {
            "codex" => codex::answer(h, id, native, allow, always),
            "opencode" => opencode::answer(h, id, native, allow, always, req["message"].as_str()),
            _ => pi::answer(h, id, native, allow, always),
        };
    }
    let mut response = if req["allow"].as_bool() == Some(true) {
        json!({"behavior": "allow", "updatedInput": req.get("input").or(raw.as_ref()).unwrap_or(&p["input"])})
    } else {
        json!({"behavior": "deny", "message": req["message"].as_str().unwrap_or("Denied in the Claude app.")})
    };
    // "Always allow": the client passes back the suggestions it picked and Claude saves them.
    if let Some(perms) = req.get("permissions") {
        response["updatedPermissions"] = perms.clone();
    }
    if s.pending.is_empty() {
        s.state = "working";
    }
    h.write(id, &json!({"type": "control_response",
                        "response": {"subtype": "success", "request_id": rid, "response": response}}))
}

// A permission request from another agent, raised as Claude Code's can_use_tool with Claude Code's
// tool names and input keys. `native`: what its answer goes back to. The one suggestion offered is
// what the client passes back for "always".
fn ask(h: &mut Host, id: &str, gen: u64, tool: &str, input: Value, native: Value, raw: Value) {
    h.next_req += 1;
    let rid = format!("host-{}", h.next_req);
    if let Some(s) = h.sessions.get_mut(id) {
        s.native.insert(format!("req:{rid}"), native);
    }
    let v = json!({"type": "control_request", "request_id": rid, "request": {"subtype": "can_use_tool", "tool_name": tool, "input": input,
                   "permission_suggestions": [{"type": "addRules", "rules": [{"toolName": tool}], "behavior": "allow", "destination": "session"}]}});
    relay(h, id, gen, v, Some(raw));
}

fn handle(shared: &Shared, h: &mut Host, cid: u64, req: &Value, tool: Option<&'static str>) -> Result<Value, String> {
    let op = req["op"].as_str().unwrap_or("");
    let ok = json!({"ev": "ok"});
    match op {
        "list" => return Ok(json!({"ev": "sessions", "sessions": h.list()})),
        "hello" => {
            let proto = req["proto"].as_u64().unwrap_or(4);
            if let Some(c) = h.clients.get_mut(&cid) {
                c.proto = proto;
            }
            return Ok(json!({"ev": "ok", "proto": PROTO}));
        }
        "start" => return start(shared, h, req, tool.unwrap_or("claude")),
        _ => {}
    }
    let id = req["id"].as_str().ok_or("this op needs an id")?.to_string();
    let Some(tool) = h.sessions.get(&id).map(|s| s.tool) else {
        return Err(format!("no such session: {id}"));
    };
    let claude_only = ["rewind", "rewind_files", "side_question", "stop_task", "settings"];
    if tool != "claude" && claude_only.contains(&op) {
        return Err(format!("{tool} sessions have no {op}"));
    }
    match op {
        "info" => Ok(json!({"ev": "info", "id": id, "info": h.sessions[&id].info})),
        "send" => {
            send_message(shared, h, &id, req["text"].as_str().ok_or("send needs text")?, &path_list(req, "images"), &path_list(req, "files"))?;
            h.announce(&id);
            Ok(ok)
        }
        "answer" => {
            answer(h, &id, req)?;
            h.announce(&id);
            Ok(ok)
        }
        "interrupt" => {
            match tool {
                "codex" => codex::interrupt(h, &id)?,
                "opencode" => opencode::interrupt(h, &id)?,
                "pi" => pi::interrupt(h, &id)?,
                _ => {
                    h.control(&id, json!({"subtype": "interrupt"}))?;
                }
            }
            Ok(ok)
        }
        // Answered when Claude Code answers (on_event): rewind the conversation to just before one
        // of your messages; see or undo the file changes made since one; ask aside without
        // touching the conversation.
        "rewind" | "rewind_files" | "side_question" | "settings" => {
            let request = match op {
                "settings" => json!({"subtype": "get_settings"}),
                // lastSeen: the newest of your messages the client shows; Claude Code refuses a rewind to
                // an earlier one ("stale target") unless the client says it has seen the latest
                "rewind" => json!({"subtype": "rewind_conversation", "target_message_uuid": req["message"].as_str().ok_or("rewind needs a message")?,
                                   "last_seen_user_message_uuid": req["lastSeen"]}),
                "rewind_files" => json!({"subtype": "rewind_files", "user_message_id": req["message"].as_str().ok_or("rewind_files needs a message")?,
                                         "dry_run": req["dry"].as_bool().unwrap_or(false)}),
                _ => json!({"subtype": "side_question", "question": req["question"].as_str().ok_or("side_question needs a question")?,
                            "history": req.get("history").cloned().filter(Value::is_array).unwrap_or(json!([]))}),
            };
            wake(shared, h, &id)?;
            let rid = h.control(&id, request)?;
            h.replies.insert(rid, (cid, req.get("tag").cloned().unwrap_or(Value::Null), id.clone()));
            Ok(json!({"ev": "deferred"}))
        }
        // answered when Claude Code answers; until the task's state changes, `agents` shows the
        // stop as asked for, or refused with Claude Code's reason
        "stop_task" => {
            let task = req["task"].as_str().ok_or("stop_task needs a task")?.to_string();
            let rid = h.control(&id, json!({"subtype": "stop_task", "task_id": task}))?;
            h.replies.insert(rid.clone(), (cid, req.get("tag").cloned().unwrap_or(Value::Null), id.clone()));
            let s = h.sessions.get_mut(&id).unwrap();
            s.stopping.insert(rid, task.clone());
            s.stops.insert(task, (json!({"state": "stopping"}), None));
            Ok(json!({"ev": "deferred"}))
        }
        // Claude Code: its flag settings now, never a settings file; pi: its thinking level now;
        // Codex and opencode: their next turn
        "effort" => {
            let value = req["effort"].as_str().filter(|e| *e != "default").map(str::to_string);
            let s = h.sessions.get_mut(&id).unwrap();
            let alive = s.to_child.is_some();
            let was = std::mem::replace(&mut s.effort, value.clone().unwrap_or_default());
            s.effort_req = value.clone();
            if tool == "claude" && alive {
                let rid = h.control(&id, json!({"subtype": "apply_flag_settings", "settings": {"effortLevel": value}}))?;
                h.sessions.get_mut(&id).unwrap().ctl.insert(rid, ("effort", was));
            } else if let (true, "pi", Some(v)) = (alive, tool, &value) {
                pi::set(h, &id, "effort", v)?;
            }
            h.announce(&id);
            Ok(ok)
        }
        "mode" | "model" => {
            let value = req[op].as_str().ok_or(format!("{op} needs {op}"))?.to_string();
            // "default": the agent's own model, as if none had been asked for
            let default = op == "model" && value == "default";
            let s = h.sessions.get_mut(&id).unwrap();
            let alive = s.to_child.is_some();
            let was = if op == "mode" { s.mode.clone() } else { s.model.clone() };
            if op == "mode" {
                s.mode_req = Some(value.clone());
                s.mode = value.clone();
            } else {
                s.model_req = (!default).then(|| value.clone());
                s.model = value.clone();
            }
            // Codex takes both with its next turn, opencode a model; an opencode mode changes its rules now
            if tool == "pi" && alive && !default {
                pi::set(h, &id, op, &value)?;
            } else if tool == "opencode" && alive && op == "mode" {
                opencode::set_mode(h, &id, &value)?;
            } else if tool == "claude" && alive {
                let request = if op == "mode" { json!({"subtype": "set_permission_mode", "mode": value}) }
                              else { json!({"subtype": "set_model", "model": if default { Value::Null } else { json!(value) }}) };
                let rid = h.control(&id, request)?;
                let field = if op == "mode" { "mode" } else { "model" };
                h.sessions.get_mut(&id).unwrap().ctl.insert(rid, (field, was));
            }
            h.announce(&id);
            Ok(ok)
        }
        "close" => {
            close(h, &id, "closed");
            h.announce(&id); // "alive" is false from here: a client that misses it sends into a closed stdin
            Ok(ok)
        }
        "watch" => {
            let from = req["from"].as_u64().unwrap_or(0);
            let s = &h.sessions[&id];
            let backlog: String = s.events.iter().filter(|(n, _)| *n >= from).map(|(_, l)| l.as_str()).collect();
            let (seq, live) = (s.seq, s.live.clone());
            h.to_client(cid, &backlog);
            if let Some(c) = h.clients.get_mut(&cid) {
                c.watching.insert(id.clone());
            }
            Ok(json!({"ev": "watching", "id": id, "seq": seq, "items": live}))
        }
        "unwatch" => {
            if let Some(c) = h.clients.get_mut(&cid) {
                c.watching.remove(&id);
            }
            Ok(ok)
        }
        _ => Err(format!("unknown op: {op}")),
    }
}

fn serve(shared: Shared, past: Past, stream: UnixStream) {
    let Ok(out) = stream.try_clone() else { return };
    let _ = out.set_write_timeout(Some(Duration::from_secs(2)));
    // the agents are probed once at start (a second or so): a client connecting before that waits
    // for it here, not under the lock
    let agents = agents_now();
    let cid = {
        let mut h = shared.lock().unwrap();
        h.next_client += 1;
        let cid = h.next_client;
        h.clients.insert(cid, Client { out, watching: HashSet::new(), proto: 4, history: 0, accounts: false });
        let hello = format!("{}\n", json!({"ev": "hello", "version": env!("CARGO_PKG_VERSION"),
                                            "proto": PROTO, "sessions": h.list(), "agents": agents,
                                            "features": ["accounts", "catalog", "effort", "results"]}));
        h.to_client(cid, &hello);
        cid
    };
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let req: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
        let op = req["op"].as_str().unwrap_or("").to_string();
        // History reads files, sometimes hundreds of MB: never under the live sessions' lock, and
        // on a thread of its own, or a send, an answer or a follow on this connection waits behind
        // a whole transcript being parsed. The `past` lock still lets only one of them run at once.
        let past_ops = ["history", "search", "transcript", "changes", "agents", "agent", "results", "read_reports", "ask"];
        if past_ops.contains(&op.as_str()) {
            let full = shared.lock().unwrap().clients.get(&cid).is_some_and(|c| c.proto >= 5);
            let (pa, sh) = (past.clone(), shared.clone());
            thread::spawn(move || {
                if op == "history" {
                    return history_reply(&sh, &pa, cid, &req, full);
                }
                let mut reply = past_op(&sh, &pa, &op, &req, full).unwrap_or_else(|e| json!({"ev": "error", "message": e}));
                if let Some(tag) = req.get("tag") {
                    reply["tag"] = tag.clone();
                }
                let line = format!("{reply}\n");
                sh.lock().unwrap().to_client(cid, &line);
            });
            continue;
        }
        // accounts, keys, sign-ins, installs and catalogs (accounts.rs): they run commands and wait
        // on them, so each on a thread of its own
        if accounts::OPS.contains(&op.as_str()) {
            let sh = shared.clone();
            thread::spawn(move || {
                let mut reply = accounts::op(cid, &req).unwrap_or_else(|e| json!({"ev": "error", "message": keys::redact(&e)}));
                if let Some(tag) = req.get("tag") {
                    reply["tag"] = tag.clone();
                }
                let line = format!("{reply}\n");
                sh.lock().unwrap().to_client(cid, &line);
            });
            continue;
        }
        // a start's or a fork's omitted account, model, effort and mode (accounts::fill)
        let filled = tool_for(&shared, &req).and_then(|tool| {
            if !matches!(op.as_str(), "start" | "fork") {
                return Ok((tool, req.clone()));
            }
            let agent = tool.or_else(|| req["id"].as_str().and_then(history::tool_of)).unwrap_or("claude");
            accounts::fill(&req, agent).map(|r| (tool, r))
        });
        let (tool, req) = match filled {
            Ok(t) => t,
            Err(e) => {
                let mut reply = json!({"ev": "error", "message": e});
                if let Some(tag) = req.get("tag") {
                    reply["tag"] = tag.clone();
                }
                shared.lock().unwrap().to_client(cid, &format!("{reply}\n"));
                continue;
            }
        };
        // Work that waits on an agent (a server starting, a thread loading, a session's process
        // exiting before its files go to the Trash) runs on a thread of its own, holding no lock
        // while it waits.
        let worker = matches!(op.as_str(), "rename" | "fork" | "delete")
            || (matches!(tool, Some("codex" | "opencode")) && matches!(op.as_str(), "start" | "send"));
        if worker {
            let (pa, sh) = (past.clone(), shared.clone());
            thread::spawn(move || {
                let mut reply = work_op(&sh, &pa, &op, &req, tool).unwrap_or_else(|e| json!({"ev": "error", "message": e}));
                if let Some(tag) = req.get("tag") {
                    reply["tag"] = tag.clone();
                }
                let line = format!("{reply}\n");
                let mut h = sh.lock().unwrap();
                h.to_client(cid, &line);
                note_account(&h, &reply);
            });
            continue;
        }
        let mut h = shared.lock().unwrap();
        let mut reply = if req.is_object() {
            handle(&shared, &mut h, cid, &req, tool).unwrap_or_else(|e| json!({"ev": "error", "message": e}))
        } else {
            json!({"ev": "error", "message": "not a JSON object"})
        };
        // a request answered later (rewind, side_question) sends its reply then
        if reply["ev"] == "deferred" {
            continue;
        }
        if let Some(tag) = req.get("tag") {
            reply["tag"] = tag.clone();
        }
        h.to_client(cid, &format!("{reply}\n"));
        note_account(&h, &reply);
    }
    shared.lock().unwrap().clients.remove(&cid);
}

// A session just started or forked under an added account is noted, so a resume keeps it.
fn note_account(h: &Host, reply: &Value) {
    if !matches!(reply["ev"].as_str(), Some("started" | "forked")) {
        return;
    }
    let id = reply["id"].as_str().unwrap_or("");
    if let Some(a) = h.sessions.get(id).and_then(|s| s.account.clone()) {
        accounts::note_session(id, &a);
    }
}

fn work_op(shared: &Shared, past: &Past, op: &str, req: &Value, tool: Option<&'static str>) -> Result<Value, String> {
    match (op, tool) {
        ("start", Some("codex")) => codex::start(shared, req),
        ("start", Some("opencode")) => opencode::start(shared, req),
        ("send", Some("codex")) => codex::send(shared, req),
        ("send", Some("opencode")) => opencode::send(shared, req),
        ("rename", _) => rename(shared, past, req),
        ("fork", _) => fork(shared, past, req),
        ("delete", _) => delete(shared, past, req),
        _ => Err(format!("unknown op: {op}")),
    }
}

// The agent's own record of a user-given name, so its own resume list shows it too: through the
// agent when the host has the session open, in its files when not (opencode: always its server).
fn rename(shared: &Shared, past: &Past, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("rename needs an id")?;
    let title = req["title"].as_str().unwrap_or("").trim().to_string();
    if title.is_empty() {
        return Err("rename needs a title".into());
    }
    let live = shared.lock().unwrap().sessions.get(id).map(|s| (s.tool, s.to_child.is_some()));
    let located = locate(past, id);
    let tool = live.map(|l| l.0).or(located.as_ref().ok().map(|l| l.1)).ok_or(format!("no transcript for {id}"))?;
    let alive = live.is_some_and(|l| l.1);
    match tool {
        "opencode" => opencode::rename(shared, id, &title)?,
        "codex" if alive => codex::rename(shared, id, &title)?,
        "pi" if alive => shared.lock().unwrap().write(id, &json!({"type": "set_session_name", "name": title}))?,
        _ => history::rename_offline(tool, id, &located?.0, &title)?,
    }
    past.lock().unwrap().refresh();
    Ok(json!({"ev": "renamed", "id": id, "title": title}))
}

// Where a past session ran, if that folder is on this machine.
fn past_cwd(past: &Past, path: &Path) -> Result<String, String> {
    let cwd = past.lock().unwrap().summaries_for(true).get(path).and_then(|(_, s)| s["cwd"].as_str().map(str::to_string)).unwrap_or_default();
    if cwd.is_empty() || !Path::new(&cwd).is_dir() {
        return Err("That session's folder is not on this machine.".into());
    }
    Ok(cwd)
}

// A new session that starts as a copy of this one, run by the host.
fn fork(shared: &Shared, past: &Past, req: &Value) -> Result<Value, String> {
    let from = req["id"].as_str().ok_or("fork needs an id")?;
    let live = shared.lock().unwrap().sessions.get(from)
        .map(|s| (s.tool, s.cwd.clone(), s.mode_req.clone(), s.model_req.clone(), s.account.clone()));
    let (tool, cwd, mode, model, account) = match live {
        Some(l) => l,
        None => {
            let (path, tool) = locate(past, from)?;
            (tool, past_cwd(past, &path)?, None, None, None)
        }
    };
    let id = match tool {
        "codex" => codex::fork(shared, from, &cwd, req)?,
        "opencode" => opencode::fork(shared, from, &cwd, req)?,
        "pi" => pi::fork(shared, &locate(past, from)?.0, &cwd, req, (mode, model, account))?,
        _ => {
            // Claude Code writes the fork under the id it is given, at its first message
            let id = new_id();
            let mut s = Session::new(&id, &cwd, "claude");
            s.fork_from = Some(from.to_string());
            (s.mode_req, s.model_req, s.account) = (mode, model, account);
            take_requests(&mut s, req);
            let mut h = shared.lock().unwrap();
            h.sessions.insert(id.clone(), s);
            if let Err(e) = spawn(shared, &mut h, &id) {
                h.sessions.remove(&id);
                return Err(e);
            }
            h.announce(&id);
            id
        }
    };
    own(&id);
    Ok(json!({"ev": "forked", "id": id, "from": from, "tool": tool}))
}

// A session gone from the app: Claude Code's and pi's files to the Trash; Codex and opencode keep
// theirs, archived (their own way of hiding a session, which can be undone in them).
fn delete(shared: &Shared, past: &Past, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("delete needs an id")?;
    // a live session is closed first (a running Codex or opencode turn is stopped), and its process
    // waited for, so nothing writes its files after they have gone
    let live = {
        let mut h = shared.lock().unwrap();
        let t = h.sessions.get(id).map(|s| (s.tool, s.state));
        if let Some((tool, state)) = t {
            if tool == "claude" && state != "idle" {
                let _ = h.control(id, json!({"subtype": "interrupt"}));
            }
            close(&mut h, id, "closed");
        }
        t.map(|t| t.0)
    };
    if matches!(live, Some("claude" | "pi")) {
        let until = Instant::now() + Duration::from_secs(20);
        while shared.lock().unwrap().sessions.get(id).is_some_and(|s| s.pid != 0) {
            if Instant::now() > until {
                return Err("The session is still closing; try again.".into());
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    if history::terminal_holder(id).is_some() {
        return Err("Open in a terminal".into());
    }
    let located = locate(past, id);
    let tool = live.or(located.as_ref().ok().map(|l| l.1)).ok_or(format!("no transcript for {id}"))?;
    let how = match tool {
        "codex" => {
            codex::archive(shared, id)?;
            "archive"
        }
        "opencode" => {
            opencode::archive(shared, id)?;
            "archive"
        }
        _ => {
            match located {
                Ok((path, _)) => history::trash_session(tool, id, &path)?,
                // a fork nothing was sent to has no file yet
                Err(e) if live.is_none() => return Err(e),
                Err(_) => {}
            }
            "trash"
        }
    };
    {
        let mut h = shared.lock().unwrap();
        if h.sessions.remove(id).is_some() {
            let list = h.list();
            h.to_all(&json!({"ev": "sessions", "sessions": list}));
        }
    }
    past.lock().unwrap().refresh();
    Ok(json!({"ev": "deleted", "id": id, "how": how}))
}

// ── the agents, as the hello reports them ────────────────────────────────────────────────────

// The agent as found on PATH, and whether its default account can run (accounts::signed_in).
fn probe_agent(agent: &'static str) -> Value {
    let found = install::detect(agent);
    if !found.installed {
        return json!({"installed": false, "version": "", "signedIn": false, "reason": "Not installed", "caps": caps(agent)});
    }
    let (signed, reason) = signed_in(agent);
    json!({"installed": true, "version": found.version, "signedIn": signed, "reason": reason, "caps": caps(agent)})
}

// Whether the agent's default account can run: true, false, or null when the host cannot tell;
// with the reason when it cannot.
fn signed_in(agent: &str) -> (Value, String) {
    accounts::signed_in(agent)
}

// value, when it was probed (None: not yet), whether a probe is running
type AgentsCell = (Mutex<(Value, Option<Instant>, bool)>, Condvar);

fn agents_cell() -> &'static AgentsCell {
    static CELL: OnceLock<AgentsCell> = OnceLock::new();
    CELL.get_or_init(|| (Mutex::new((json!({}), None, false)), Condvar::new()))
}

fn probe_agents() {
    let (m, cv) = agents_cell();
    {
        let mut g = m.lock().unwrap();
        if g.2 {
            return;
        }
        g.2 = true;
    }
    let all: Vec<_> = ["claude", "codex", "opencode", "pi"].into_iter().map(|a| (a, thread::spawn(move || probe_agent(a)))).collect();
    let mut v = json!({});
    for (a, t) in all {
        v[a] = t.join().unwrap_or(Value::Null);
    }
    let mut g = m.lock().unwrap();
    *g = (v, Some(Instant::now()), false);
    cv.notify_all();
}

// The agents for a hello: the first probe is waited for (at most 3 s); one older than 5 minutes
// is probed again in the background and this hello gets the old one.
fn agents_now() -> Value {
    let (m, cv) = agents_cell();
    let mut g = m.lock().unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while g.1.is_none() {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        g = cv.wait_timeout(g, left).unwrap().0;
    }
    if g.1.is_some_and(|t| t.elapsed() > Duration::from_secs(300)) && !g.2 {
        thread::spawn(probe_agents);
    }
    g.0.clone()
}

// `full`: a protocol 5 client, which also gets pi and opencode sessions and the protocol 5 fields.
// ifChanged: rows identical to the ones last sent on this connection are not sent again, which
// spares the client parsing them.
fn history_reply(shared: &Shared, past: &Past, cid: u64, req: &Value, full: bool) {
    let rows = {
        let mut p = past.lock().unwrap();
        p.refresh();
        Value::Array(p.list_for(full)).to_string()
    };
    let mut hs = DefaultHasher::new();
    rows.hash(&mut hs);
    let hash = hs.finish();
    let tag = req.get("tag").map(|t| format!(",\"tag\":{t}")).unwrap_or_default();
    let same = {
        let mut h = shared.lock().unwrap();
        let Some(c) = h.clients.get_mut(&cid) else { return };
        let same = c.history == hash;
        c.history = hash;
        same
    };
    let line = if same && req["ifChanged"] == true {
        format!("{{\"ev\":\"history\",\"unchanged\":true{tag}}}\n")
    } else {
        format!("{{\"ev\":\"history\",\"sessions\":{rows}{tag}}}\n")
    };
    shared.lock().unwrap().to_client(cid, &line);
}

fn locate(past: &Past, id: &str) -> Result<(PathBuf, &'static str), String> {
    let mut p = past.lock().unwrap();
    p.locate(id).or_else(|| { p.refresh(); p.locate(id) }).ok_or(format!("no transcript for {id}"))
}

fn past_op(shared: &Shared, past: &Past, op: &str, req: &Value, full: bool) -> Result<Value, String> {
    match op {
        "search" => {
            // the words search reads are written by refresh: a session's newest words are found too
            let known = {
                let mut p = past.lock().unwrap();
                p.refresh();
                p.summaries_for(full)
            };
            let q = req["q"].as_str().unwrap_or("");
            Ok(json!({"ev": "search", "q": q, "results": history::search(q, &known)}))
        }
        _ => {
            let id = req["id"].as_str().ok_or("transcript needs an id")?;
            let (path, tool) = locate(past, id)?;
            if op == "results" || op == "read_reports" || op == "ask" {
                let agents = if tool == "claude" { agents_reply(shared, &path, id) } else { json!({}) };
                if op == "read_reports" {
                    return Ok(json!({"ev": op, "id": id, "queued": results::read_reports(&path, tool, &agents, id)}));
                }
                if op == "ask" {
                    let q = req["q"].as_str().filter(|q| !q.trim().is_empty()).ok_or("ask needs a question")?;
                    let mut a = results::ask(&path, tool, &agents, id, q)?;
                    a["ev"] = json!("ask");
                    a["id"] = json!(id);
                    a["q"] = json!(q);
                    return Ok(a);
                }
                let mut r = results::results(&path, tool, &agents, id);
                r["ev"] = json!("results");
                r["id"] = json!(id);
                return Ok(r);
            }
            if op == "agents" || op == "agent" {
                if tool != "claude" {
                    return Ok(json!({"ev": op, "id": id, "agents": [], "workflows": [], "shells": [], "items": [], "readable": false, "tool": tool}));
                }
                if op == "agent" {
                    let agent = req["agent"].as_str().ok_or("agent needs an agent")?;
                    if !agent.chars().all(|c| c.is_ascii_alphanumeric()) {
                        return Err(format!("not an agent id: {agent}"));
                    }
                    let mut a = history::agent_items(&path, agent);
                    a["ev"] = json!("agent");
                    a["id"] = json!(id);
                    a["agent"] = json!(agent);
                    return Ok(a);
                }
                let mut a = agents_reply(shared, &path, id);
                a["ev"] = json!("agents");
                a["id"] = json!(id);
                return Ok(a);
            }
            if op == "changes" {
                let mut c = history::changes(&path, tool, req["before"].as_str());
                swept(&mut c, id);
                c["ev"] = json!("changes");
                c["id"] = json!(id);
                return Ok(c);
            }
            if let Some(after) = req["after"].as_u64() {
                let (items, offset) = history::transcript_from(&path, tool, after, true);
                return Ok(json!({"ev": "transcript", "id": id, "tool": tool, "after": after, "offset": offset, "items": items}));
            }
            let limit = req["limit"].as_u64().unwrap_or(400) as usize;
            if req["before"].is_null() {
                // read from the end: "startByte" is where the page begins (opencode: a message's
                // number), sent back as "beforeByte" for the one before it (0: this is the first)
                let (items, start, offset) = history::transcript_tail_of(&path, tool, limit, req["beforeByte"].as_u64());
                return Ok(json!({"ev": "transcript", "id": id, "tool": tool, "startByte": start, "offset": offset, "items": items}));
            }
            let (items, offset) = history::transcript_from(&path, tool, 0, true);
            let end = req["before"].as_u64().map_or(items.len(), |b| (b as usize).min(items.len()));
            let start = end.saturating_sub(limit);
            Ok(json!({"ev": "transcript", "id": id, "tool": tool, "total": items.len(), "start": start, "offset": offset, "items": items[start..end]}))
        }
    }
}

// A session's agents, workflows and commands, with what its process says is running (B3), and
// each task's stop record until that task's state changes from the one first shown with it.
fn agents_reply(shared: &Shared, path: &Path, id: &str) -> Value {
    let (live, stops) = {
        let h = shared.lock().unwrap();
        let Some(s) = h.sessions.get(id) else {
            drop(h);
            return history::agents(path);
        };
        let running = s.tasks.iter().flat_map(|(t, c)| [t.clone(), c.clone()]).filter(|x| !x.is_empty()).collect();
        (history::LiveTasks { running, alive: s.to_child.is_some() }, s.stops.clone())
    };
    let mut a = history::agents_live(path, &live);
    if stops.is_empty() {
        return a;
    }
    let mut seen: HashMap<String, Option<String>> = HashMap::new(); // task -> its state (None: changed)
    for list in ["agents", "workflows", "shells"] {
        for e in a[list].as_array_mut().into_iter().flatten() {
            let Some(task) = e["task"].as_str().map(str::to_string) else { continue };
            let Some((rec, was)) = stops.get(&task) else { continue };
            let now = e["state"].as_str().unwrap_or("").to_string();
            if was.as_ref().is_none_or(|w| *w == now) {
                e["stop"] = rec.clone();
                seen.insert(task, Some(now));
            } else {
                seen.insert(task, None);
            }
        }
    }
    let mut h = shared.lock().unwrap();
    if let Some(s) = h.sessions.get_mut(id) {
        for (task, state) in seen {
            match state {
                Some(now) => {
                    if let Some(x) = s.stops.get_mut(&task).filter(|x| x.1.is_none()) {
                        x.1 = Some(now);
                    }
                }
                None => {
                    s.stops.remove(&task);
                }
            }
        }
    }
    a
}

// What the host's sweeps after each turn found (made_store), added to a changes reply's `made`:
// files still there that the turn's tools did not already name. A file a tool edited (rather
// than created) is a change, not something made.
fn swept(c: &mut Value, id: &str) {
    let found = history::made_load(id);
    if found.is_empty() {
        return;
    }
    let edited: HashSet<&str> = c["files"].as_array().into_iter().flatten()
        .filter(|f| f["created"] != true).filter_map(|f| f["path"].as_str()).collect();
    let mut made: Vec<Value> = c["made"].as_array().cloned().unwrap_or_default();
    for m in found {
        let Some(p) = m["path"].as_str() else { continue };
        if edited.contains(p) || made.iter().any(|x| x["path"] == p) {
            continue;
        }
        let Ok(meta) = fs::metadata(p) else { continue };
        let mut m = m.clone();
        m["size"] = json!(meta.len());
        made.push(m);
    }
    made.sort_by(|a, b| b["at"].as_u64().cmp(&a["at"].as_u64()));
    c["made"] = Value::Array(made);
}

// The line protocol. Bump it when an op or an event changes shape: the shell is reloaded many
// times a day while this daemon keeps running, so old-daemon/new-shell is the normal state after a
// pull, and the shell compares this against the number it was written for (modules/claude/Host.qml).
const PROTO: u32 = 6;

// SIGTERM (systemd stopping the unit), SIGINT and SIGHUP end the host through here: the flag is all
// the handler does, a thread stops the opencode servers and leaves.
static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
extern "C" {
    fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
}
extern "C" fn on_stop(_: i32) {
    STOP.store(true, std::sync::atomic::Ordering::Relaxed);
}
fn stop_on_signal(socket: String) {
    for sig in [1, 2, 15] {
        unsafe { signal(sig, on_stop) };
    }
    thread::spawn(move || {
        while !STOP.load(std::sync::atomic::Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }
        opencode::stop_all();
        let _ = fs::remove_file(&socket);
        process::exit(0);
    });
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let socket = args.iter().position(|a| a == "--socket").and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| format!("{}/agent-host.sock", env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into())));
    let idle = Duration::from_secs(env::var("AGENT_HOST_IDLE").ok().and_then(|v| v.parse().ok()).unwrap_or(600));
    if UnixStream::connect(&socket).is_ok() {
        eprintln!("agent-host: already running at {socket}");
        process::exit(1);
    }
    let _ = fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket).unwrap_or_else(|e| {
        eprintln!("agent-host: {socket}: {e}");
        process::exit(1)
    });
    let _ = fs::set_permissions(&socket, fs::Permissions::from_mode(0o600));
    let shared: Shared = Arc::new(Mutex::new(Host::default()));
    stop_on_signal(socket.clone());
    thread::spawn(opencode::sweep_orphans);
    let past: Past = Arc::new(Mutex::new(History::default()));
    accounts::start(shared.clone());
    thread::spawn(probe_agents);
    let first = past.clone();
    thread::spawn(move || {
        history::prune_media(30);
        first.lock().unwrap().refresh(); // index before the app first asks
    });

    let reaper = shared.clone();
    thread::spawn(move || loop {
        thread::sleep(idle.min(Duration::from_secs(30)));
        let mut h = reaper.lock().unwrap();
        // a session whose process still runs background agents or commands is left running (B15)
        let reaped: Vec<String> = h.sessions.values()
            .filter(|s| s.to_child.is_some() && s.state == "idle" && s.last.elapsed() >= idle && s.tasks.is_empty())
            .map(|s| s.id.clone()).collect();
        // Unannounced, every client goes on believing the session is alive and sends into a
        // closed stdin, which comes back as "still closing" with the message lost.
        for id in reaped {
            close(&mut h, &id, "idle");
            h.announce(&id);
        }
        // a shared server with no session left on it goes too
        codex::reap(&mut h, idle);
        opencode::reap(&mut h, idle);
    });

    eprintln!("agent-host: {} (protocol {PROTO}) listening at {socket}, idle {}s",
              env!("CARGO_PKG_VERSION"), idle.as_secs());
    for stream in listener.incoming().flatten() {
        let (sh, pa) = (shared.clone(), past.clone());
        thread::spawn(move || serve(sh, pa, stream));
    }
}
