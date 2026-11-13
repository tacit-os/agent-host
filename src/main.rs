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
//!   → {"op":"fork","id":ID}   {"op":"delete","id":ID,"stop":?BOOL}   ← {"ev":"forked","id":NEW,"from":ID,"tool":T}   {"ev":"deleted","id":ID,"how":"trash"|"archive"}
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
//!   ← {"ev":"exit","id":ID,"code":N,"reason":"idle"|"closed"|"exited"|"crashed"|"signedOut"|"expired","stderr":[..],"ended":?"oom"}   {"ev":"ok"}   {"ev":"error","message":..}
//!     (ended "oom": the kernel's out-of-memory killer ended it, and the session's state says so until its next process)
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
    to_default: Vec<&'static str>, // codex: "model", "effort" chosen as the default, sent as such with its next turn
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
    limited: Option<(u64, String)>, // the plan's usage limit stopped it: when that resets (unix s, 0: not said), on which account
    ended: Option<&'static str>, // its last process was ended by the out-of-memory killer ("oom"), until the next starts
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
            to_default: Vec::new(),
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
            limited: None,
            ended: None,
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
        if let Some(at) = self.limit(now_ms()) {
            s["limited"] = json!({"resetsAt": (at > 0).then_some(at)});
        }
        if let Some(e) = self.ended {
            s["ended"] = json!(e);
        }
        s
    }

    // The usage limit that stopped it, while it holds: until its reset time, and on the account it was reached on.
    fn limit(&self, now: u64) -> Option<u64> {
        let (at, on) = self.limited.as_ref()?;
        (*on == accounts::account_id(self.tool, self.account.as_deref()) && (*at == 0 || at * 1000 > now)).then_some(*at)
    }

    // Stopped by the usage limit `info` describes (Claude Code's rate_limit_info).
    fn limit_by(&mut self, info: &Value) {
        self.limited = Some((info["resetsAt"].as_u64().unwrap_or(0), accounts::account_id(self.tool, self.account.as_deref())));
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
    // the keys the request being handled will start a process with, read before the lock (keys_ahead)
    ready: Option<Ready>,
}

type Shared = Arc<Mutex<Host>>;
type Ready = (&'static str, Option<String>, Result<accounts::Spawn, String>); // agent, account, its keys
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

// A session's process command (accounts::command_with), from the keys keys_ahead read when they are its agent's
// and account's.
fn session_command(h: &mut Host, agent: &str, account: Option<&str>) -> Result<(Command, Option<String>), String> {
    match h.ready.take() {
        Some((a, acct, keys)) if a == agent && acct.as_deref() == account => keys.map(|sp| accounts::command_from(agent, sp)),
        _ => accounts::command_with(agent, account),
    }
}

// The keys a request that starts a Claude Code or pi process will need, read before the host's lock is taken: the
// keyring can take seconds to answer (busctl, then secret-tool, for every key), and every session's output, client
// and permission answer waits on that lock. None: the request starts no process.
fn keys_ahead(shared: &Shared, req: &Value, tool: Option<&'static str>) -> Option<Ready> {
    let agent = tool.filter(|t| matches!(*t, "claude" | "pi"))?;
    let op = req["op"].as_str().unwrap_or("");
    if !matches!(op, "start" | "send" | "rewind" | "rewind_files" | "side_question" | "settings") {
        return None;
    }
    let id = if op == "start" { req["resume"].as_str().or(req["id"].as_str()) } else { req["id"].as_str() };
    // a start's account replaces the session's (take_requests)
    let asked = req["account"].as_str().filter(|_| op == "start").map(str::to_string);
    let live = id.and_then(|id| shared.lock().unwrap().sessions.get(id).map(|s| (s.to_child.is_some() || s.pid != 0, s.account.clone())));
    let account = match live {
        Some((true, _)) => return None, // its process runs, or is still closing: nothing is started
        Some((false, had)) => asked.or(had),
        None if op == "start" => asked,
        None => return None,
    };
    let keys = accounts::keys_for(agent, account.as_deref());
    Some((agent, account, keys))
}

// `cmd` run in a systemd scope of its own with a memory cap, so one runaway agent cannot take the
// machine with it. MemoryMax counts memory only: with no swap cap a session past MemoryHigh went on
// into swap (a session reached 3 GB plus 9.2 GB of swap, 2026-11-12) until the whole desktop froze.
fn in_scope(cmd: &Command, unit: &str) -> Command {
    let mut s = Command::new("systemd-run");
    s.args(["--user", "--scope", "--quiet", "--collect", "-p", "MemoryHigh=3G", "-p", "MemoryMax=5G", "-p", "MemorySwapMax=2G"])
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
    // One process per conversation. Another that has it open (a terminal, or the one a previous host started, which
    // finishes its turn after the host has gone) would write the same transcript beside this one. The host has no
    // process of its own for the session here: start, wake and fork spawn only once it has exited.
    if let Some(pid) = history::holder_outside(id, 0) {
        return Err(format!("Another Claude Code process (pid {pid}) has this session open. Send again once it has closed."));
    }
    h.next_gen += 1;
    let gen = h.next_gen;
    let account = h.sessions.get(id).ok_or("no such session")?.account.clone();
    let (mut cmd, key) = session_command(h, "claude", account.as_deref())?;
    let s = h.sessions.get_mut(id).ok_or("no such session")?;
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
    s.ended = None;
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

// Each line of `r` until it ends, a byte that is not UTF-8 read as U+FFFD. BufRead::lines() fails on such a line,
// and a reader that stopped there left the process writing into a pipe nobody drained.
fn each_line(r: impl std::io::Read, mut f: impl FnMut(String)) {
    let mut r = BufReader::new(r);
    let mut buf = Vec::new();
    while r.read_until(b'\n', &mut buf).is_ok_and(|n| n > 0) {
        f(String::from_utf8_lossy(&buf).trim_end_matches(['\n', '\r']).to_string());
        buf.clear();
    }
}

fn read_out<R: std::io::Read>(shared: Shared, id: String, gen: u64, out: R, child: Child) {
    read_out_in(shared, id, gen, out, child, Path::new("/sys/fs/cgroup"));
}

// `cgroups`: where the cgroup tree is (a test's copy of it).
fn read_out_in<R: std::io::Read>(shared: Shared, id: String, gen: u64, out: R, mut child: Child, cgroups: &Path) {
    // the process's scope, found at its first line: systemd-run has moved into the scope before it starts the agent
    let (pid, mut scope, began) = (child.id(), None, now_ms() / 1000);
    each_line(out, |line| {
        if scope.is_none() {
            scope = Some(scope_of(cgroups, pid));
        }
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            on_event(&mut shared.lock().unwrap(), &id, gen, v);
        }
    });
    // Read at once: systemd removes the scope a few ms after its last process has gone (--collect), its count with it
    let events = scope.flatten().map(|d| (fs::read_to_string(d.join("memory.events")).ok(), d));
    let code = child.wait().ok().and_then(|s| s.code());
    // An out-of-memory kill in the scope ends the session either way: the agent's own, or systemd stopping the scope
    // after another process's (OOMPolicy=stop). Asked only of an exit the host did not ask for, outside its lock.
    let asked = shared.lock().unwrap().sessions.get(&id).is_none_or(|s| s.gen != gen || s.closing.is_some() || s.auth.is_some());
    let oom = !asked && events.is_some_and(|(e, d)| e.map_or_else(|| oom_logged(&d, began), |e| oom_kills(&e) > 0));
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
            let (closing, auth) = (s.closing.take(), s.auth.take());
            s.ended = (oom && closing.is_none() && auth.is_none()).then_some("oom");
            match (closing, auth) {
                (Some(r), _) => (r, Vec::new()),
                (None, Some(r)) => (r, s.stderr.iter().cloned().collect()),
                (None, None) if code == Some(0) => ("exited", Vec::new()),
                (None, None) => ("crashed", s.stderr.iter().cloned().collect()),
            }
        }
        _ => return,
    };
    let mut exit = json!({"ev": "exit", "id": id, "code": code, "reason": reason, "stderr": stderr});
    if let Some(e) = h.sessions.get(&id).and_then(|s| s.ended) {
        exit["ended"] = json!(e);
    }
    h.to_all(&exit);
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

// The cgroup of the scope process `pid` runs in, under `cgroups`; None outside a scope (the host's own service).
fn scope_of(cgroups: &Path, pid: u32) -> Option<PathBuf> {
    let cg = fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    let path = cg.lines().find_map(|l| l.strip_prefix("0::"))?;
    path.ends_with(".scope").then(|| cgroups.join(path.trim_start_matches('/')))
}

// How many processes the out-of-memory killer has ended in a scope, from its memory.events (oom_kill).
fn oom_kills(events: &str) -> u64 {
    events.lines().find_map(|l| l.strip_prefix("oom_kill ")?.trim().parse().ok()).unwrap_or(0)
}

// Whether systemd logged that the out-of-memory killer ended processes in the scope `dir` was (its unit) since `since`
// (unix s), when the scope has gone: SD_MESSAGE_UNIT_OUT_OF_MEMORY in the user journal, written as systemd saw the kill,
// before it removed the scope. Asked again briefly for a record journald has not written yet; no journal reads as none.
fn oom_logged(dir: &Path, since: u64) -> bool {
    let Some(unit) = dir.file_name().and_then(|u| u.to_str()) else { return false };
    for _ in 0..10 {
        let out = Command::new("journalctl").args(["--user", "--no-pager", "-q", "-o", "cat", "-u", unit, "MESSAGE_ID=fe6faa94e7774663a0da52717891d8ef"])
            .arg(format!("--since=@{}", since.saturating_sub(1))).stdin(Stdio::null()).stderr(Stdio::null()).output();
        match out {
            Ok(o) if o.status.success() && !o.stdout.is_empty() => return true,
            Ok(o) if o.status.success() => thread::sleep(Duration::from_millis(50)),
            _ => return false,
        }
    }
    false
}

fn read_err(shared: Shared, id: String, gen: u64, err: ChildStderr) {
    each_line(err, |line| {
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
    });
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
        // the plan's usage limit, as Claude Code reports each change of it: refused, and not run on paid extra usage,
        // every turn stops at once until it resets
        "rate_limit_event" => {
            let (i, was) = (&v["rate_limit_info"], s.limited.take());
            if i["status"] == "rejected" && i["isUsingOverage"] != true {
                s.limit_by(i);
            }
            changed = was != s.limited;
        }
        "result" => {
            if s.pending.is_empty() {
                s.state = "idle";
            }
            // a sign-in that no longer works ends the session with why (exit.reason)
            if v["is_error"] == true {
                auth = accounts::auth_failure(s.tool, v["result"].as_str().unwrap_or(""))
                    .or((v["authError"] == true).then_some("signedOut"));
            } else {
                s.limited = None; // a turn that ended well: the limit has lifted
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
            // the reply a turn stopped by the limit ends on says when it resets too (a process that started limited
            // may never see the limit change)
            if v["api_error"] == "usage_limit_reached" {
                s.limit_by(&v["api_error_params"]["rate_limit_info"]);
                changed = true;
            }
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

// A usage limit whose reset time has passed, or that its session's account no longer has, is let go and said.
fn lift_limits(h: &mut Host) {
    let now = now_ms();
    let lifted: Vec<String> = h.sessions.values().filter(|s| s.limited.is_some() && s.limit(now).is_none()).map(|s| s.id.clone()).collect();
    for id in lifted {
        h.sessions.get_mut(&id).unwrap().limited = None;
        h.announce(&id);
    }
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
    let fresh = !h.sessions.contains_key(&id);
    let s = h.sessions.entry(id.clone()).or_insert_with(|| Session::new(&id, cwd, tool));
    if s.tool != tool {
        return Err(format!("{id} is a {} session", s.tool));
    }
    if resume.is_some() {
        s.used = true;
    }
    take_requests(s, req);
    if s.to_child.is_none() && s.pid == 0 {
        if let Err(e) = spawn(shared, h, &id) {
            // a session that never ran (a locked keyring, a removed account) is not kept, nor is it recorded as ours
            if fresh {
                h.sessions.remove(&id);
            }
            return Err(e);
        }
    }
    if resume.is_none() && req["id"].as_str().is_none() {
        own(&id);
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

// Every permission request still open on the session is refused, for an interrupt. An error (the agent had
// already dropped it) is nothing to report: the card is gone either way.
fn refuse_pending(h: &mut Host, id: &str) {
    let open: Vec<Value> = h.sessions.get(id).map(|s| s.pending.iter().map(|p| p["request"].clone()).collect()).unwrap_or_default();
    for rid in open {
        let _ = answer(h, id, &json!({"op": "answer", "id": id, "request": rid, "allow": false, "message": "Stopped by the user.", "interrupt": true}));
    }
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
    if req["interrupt"] == true && req["allow"].as_bool() != Some(true) {
        response["interrupt"] = json!(true);
    }
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
            // The cards the turn is stopped under are refused first: agents do not all take a stop as the end
            // of a question, and a card left standing kept asking about a tool that would never run (a
            // follow-up sent instead of an answer left it up until Cancel was pressed).
            refuse_pending(h, &id);
            let stopped = match tool {
                "codex" => codex::interrupt(h, &id),
                "opencode" => opencode::interrupt(h, &id),
                "pi" => pi::interrupt(h, &id),
                _ => h.control(&id, json!({"subtype": "interrupt"})).map(drop),
            };
            h.announce(&id);
            stopped.map(|_| ok)
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
            s.to_default.retain(|f| *f != "effort");
            if tool == "codex" && value.is_none() {
                s.to_default.push("effort");
            }
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
                s.to_default.retain(|f| *f != "model");
                if tool == "codex" && default {
                    s.to_default.push("model");
                }
            }
            // Codex takes both with its next turn (a default as the one a new thread gets: codex::defaults), opencode a
            // model; an opencode mode changes its rules now
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
        let ready = keys_ahead(&shared, &req, tool);
        let mut h = shared.lock().unwrap();
        h.ready = ready;
        let mut reply = if req.is_object() {
            handle(&shared, &mut h, cid, &req, tool).unwrap_or_else(|e| json!({"ev": "error", "message": e}))
        } else {
            json!({"ev": "error", "message": "not a JSON object"})
        };
        h.ready = None; // keys nothing started with are not kept
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
            let ready = ("claude", s.account.clone(), accounts::keys_for("claude", s.account.as_deref()));
            let mut h = shared.lock().unwrap();
            h.sessions.insert(id.clone(), s);
            h.ready = Some(ready);
            let started = spawn(shared, &mut h, &id);
            h.ready = None;
            if let Err(e) = started {
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

// Why a delete is refused while process `pid` has the session open: a terminal, or the process a previous host
// started, whose turn Praxis follows until it ends (Host.qml's leftovers), in Praxis's words for it.
fn held_by(pid: u32, id: &str) -> String {
    if history::in_session_scope(pid, id) { "Still finishing a turn from before the background service restarted".into() } else { "Open in a terminal".into() }
}

// A session gone from the app: Claude Code's and pi's files to the Trash; Codex and opencode keep
// theirs, archived (their own way of hiding a session, which can be undone in them).
fn delete(shared: &Shared, past: &Past, req: &Value) -> Result<Value, String> {
    let id = req["id"].as_str().ok_or("delete needs an id")?;
    // refused before anything is stopped: a terminal that has the session open (the host's own process for it is not one).
    // With `stop`, a turn the previous host's process is finishing is ended first, as Praxis's Stop ends it (TERM to
    // that process), and waited for.
    let own = shared.lock().unwrap().sessions.get(id).map_or(0, |s| s.pid);
    let (stop, until, mut ended) = (req["stop"] == true, Instant::now() + Duration::from_secs(20), None);
    while let Some(pid) = history::holder_outside(id, own) {
        if ended != Some(pid) {
            if !stop || !history::in_session_scope(pid, id) {
                return Err(held_by(pid, id));
            }
            let _ = Command::new("kill").args(["-s", "TERM", "--", &pid.to_string()]).stdin(Stdio::null()).stderr(Stdio::null()).status();
            ended = Some(pid);
        }
        if Instant::now() > until {
            return Err("The session is still closing; try again.".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
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
    if let Some(pid) = history::terminal_holder(id) {
        return Err(held_by(pid, id));
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
                // a turn is running only while the host's session is mid-turn (working, or waiting on an answer)
                let working = shared.lock().unwrap().sessions.get(id).is_some_and(|s| s.state != "idle");
                if op == "read_reports" {
                    return Ok(json!({"ev": op, "id": id, "queued": results::read_reports(&path, tool, &agents, id, working)}));
                }
                if op == "ask" {
                    let q = req["q"].as_str().filter(|q| !q.trim().is_empty()).ok_or("ask needs a question")?;
                    let mut a = results::ask(&path, tool, &agents, id, working, q)?;
                    a["ev"] = json!("ask");
                    a["id"] = json!(id);
                    a["q"] = json!(q);
                    return Ok(a);
                }
                let mut r = results::results(&path, tool, &agents, id, working);
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
        lift_limits(&mut h);
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

#[cfg(test)]
mod tests {
    use super::*;

    // HOME and the accounts file for these tests (and any other that reaches the accounts state): start
    // writes ~/.local/state, delete reads ~/.claude/sessions, accounts keep their folders under ~/.local/share
    pub(crate) fn scratch_home() -> PathBuf {
        static HOME: OnceLock<PathBuf> = OnceLock::new();
        HOME.get_or_init(|| {
            let p = env::temp_dir().join(format!("agent-host-main-test-{}", process::id()));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).unwrap();
            env::set_var("HOME", &p);
            env::set_var("AGENT_HOST_CONFIG", p.join(".config/agent-host"));
            p
        })
        .clone()
    }

    // The opencode database under scratch_home, for the tests that write it: one at a time, or sqlite refuses one.
    pub(crate) fn opencode_db_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    // The host's own drop-in caps its swap as in_scope caps a session's: MemoryMax counts memory only, so with no
    // MemorySwapMax a host past MemoryHigh went on into swap without a bound.
    #[test]
    fn the_host_caps_its_swap_as_its_sessions_do() {
        let conf = include_str!("../contrib/systemd/agent-host.service.d/90-memory.conf");
        let set = |key: &str| conf.lines().find_map(|l| l.trim().strip_prefix(&format!("{key}=")).map(str::to_string));
        let scope = in_scope(&Command::new("true"), "agent-test");
        let args: Vec<String> = scope.get_args().map(|a| a.to_string_lossy().into_owned()).collect();
        let session = args.iter().find_map(|a| a.strip_prefix("MemorySwapMax=")).map(str::to_string);
        assert!(session.is_some(), "sessions run with no swap cap: {args:?}");
        assert!(set("MemoryMax").is_some(), "the host's drop-in has no MemoryMax");
        assert_eq!(set("MemorySwapMax"), session, "the host's swap is not capped as its sessions' is");
    }

    #[test]
    fn a_start_that_cannot_spawn_leaves_no_session() {
        let home = scratch_home();
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let mut h = shared.lock().unwrap();
        // a codex session has no process of its own, so its spawn is refused
        let r = start(&shared, &mut h, &json!({"op": "start", "cwd": home.to_str().unwrap()}), "codex");
        assert!(r.is_err());
        assert!(h.sessions.is_empty(), "the failed start left a session in the list");
        assert!(!history::own_file().exists(), "the failed start was recorded as ours");
    }

    // ~/.claude/sessions/<pid>.json as Claude Code writes it for a process that has the session open
    fn hold(home: &Path, id: &str, pid: u32) {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        let start = stat.rsplit_once(')').unwrap().1.split_whitespace().nth(19).unwrap().to_string();
        let dir = home.join(".claude/sessions");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(format!("{pid}.json")), json!({"sessionId": id, "pid": pid, "procStart": start}).to_string()).unwrap();
    }

    #[test]
    fn a_refused_delete_has_not_closed_the_session() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d001";
        hold(&home, id, process::id());
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let (tx, _rx) = channel::<String>();
        {
            let mut h = shared.lock().unwrap();
            let mut s = Session::new(id, home.to_str().unwrap(), "claude");
            s.to_child = Some(tx);
            h.sessions.insert(id.into(), s);
        }
        let past: Past = Arc::new(Mutex::new(History::default()));
        let r = delete(&shared, &past, &json!({"op": "delete", "id": id}));
        assert_eq!(r.unwrap_err(), "Open in a terminal");
        let h = shared.lock().unwrap();
        assert!(h.sessions[id].to_child.is_some(), "the refused delete closed the session's process");
    }

    // A line that is not UTF-8 is read on past, on stdout and on stderr: the readers ended at it, the process's
    // pipes were no longer drained, and the session was never seen to exit.
    #[test]
    fn a_line_that_is_not_utf8_is_read_on_past() {
        let id = "4a0f1c2e-0000-4000-8000-00000000d004";
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let mut s = Session::new(id, "/", "claude");
        (s.gen, s.pid) = (1, 1);
        shared.lock().unwrap().sessions.insert(id.into(), s);
        let mut child = Command::new("sh").args(["-c", r"printf 'bad \377 byte\nlater\n' >&2"]).stderr(Stdio::piped()).spawn().unwrap();
        read_err(shared.clone(), id.into(), 1, child.stderr.take().unwrap());
        let out = b"\xff not json\n{\"type\": \"system\", \"subtype\": \"init\", \"model\": \"after-the-bad-line\"}\n";
        read_out(shared.clone(), id.into(), 1, &out[..], child);
        let h = shared.lock().unwrap();
        let s = &h.sessions[id];
        assert_eq!(s.model, "after-the-bad-line");
        assert_eq!((s.pid, s.stderr.iter().cloned().collect::<Vec<_>>()), (0, vec!["bad \u{FFFD} byte".to_string(), "later".to_string()]));
    }

    // The exit a session's process ended with, and its state after: what read_out tells clients.
    fn exit_and_state(shared: &Shared, id: &str, mut child: Child, cgroups: &Path) -> (Value, Value) {
        let (a, b) = UnixStream::pair().unwrap();
        shared.lock().unwrap().clients.insert(1, Client { out: a, watching: HashSet::new(), proto: 5, history: 0, accounts: false });
        let stdout = child.stdout.take().unwrap();
        read_out_in(shared.clone(), id.into(), 1, stdout, child, cgroups);
        b.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let exit = BufReader::new(b).lines().map_while(Result::ok).filter_map(|l| serde_json::from_str::<Value>(&l).ok())
            .find(|v| v["ev"] == "exit").expect("an exit");
        let state = shared.lock().unwrap().sessions[id].summary();
        (exit, state)
    }

    // A process the out-of-memory killer ended leaves its session saying so: ended "oom" on its exit and in its state,
    // from its scope's memory.events (a copy of the cgroup tree here, with the count in it). Another crash does not,
    // nor a process the host closed. Runs where the test's children are in a scope (a terminal's, or agent-host's).
    #[test]
    fn an_out_of_memory_kill_is_said() {
        let root = env::temp_dir().join(format!("agent-host-cgroups-{}", process::id()));
        for (n, (kills, closing, reason, ended)) in [(1, None, "crashed", Some("oom")), (0, None, "crashed", None), (1, Some("idle"), "idle", None)]
            .into_iter().enumerate() {
            let id = format!("4a0f1c2e-0000-4000-8000-00000000d10{n}");
            let shared: Shared = Arc::new(Mutex::new(Host::default()));
            let mut s = Session::new(&id, "/", "claude");
            (s.gen, s.pid, s.closing) = (1, 1, closing);
            shared.lock().unwrap().sessions.insert(id.clone(), s);
            // its first line, then killed once the test lets go of its stdin
            let mut child = Command::new("sh").args(["-c", r#"echo '{"type": "system", "subtype": "init", "model": "m"}'; read x; kill -9 $$"#])
                .stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
            let Some(scope) = scope_of(&root, child.id()) else {
                eprintln!("an_out_of_memory_kill_is_said: the test does not run in a scope, so there is nothing to read");
                let _ = child.kill();
                let _ = child.wait();
                return;
            };
            fs::create_dir_all(&scope).unwrap();
            fs::write(scope.join("memory.events"), format!("low 0\nhigh 40\nmax 12\noom 1\noom_kill {kills}\noom_group_kill 0\n")).unwrap();
            let stdin = child.stdin.take();
            let (sh, i, r) = (shared.clone(), id.clone(), root.clone());
            let reader = thread::spawn(move || exit_and_state(&sh, &i, child, &r));
            let until = Instant::now() + Duration::from_secs(5);
            while shared.lock().unwrap().sessions[&id].model != "m" && Instant::now() < until {
                thread::sleep(Duration::from_millis(10));
            }
            drop(stdin);
            let (exit, state) = reader.join().unwrap();
            assert_eq!((exit["reason"].as_str(), exit["ended"].as_str(), state["ended"].as_str()), (Some(reason), ended, ended), "case {n}");
        }
        let _ = fs::remove_dir_all(&root);
    }

    // A real out-of-memory kill, in a real scope with a 64 MB cap, and a real kill that is not one: cargo test --
    // --ignored a_real_out_of_memory_kill. The scope is often gone before its memory.events is read; then the journal says.
    #[test]
    #[ignore = "runs a real systemd scope"]
    fn a_real_out_of_memory_kill_is_said() {
        let fill = r#"import time; print('{"type": "system", "subtype": "init"}', flush=True); time.sleep(0.5); x = bytearray(b"x") * (512 << 20)"#;
        let die = r#"import os, time; print('{"type": "system", "subtype": "init"}', flush=True); time.sleep(0.5); os.kill(os.getpid(), 9)"#;
        for (n, (script, ended)) in [(fill, json!("oom")), (die, Value::Null)].into_iter().enumerate() {
            let id = format!("4a0f1c2e-0000-4000-8000-00000000d10{}", 7 + n);
            let shared: Shared = Arc::new(Mutex::new(Host::default()));
            let mut s = Session::new(&id, "/", "claude");
            (s.gen, s.pid) = (1, 1);
            shared.lock().unwrap().sessions.insert(id.clone(), s);
            let mut child = Command::new("systemd-run")
                .args(["--user", "--scope", "--quiet", "--collect", "-p", "MemoryMax=64M", "-p", "MemorySwapMax=0"])
                .arg(format!("--unit=agent-test-oom-{}-{n}", process::id())).args(["--", "python3", "-c", script])
                .stdout(Stdio::piped()).spawn().unwrap();
            let out = child.stdout.take().unwrap();
            let t = Instant::now();
            read_out(shared.clone(), id.clone(), 1, out, child);
            eprintln!("case {n}: read in {:?}", t.elapsed());
            assert_eq!(shared.lock().unwrap().sessions[&id].summary()["ended"], ended, "case {n}");
        }
    }

    // A delete refused for the process a previous host started for the session, still finishing its turn, says so
    // instead of "Open in a terminal". That process runs in the session's own agent scope, and the holder here is a
    // child of the test, in the test's scope: the leftover half runs only where the test runs in an agent-host scope
    // (from Praxis); from a terminal only the terminal half does.
    #[test]
    fn a_previous_hosts_turn_is_not_called_a_terminal() {
        let home = scratch_home();
        let past: Past = Arc::new(Mutex::new(History::default()));
        let mut cases = vec![("4a0f1c2e-0000-4000-8000-00000000d005".to_string(), "Open in a terminal")];
        if let Some(short) = own_scope() {
            cases.push((format!("{short}-0000-4000-8000-00000000d005"), "Still finishing a turn from before the background service restarted"));
        }
        for (id, said) in cases {
            let mut child = Command::new("sleep").arg("30").spawn().unwrap();
            hold(&home, &id, child.id());
            let shared: Shared = Arc::new(Mutex::new(Host::default()));
            let r = delete(&shared, &past, &json!({"op": "delete", "id": id}));
            let _ = child.kill();
            let _ = child.wait();
            assert_eq!(r.unwrap_err(), said);
        }
    }

    // The first 8 characters of the agent-host scope the test runs in (a session id's that names it), if it runs in one.
    fn own_scope() -> Option<String> {
        let cgroup = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
        cgroup.trim().rsplit('/').next().and_then(|u| u.strip_prefix("agent-")).and_then(|u| u.get(..8))
            .filter(|s| s.chars().all(|c| c.is_ascii_hexdigit())).map(str::to_string)
    }

    // A delete with stop ends the turn a previous host's process is finishing, with TERM as Praxis's Stop does, waits for
    // it and goes on; a terminal's process it never ends. The leftover half runs only where the test runs in an
    // agent-host scope, as above.
    #[test]
    fn a_delete_with_stop_ends_a_previous_hosts_turn() {
        use std::os::unix::process::ExitStatusExt;
        let home = scratch_home();
        let past: Past = Arc::new(Mutex::new(History::default()));
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let id = "4a0f1c2e-0000-4000-8000-00000000d006";
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        hold(&home, id, child.id());
        let r = delete(&shared, &past, &json!({"op": "delete", "id": id, "stop": true}));
        let alive = child.try_wait().unwrap().is_none();
        let _ = child.kill();
        let _ = child.wait();
        assert_eq!((r.unwrap_err().as_str(), alive), ("Open in a terminal", true), "a terminal's process is left running");
        let Some(short) = own_scope() else { return };
        let id = format!("{short}-0000-4000-8000-00000000d006");
        let mut child = Command::new("sleep").arg("5").spawn().unwrap();
        hold(&home, &id, child.id());
        // reaped once it ends, as systemd reaps the previous host's process
        let waited = thread::spawn(move || child.wait().unwrap());
        let r = delete(&shared, &past, &json!({"op": "delete", "id": id, "stop": true}));
        let status = waited.join().unwrap();
        assert_eq!(r.unwrap_err(), format!("no transcript for {id}"), "past the turn, the delete goes on (to a transcript this test has not)");
        assert_eq!(status.signal(), Some(15), "the turn is ended by TERM");
    }

    #[test]
    fn the_hosts_own_process_is_not_a_terminal() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d002";
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        hold(&home, id, child.id());
        // the host's own child, or what it started
        assert_eq!(history::holder_outside(id, child.id()), None);
        assert_eq!(history::holder_outside(id, process::id()), None);
        // with no process of the host's own, or another's, it is a terminal
        assert_eq!(history::holder_outside(id, 0), Some(child.id()));
        assert_eq!(history::holder_outside(id, child.id() + 1), Some(child.id()));
        let _ = child.kill();
        let _ = child.wait();
    }

    // Stopping a turn that waits on a permission card refuses the card, so a follow-up sent instead of an
    // answer does not leave it standing.
    #[test]
    fn an_interrupt_refuses_the_cards_it_stops() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d003";
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let (tx, rx) = channel::<String>();
        let mut h = shared.lock().unwrap();
        let mut s = Session::new(id, home.to_str().unwrap(), "claude");
        s.to_child = Some(tx);
        s.state = "waiting";
        s.pending.push(json!({"request": "r1", "tool": "Bash", "input": {}}));
        h.sessions.insert(id.into(), s);
        handle(&shared, &mut h, 0, &json!({"op": "interrupt", "id": id}), None).unwrap();
        assert!(h.sessions[id].pending.is_empty(), "the card is still pending after an interrupt");
        assert_eq!(h.sessions[id].state, "working");
        let sent: Vec<Value> = rx.try_iter().map(|l| serde_json::from_str(&l).unwrap()).collect();
        assert_eq!(sent[0]["response"]["request_id"], "r1");
        assert_eq!(sent[0]["response"]["response"]["behavior"], "deny");
        assert_eq!(sent[0]["response"]["response"]["interrupt"], true);
        assert_eq!(sent[1]["request"]["subtype"], "interrupt");
    }

    // A session another process has open (here a stand-in with its ~/.claude/sessions record) is not started a second
    // time, by a start or by a message to a session the host has closed. The account is one that does not exist, so
    // that nothing past the check (the old code) can start a real claude: it fails on the account instead.
    #[test]
    fn a_session_another_process_holds_is_not_started_again() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d006";
        let mut other = Command::new("sleep").arg("30").spawn().unwrap();
        hold(&home, id, other.id());
        let shared: Shared = Arc::new(Mutex::new(Host::default()));
        let mut h = shared.lock().unwrap();
        let req = json!({"op": "start", "cwd": home.to_str().unwrap(), "resume": id, "account": "claude-none"});
        let held = format!("Another Claude Code process (pid {}) has this session open. Send again once it has closed.", other.id());
        assert_eq!(start(&shared, &mut h, &req, "claude").unwrap_err(), held);
        assert!(h.sessions.is_empty(), "the refused start left a session in the list");
        let mut s = Session::new(id, home.to_str().unwrap(), "claude");
        (s.used, s.account) = (true, Some("claude-none".into()));
        h.sessions.insert(id.into(), s);
        assert_eq!(send_message(&shared, &mut h, id, "hi", &[], &[]).unwrap_err(), held);
        let _ = other.kill();
        let _ = other.wait();
        // once it has gone the start goes on, here to the missing account
        h.sessions.clear();
        assert_eq!(start(&shared, &mut h, &req, "claude").unwrap_err(), "no such claude account: claude-none");
    }

    // Claude Code 2.1.322's rate_limit_event, and the reply a stopped turn ends on as this machine's transcripts have
    // it: the session's state says limited, with the reset in unix seconds, until the limit lifts.
    #[test]
    fn a_usage_limit_is_in_the_sessions_state_until_it_lifts() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d004";
        let mut h = Host::default();
        h.sessions.insert(id.into(), Session::new(id, home.to_str().unwrap(), "claude"));
        let at = now_ms() / 1000 + 3600;
        let event = |status: &str, at: u64, overage: bool| json!({"type": "rate_limit_event", "uuid": "u", "session_id": id,
            "rate_limit_info": {"status": status, "resetsAt": at, "rateLimitType": "five_hour", "overageStatus": "rejected",
                                "overageDisabledReason": "out_of_credits", "isUsingOverage": overage}});
        let limited = |h: &Host| h.sessions[id].summary().get("limited").cloned();
        relay(&mut h, id, 0, event("rejected", at, false), None);
        assert_eq!(limited(&h), Some(json!({"resetsAt": at})));
        relay(&mut h, id, 0, json!({"type": "result", "subtype": "success", "is_error": true, "api_error": "usage_limit_reached"}), None);
        assert_eq!(limited(&h), Some(json!({"resetsAt": at})), "the turn it stopped ended");
        relay(&mut h, id, 0, event("allowed", at, false), None);
        assert_eq!(limited(&h), None, "allowed again");
        relay(&mut h, id, 0, json!({"type": "assistant", "parent_tool_use_id": null, "session_id": id, "error": "rate_limit",
            "message": {"role": "assistant", "content": [{"type": "text", "text": "You've hit your session limit · resets 7:40am (Europe/Stockholm)"}]},
            "api_error": "usage_limit_reached", "api_error_params": {"rate_limit_info": {"status": "rejected", "resetsAt": at, "isUsingOverage": false}}}), None);
        assert_eq!(limited(&h), Some(json!({"resetsAt": at})), "the stopped reply with no event before it");
        relay(&mut h, id, 0, json!({"type": "result", "subtype": "success", "is_error": false}), None);
        assert_eq!(limited(&h), None, "a turn that ended well");
        relay(&mut h, id, 0, event("rejected", at, true), None);
        assert_eq!(limited(&h), None, "a turn on paid extra usage goes on");
        relay(&mut h, id, 0, event("rejected", now_ms() / 1000 - 1, false), None);
        assert_eq!(limited(&h), None, "its reset time has passed");
        relay(&mut h, id, 0, event("rejected", at, false), None);
        h.sessions.get_mut(id).unwrap().account = Some("claude-work".into());
        assert_eq!(limited(&h), None, "another account");
    }

    #[test]
    fn a_lifted_limit_is_let_go() {
        let home = scratch_home();
        let id = "4a0f1c2e-0000-4000-8000-00000000d005";
        let mut h = Host::default();
        let mut s = Session::new(id, home.to_str().unwrap(), "claude");
        s.limited = Some((now_ms() / 1000 + 3600, "claude-personal".into()));
        h.sessions.insert(id.into(), s);
        lift_limits(&mut h);
        assert!(h.sessions[id].limited.is_some(), "it holds");
        h.sessions.get_mut(id).unwrap().limited.as_mut().unwrap().0 = now_ms() / 1000 - 1;
        lift_limits(&mut h);
        assert!(h.sessions[id].limited.is_none(), "its reset time has passed");
    }
}
