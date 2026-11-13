//! Signing an agent account in and out, and reading its sign-in, for every client alike (plan
//! §2.12). Each agent signs in its own way:
//!   Claude Code  `claude auth login --claudeai|--console`. The CLI gives its automatic address (the
//!                one that returns to its localhost listener) to the browser named by BROWSER, and
//!                prints a manual page ("If the browser didn't open, visit: …") whose code a pasted
//!                `code#state` goes to its stdin as the fallback. Done at exit 0 after "Login successful.".
//!   Codex        a short-lived `codex app-server`: account/login/start (chatgpt or
//!                chatgptDeviceCode), done at account/login/completed. The host opens authUrl.
//!   pi           pi-login.mjs on pi's own SDK: JSON lines out, a pasted code in. The host opens the URL.
//!   opencode     a short-lived `opencode serve` on 127.0.0.1 behind a random password:
//!                POST /provider/{id}/oauth/authorize, then …/oauth/callback. The host opens the URL.
//!                opencode 2: POST /api/integration/{id}/connect/oauth, then its attempt's status until
//!                complete (a pasted code first goes to …/complete; a refused one starts a new attempt).
//!
//! One sign-in runs at a time: a second one gets Busy naming the agent that holds the first. Codex,
//! pi and opencode all take 127.0.0.1:1455 for a ChatGPT sign-in in the browser, so that one also
//! gets Busy while another program listens there (the device code does not use it). A sign-in
//! fails after AGENT_HOST_SIGNIN_TIMEOUT seconds (default 600). Each child runs in a process group
//! of its own, killed on cancel, at the timeout, when the Login is dropped and once a server is
//! done with. Steps reach `on` in order from a thread of their own; the last is Done, Failed,
//! Cancelled or Busy. URLs, codes and pasted codes are never logged.
//!
//! Code is sent only when a pasted code is the one way on (opencode's "code" method) or after a
//! pasted one was refused. Claude Code's standing "Paste code here if prompted" and pi's paste
//! prompt, both raced against the browser's return, are no step: Login::code takes a code at any
//! time while those wait.
//!
//! An added account's folder is passed as CLAUDE_CONFIG_DIR, CODEX_HOME or PI_CODING_AGENT_DIR.
//! Every child's env first loses every *_API_KEY variable and the account variables (plan §2.14);
//! the spec's `env` is added after.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

const HELPER: &str = include_str!("pi-login.mjs");
// Claude Code's browser helper: the CLI runs it with the automatic sign-in address, whose return is
// its localhost listener; it is left to the host to open, so the helper only leaves the address.
const BROWSER_HELPER: &str = r#"#!/bin/sh
[ -n "$AGENT_HOST_BROWSER_URL_FILE" ] || exit 0
printf '%s\n' "$1" > "$AGENT_HOST_BROWSER_URL_FILE.tmp" && mv -f "$AGENT_HOST_BROWSER_URL_FILE.tmp" "$AGENT_HOST_BROWSER_URL_FILE"
"#;
const CHATGPT_PORT: u16 = 1455;
const WAIT: Duration = Duration::from_secs(20);
const SCRUB: [&str; 9] = ["ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "OPENCODE_AUTH_CONTENT", "CLAUDE_CONFIG_DIR",
                          "CODEX_HOME", "PI_CODING_AGENT_DIR", "PI_CODING_AGENT_SESSION_DIR", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"];

#[derive(Clone)]
pub struct LoginSpec {
    pub agent: String,
    pub account: String,
    pub home: Option<PathBuf>,       // the account's folder; None: the agent's own
    pub method: String,              // "browser" | "device" | "console" (Claude Code's Console)
    pub provider: Option<String>,    // pi and opencode: the provider to sign in to
    pub env: Vec<(String, String)>,  // added to the child's env
}

#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Waiting { url: String, opened: bool },  // opened: the CLI or the host opened the browser
    Code { url: String, message: Option<String> },  // url: the page that shows the code ("" none); message: the last one was refused
    Device { url: String, user_code: String },
    Busy { holder: String },                // holder: the agent signing in or holding 1455; "" another program
    Done { email: Option<String>, plan: Option<String> },
    Failed(String),
    Cancelled,
}

impl Step {
    /// The `signin` push's fields (plan §3.2), without account and provider.
    pub fn to_json(&self) -> Value {
        match self {
            Step::Waiting { url, opened } => json!({"step": "waiting", "url": url, "opened": opened}),
            Step::Code { url, message } => {
                let mut v = json!({"step": "code"});
                if !url.is_empty() {
                    v["url"] = json!(url);
                }
                if let Some(m) = message {
                    v["message"] = json!(m);
                }
                v
            }
            Step::Device { url, user_code } => json!({"step": "device", "url": url, "userCode": user_code}),
            Step::Busy { holder } => json!({"step": "busy", "holder": holder}),
            Step::Done { email, plan } => json!({"step": "done", "who": {"email": email, "plan": plan}}),
            Step::Failed(m) => json!({"step": "failed", "message": m}),
            Step::Cancelled => json!({"step": "cancelled"}),
        }
    }
}

pub struct Login {
    run: Arc<Run>,
}

impl Login {
    /// A pasted code: to Claude Code's or the pi helper's stdin, or opencode's callback.
    pub fn code(&self, code: &str) -> Result<(), String> {
        let code = code.trim();
        if code.is_empty() || code.contains(['\n', '\r']) {
            return Err("Paste the code the page shows".into());
        }
        let ended = *self.run.ended.lock().unwrap();
        match &mut *self.run.input.lock().unwrap() {
            Input::Stdin(w) => writeln!(w, "{code}").map_err(|_| "This sign-in has ended".to_string()),
            Input::Codes(tx) => tx.send(code.to_string()).map_err(|_| "This sign-in has ended".to_string()),
            Input::None if ended => Err("This sign-in has ended".into()),
            Input::None => Err(format!("{} takes no code here", name(&self.run.agent))),
        }
    }

    pub fn cancel(&self) {
        // one that has ended has its last step (accounts.rs drops it then): the check below runs the CLI
        if *self.run.ended.lock().unwrap() {
            return;
        }
        // a sign-in the CLI already saved is done, not cancelled (Try again would start a second one)
        let saved = self.run.settled.lock().unwrap().as_ref().and_then(|f| f());
        self.run.end(saved.unwrap_or(Step::Cancelled));
    }

    /// The sign-in's process (claude, the pi helper, codex app-server, opencode serve) while it runs.
    pub fn pid(&self) -> Option<u32> {
        if *self.run.ended.lock().unwrap() {
            return None;
        }
        self.run.procs.lock().unwrap().first().map(|p| p.pid)
    }
}

impl Drop for Login {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// The agent and account of the sign-in under way.
pub fn active() -> Option<(String, String)> {
    ACTIVE.lock().unwrap().as_ref().map(|(_, a, b)| (a.clone(), b.clone()))
}

pub fn start(spec: LoginSpec, on: Box<dyn Fn(Step) + Send>) -> Result<Login, String> {
    let secs = env::var("AGENT_HOST_SIGNIN_TIMEOUT").ok().and_then(|s| s.parse().ok()).filter(|&n| n > 0).unwrap_or(600);
    start_in(spec, on, Duration::from_secs(secs), &crate::home().join(".local/share/agent-host"))
}

fn start_in(spec: LoginSpec, on: Box<dyn Fn(Step) + Send>, timeout: Duration, share: &Path) -> Result<Login, String> {
    let chatgpt = match (spec.agent.as_str(), spec.method.as_str()) {
        ("claude", "browser" | "console") | ("codex", "device") => false,
        ("codex", "browser") => true,
        (a @ ("pi" | "opencode"), m @ ("browser" | "device")) => {
            let p = spec.provider.as_deref().ok_or_else(|| format!("{} signs in to a provider; name one", name(a)))?;
            if p.is_empty() || !p.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) {
                return Err(format!("no such provider: {p}"));
            }
            m == "browser" && p == if a == "pi" { "openai-codex" } else { "openai" }
        }
        (a @ ("claude" | "codex" | "pi" | "opencode"), m) => return Err(format!("{} has no {m} sign-in", name(a))),
        (a, _) => return Err(format!("unknown agent {a}")),
    };
    let (tx, rx) = channel::<Step>();
    thread::spawn(move || {
        for s in rx {
            on(s);
        }
    });
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let run = Arc::new(Run { agent: spec.agent.clone(), id: NEXT.fetch_add(1, Ordering::Relaxed), ended: Mutex::new(false),
                             tx: Mutex::new(Some(tx)), procs: Mutex::new(Vec::new()), input: Mutex::new(Input::None),
                             url: Mutex::new(String::new()), settled: Mutex::new(None) });
    let busy = {
        let mut a = ACTIVE.lock().unwrap();
        let holder = match &*a {
            Some((_, agent, _)) => Some(agent.clone()),
            None if chatgpt && port_busy(CHATGPT_PORT) => Some(port_holder(CHATGPT_PORT)),
            None => None,
        };
        if holder.is_none() {
            *a = Some((run.id, spec.agent.clone(), spec.account.clone()));
        }
        holder
    };
    if let Some(holder) = busy {
        run.end(Step::Busy { holder });
        return Ok(Login { run });
    }
    let started = match spec.agent.as_str() {
        "claude" => claude_login(&run, &spec, share),
        "codex" => codex_login(&run, &spec),
        "pi" => pi_login(&run, &spec, share),
        _ => opencode_login(&run, &spec),
    };
    if let Err(e) = started {
        run.abort();
        return Err(e);
    }
    let r = run.clone();
    thread::spawn(move || {
        let until = Instant::now() + timeout;
        while Instant::now() < until {
            if *r.ended.lock().unwrap() {
                return;
            }
            thread::sleep(Duration::from_millis(100).min(until.saturating_duration_since(Instant::now())));
        }
        let n = timeout.as_secs();
        let span = if n >= 60 && n % 60 == 0 { plural(n / 60, "minute") } else { plural(n, "second") };
        r.end(Step::Failed(format!("Sign-in didn't finish in {span}")));
    });
    Ok(Login { run })
}

// ── one sign-in's state ──────────────────────────────────────────────────────────────────────

// (id, agent, account) of the sign-in under way
static ACTIVE: Mutex<Option<(u64, String, String)>> = Mutex::new(None);

enum Input {
    None,
    Stdin(ChildStdin),
    Codes(Sender<String>),
}

struct Run {
    agent: String,
    id: u64,
    ended: Mutex<bool>,
    tx: Mutex<Option<Sender<Step>>>,  // to the thread that calls `on`
    procs: Mutex<Vec<Arc<Proc>>>,
    input: Mutex<Input>,
    url: Mutex<String>,  // the page that shows the code: the last Waiting's address, or Claude Code's printed one
    settled: Mutex<Option<Box<dyn Fn() -> Option<Step> + Send>>>,  // Claude: Done if the CLI has already saved the sign-in
}

impl Run {
    fn step(&self, s: Step) {
        let ended = self.ended.lock().unwrap();
        if !*ended {
            if let Step::Waiting { url, .. } = &s {
                *self.url.lock().unwrap() = url.clone();
            }
            if let Some(tx) = &*self.tx.lock().unwrap() {
                let _ = tx.send(s);
            }
        }
    }

    // Code entry, with the address of the page that shows the code.
    fn ask_code(&self, message: Option<String>) {
        let url = self.url.lock().unwrap().clone();
        self.step(Step::Code { url, message });
    }

    // The last step, once: the children are stopped and the sign-in lock released before it is sent.
    fn end(&self, s: Step) {
        if self.finish() {
            if let Some(tx) = self.tx.lock().unwrap().take() {
                let _ = tx.send(s);
            }
        }
    }

    // Ends it with no step: the start failed, and its Err says why.
    fn abort(&self) {
        if self.finish() {
            self.tx.lock().unwrap().take();
        }
    }

    fn finish(&self) -> bool {
        {
            let mut e = self.ended.lock().unwrap();
            if *e {
                return false;
            }
            *e = true;
        }
        *self.input.lock().unwrap() = Input::None;
        let procs: Vec<Arc<Proc>> = self.procs.lock().unwrap().clone();
        for p in procs {
            p.stop();
        }
        let mut a = ACTIVE.lock().unwrap();
        if a.as_ref().is_some_and(|x| x.0 == self.id) {
            *a = None;
        }
        true
    }

    fn add(&self, child: Child) -> Arc<Proc> {
        let p = Proc::new(child);
        self.procs.lock().unwrap().push(p.clone());
        p
    }
}

// ── processes ────────────────────────────────────────────────────────────────────────────────

/// A child in a process group of its own (its pgid is its pid).
pub(crate) struct Proc {
    child: Mutex<Option<Child>>,  // None once reaped
    pub(crate) pid: u32,
}

/// Spawns `cmd` as the leader of a new process group.
pub(crate) fn spawn(cmd: &mut Command) -> std::io::Result<Child> {
    cmd.process_group(0).spawn()
}

impl Proc {
    pub(crate) fn new(child: Child) -> Arc<Proc> {
        Arc::new(Proc { pid: child.id(), child: Mutex::new(Some(child)) })
    }

    /// Waits for the child to end on its own; None when `stop` ended it.
    pub(crate) fn wait(&self) -> Option<ExitStatus> {
        loop {
            {
                let mut g = self.child.lock().unwrap();
                match g.as_mut() {
                    None => return None,
                    Some(c) => {
                        if let Ok(Some(s)) = c.try_wait() {
                            *g = None;
                            return Some(s);
                        }
                    }
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// TERM to the group, KILL a second later, then reap. The leader is reaped last, so its pid
    /// (the group's id) cannot have passed to another process while the group is signalled.
    /// False at once, the child left running for `wait`, when TERM reaches none of the group: its
    /// processes run as another user (pacman as root under pkexec).
    pub(crate) fn stop(&self) -> bool {
        let mut g = self.child.lock().unwrap();
        let Some(c) = g.as_mut() else { return true };
        if let Ok(None) = c.try_wait() {
            if !signal(self.pid, "TERM") {
                return false;
            }
            let until = Instant::now() + Duration::from_secs(1);
            while Instant::now() < until && !zombie(self.pid) {
                thread::sleep(Duration::from_millis(20));
            }
            signal(self.pid, "KILL");
        }
        if let Some(mut c) = g.take() {
            let _ = c.wait();
        }
        true
    }
}

// Whether the signal reached a process of the group.
fn signal(pgid: u32, sig: &str) -> bool {
    Command::new("kill").args(["-s", sig, "--", &format!("-{pgid}")])
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).status().is_ok_and(|s| s.success())
}

// Ended but not yet reaped.
fn zombie(pid: u32) -> bool {
    fs::read_to_string(format!("/proc/{pid}/stat")).ok()
        .and_then(|s| s.rfind(')').map(|i| s[i + 1..].trim_start().starts_with('Z')))
        .unwrap_or(true)
}

/// `program` with the account's env: key and account variables removed, then `home` as the
/// agent's folder variable, then `env`.
pub(crate) fn command(agent: &str, home: Option<&Path>, env: &[(String, String)], program: &str) -> Command {
    let mut cmd = Command::new(program);
    scrub(&mut cmd, env::vars_os().map(|(k, _)| k.to_string_lossy().into_owned()));
    let var = match agent {
        "claude" => Some("CLAUDE_CONFIG_DIR"),
        "codex" => Some("CODEX_HOME"),
        "pi" => Some("PI_CODING_AGENT_DIR"),
        _ => None,
    };
    if let (Some(var), Some(home)) = (var, home) {
        cmd.env(var, home);
    }
    cmd.envs(desktop());
    cmd.envs(env.iter().map(|(k, v)| (k, v)));
    cmd
}

/// The desktop's display, as the user manager has it now. The host starts with the login, before the compositor, so
/// its own env has none: xdg-open found no display and a sign-in's browser never opened (Discord, 2026-10-20). The
/// session puts these in the user manager (Hyprland's execs); read at each start, so a new session's display counts.
fn desktop() -> Vec<(String, String)> {
    const KEYS: [&str; 5] = ["WAYLAND_DISPLAY", "DISPLAY", "XDG_SESSION_TYPE", "XDG_CURRENT_DESKTOP", "HYPRLAND_INSTANCE_SIGNATURE"];
    let Ok(out) = Command::new("systemctl").args(["--user", "show-environment"]).stdin(Stdio::null()).stderr(Stdio::null()).output() else {
        return Vec::new();
    };
    let mut env: Vec<(String, String)> = String::from_utf8_lossy(&out.stdout).lines()
        .filter_map(|l| l.split_once('='))
        .filter(|(k, v)| KEYS.contains(k) && !v.is_empty())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    // a browser's engine picks Wayland by the session type; the user manager can have the display without it
    if env.iter().any(|(k, _)| k == "WAYLAND_DISPLAY") && !env.iter().any(|(k, _)| k == "XDG_SESSION_TYPE") {
        env.push(("XDG_SESSION_TYPE".into(), "wayland".into()));
    }
    env
}

fn scrub(cmd: &mut Command, names: impl Iterator<Item = String>) {
    for k in names {
        if k.ends_with("_API_KEY") || SCRUB.contains(&k.as_str()) {
            cmd.env_remove(k);
        }
    }
}

fn spec_command(spec: &LoginSpec, program: &str) -> Command {
    command(&spec.agent, spec.home.as_deref(), &spec.env, program)
}

/// Reads `r` to its end on a thread of its own, a line at a time (no line ending, lossy UTF-8).
/// The receiver disconnects at the end.
pub(crate) fn read_lines<R: Read + Send + 'static>(r: R, mut f: impl FnMut(String) + Send + 'static) -> Receiver<()> {
    let (tx, rx) = channel::<()>();
    thread::spawn(move || {
        let _tx = tx;
        let mut r = BufReader::new(r);
        let mut buf = Vec::new();
        while r.read_until(b'\n', &mut buf).is_ok_and(|n| n > 0) {
            let line = String::from_utf8_lossy(&buf).trim_end_matches(['\n', '\r']).to_string();
            f(line);
            buf.clear();
        }
    });
    rx
}

// Output that a grandchild holding the pipe open would keep from ending is not waited for.
pub(crate) fn drained(rx: &Receiver<()>) {
    let _ = rx.recv_timeout(Duration::from_secs(2));
}

/// Its stdout, whatever its exit, when it ends within `wait`; at `wait` its group is killed.
pub(crate) fn output(cmd: &mut Command, wait: Duration) -> Result<(ExitStatus, String), String> {
    let name = cmd.get_program().to_string_lossy().into_owned();
    let mut child = spawn(cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()))
        .map_err(|e| format!("{name}: {e}"))?;
    let mut out = child.stdout.take().ok_or_else(|| format!("{name}: no stdout"))?;
    let p = Proc::new(child);
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut s = Vec::new();
        let _ = out.read_to_end(&mut s);
        let _ = tx.send(String::from_utf8_lossy(&s).into_owned());
    });
    let (w, p2) = (channel(), p.clone());
    thread::spawn(move || {
        let _ = w.0.send(p2.wait());
    });
    match w.1.recv_timeout(wait) {
        Ok(Some(st)) => Ok((st, rx.recv_timeout(Duration::from_secs(2)).unwrap_or_default())),
        _ => {
            p.stop();
            Err(format!("{name} did not finish within {} s", wait.as_secs()))
        }
    }
}

/// Opens `url` with xdg-open as a unit of its own (systemd-run): the user manager's environment, which holds the
/// desktop's and none of an account's keys, and outside this host and the sign-in's process group. As a child here the
/// browser it started went with a Cancel, a host restart or the host's memory cap, and a browser not yet running
/// started without the session type and its engine failed (Discord, 2026-10-20: sign-ins opened nothing).
/// A test runs the recording xdg-open its sign-in's env puts first on PATH, never the desktop's browser (2026-10-21: one
/// run opened its made-up sign-ins on the owner's screen).
fn open(spec: &LoginSpec, url: &str) -> bool {
    if cfg!(test) {
        let mut cmd = command("", None, &spec.env, "xdg-open");
        return matches!(cmd.arg(url).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).status(), Ok(s) if s.success());
    }
    let mut cmd = Command::new("systemd-run");
    cmd.args(["--user", "--quiet", "--collect"]);
    for (k, v) in desktop() {
        cmd.arg(format!("--setenv={k}={v}"));
    }
    cmd.args(["--", "xdg-open", url]);
    matches!(cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).status(), Ok(s) if s.success())
}

// ── port 1455 ────────────────────────────────────────────────────────────────────────────────

fn port_busy(port: u16) -> bool {
    !listeners(port).is_empty() || TcpListener::bind(("127.0.0.1", port)).is_err()
}

// Socket inodes listening on `port`, on any address.
fn listeners(port: u16) -> Vec<String> {
    let want = format!(":{port:04X}");
    let mut found = Vec::new();
    for f in ["/proc/net/tcp", "/proc/net/tcp6"] {
        for l in fs::read_to_string(f).unwrap_or_default().lines().skip(1) {
            let c: Vec<&str> = l.split_whitespace().collect();
            if c.len() > 9 && c[1].ends_with(&want) && c[3] == "0A" {
                found.push(c[9].to_string());
            }
        }
    }
    found
}

// The agent whose process listens on `port` ("" another program, or one this user cannot see).
fn port_holder(port: u16) -> String {
    let socks: Vec<String> = listeners(port).iter().map(|i| format!("socket:[{i}]")).collect();
    for p in fs::read_dir("/proc").into_iter().flatten().flatten() {
        if !p.file_name().to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let holds = fs::read_dir(p.path().join("fd")).into_iter().flatten().flatten()
            .any(|fd| fs::read_link(fd.path()).is_ok_and(|l| socks.iter().any(|s| l.as_os_str() == s.as_str())));
        if holds {
            return agent_of(&fs::read(p.path().join("cmdline")).unwrap_or_default()).into();
        }
    }
    String::new()
}

fn agent_of(cmdline: &[u8]) -> &'static str {
    let args: Vec<String> = cmdline.split(|&b| b == 0).map(|a| String::from_utf8_lossy(a).into_owned()).collect();
    let all = args.join(" ");
    let base = |a: &String| Path::new(a).file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_default();
    if all.contains("pi-login.mjs") || all.contains("pi-coding-agent") || args.iter().any(|a| base(a) == "pi") {
        "pi"
    } else if all.contains("opencode") {
        "opencode"
    } else if all.contains("codex") {
        "codex"
    } else if all.contains("claude") {
        "claude"
    } else {
        ""
    }
}

// ── Claude Code ──────────────────────────────────────────────────────────────────────────────

// The helper, written to `share` when it differs from this build's (as pi-login.mjs is).
fn browser_helper(share: &Path) -> Result<PathBuf, String> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(share).map_err(|e| format!("{}: {e}", share.display()))?;
    let path = share.join("browser.sh");
    if fs::read_to_string(&path).ok().as_deref() != Some(BROWSER_HELPER) {
        let tmp = share.join(format!(".browser.sh.{}", std::process::id()));
        fs::write(&tmp, BROWSER_HELPER).and_then(|_| fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)))
            .and_then(|_| fs::rename(&tmp, &path)).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(path)
}

// The address the CLI gave the helper, when it comes within a moment: the CLI prints its manual page
// first and asks the browser for the automatic one just after. None: the printed one is all there is.
fn browser_url(file: &Path) -> Option<String> {
    let until = Instant::now() + Duration::from_secs(1);
    loop {
        if let Ok(s) = fs::read_to_string(file) {
            let url = s.lines().next().unwrap_or("").trim().to_string();
            if url.starts_with("http://") || url.starts_with("https://") {
                let _ = fs::remove_file(file);
                return Some(url);
            }
        }
        if Instant::now() >= until {
            return None;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn claude_login(run: &Arc<Run>, spec: &LoginSpec, share: &Path) -> Result<(), String> {
    let flag = if spec.method == "console" { "--console" } else { "--claudeai" };
    let mut cmd = spec_command(spec, "claude");
    // the host opens the page (open), as for the other agents: the CLI's own xdg-open ran as its child, where the
    // browser it started did not come up (and went with the sign-in's process group). BROWSER names a helper that
    // leaves the CLI's automatic address (the one that returns to its localhost listener) for the host to open;
    // the manual page the CLI prints is only the fallback, for a pasted code.
    let helper = browser_helper(share)?;
    let capture = share.join(format!("claude-browser-{}.url", run.id));
    let _ = fs::remove_file(&capture);
    cmd.env("BROWSER", &helper).env("AGENT_HOST_BROWSER_URL_FILE", &capture);
    cmd.args(["auth", "login", flag]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = spawn(&mut cmd).map_err(|e| format!("start claude: {e}"))?;
    let (out, err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    // stdin stays open for a pasted code while "Paste code here if prompted >" waits
    *run.input.lock().unwrap() = Input::Stdin(child.stdin.take().unwrap());
    let proc = run.add(child);
    let ok = Arc::new(Mutex::new(false));
    let (r, ok2, spec_open) = (run.clone(), ok.clone(), spec.clone());
    let capture_read = capture.clone();
    let mut url_next = false;
    let orx = read_lines(out, move |line| {
        let text = plain(&line);
        if url_next || text.contains("If the browser didn't open, visit:") {
            let after = if url_next { line.as_str() } else { line.split_once("visit:").map_or("", |x| x.1) };
            match url_in(after) {
                Some(manual) => {
                    url_next = false;
                    let url = browser_url(&capture_read).unwrap_or_else(|| manual.clone());
                    let opened = open(&spec_open, &url);
                    r.step(Step::Waiting { url, opened });
                    // the automatic address is opened, but its page goes back to the CLI's localhost listener and
                    // shows no code: a code to paste comes from the printed page, which Code steps point at
                    *r.url.lock().unwrap() = manual;
                }
                None => url_next = !url_next,
            }
        } else if text.contains("Login successful") {
            *ok2.lock().unwrap() = true;
        }
    });
    let errs = Arc::new(Mutex::new(Vec::<String>::new()));
    let (r, e2) = (run.clone(), errs.clone());
    let erx = read_lines(err, move |l| {
        let l = plain(&l);
        if l.starts_with("Invalid code") {
            r.ask_code(Some(l.trim().to_string()));
        } else if !l.trim().is_empty() {
            e2.lock().unwrap().push(l.trim().to_string());
        }
    });
    // The CLI saves the sign-in to .credentials.json, but on the automatic route it may not print "Login successful"
    // or exit (Abdul, 2026-10-27: saved at 19:54:33, never reported). So the saved sign-in is watched for as well.
    let before = creds_stamp(spec.home.as_deref());
    let settle: Arc<dyn Fn() -> Option<Step> + Send + Sync> = {
        let (home, env) = (spec.home.clone(), spec.env.clone());
        Arc::new(move || {
            if creds_stamp(home.as_deref()) == before {
                return None;
            }
            let w = claude_status(home.as_deref(), &env).ok().filter(|w| w.state == "ready")?;
            Some(Step::Done { email: w.email, plan: w.plan })
        })
    };
    let s2 = settle.clone();
    *run.settled.lock().unwrap() = Some(Box::new(move || s2()));
    let (r, s3, home) = (run.clone(), settle.clone(), spec.home.clone());
    thread::spawn(move || {
        let mut seen = before;
        while !*r.ended.lock().unwrap() {
            thread::sleep(Duration::from_millis(200));
            let now = creds_stamp(home.as_deref());
            if now == seen {
                continue;
            }
            seen = now;
            // let the CLI finish by itself first
            let until = Instant::now() + Duration::from_secs(3);
            while Instant::now() < until && !*r.ended.lock().unwrap() {
                thread::sleep(Duration::from_millis(100));
            }
            if *r.ended.lock().unwrap() {
                return;
            }
            if let Some(done) = s3() {
                r.end(done);
            }
        }
    });
    let (spec_home, spec_env) = (spec.home.clone(), spec.env.clone());
    let (r, capture_end) = (run.clone(), capture);
    thread::spawn(move || {
        let st = proc.wait();
        drained(&orx);
        drained(&erx);
        let _ = fs::remove_file(&capture_end);
        match st {
            None => {}
            Some(s) if s.success() && (*ok.lock().unwrap() || settle().is_some()) => {
                let w = claude_status(spec_home.as_deref(), &spec_env).ok();
                r.end(Step::Done { email: w.as_ref().and_then(|w| w.email.clone()), plan: w.and_then(|w| w.plan) });
            }
            Some(s) => {
                let errs = errs.lock().unwrap();
                let why = errs.iter().rev().find_map(|l| l.strip_prefix("Login failed:").or(l.strip_prefix("OAuth login failed:")))
                    .or(errs.last().map(String::as_str)).map(|m| redact(m.trim()))
                    .unwrap_or_else(|| format!("claude auth login exited with {s}"));
                r.end(Step::Failed(why));
            }
        }
    });
    Ok(())
}

// The saved sign-in's size and time; changes when the CLI saves a new one.
fn creds_stamp(home: Option<&Path>) -> Option<(std::time::SystemTime, u64)> {
    let dir = home.map(Path::to_path_buf).unwrap_or_else(|| crate::home().join(".claude"));
    fs::metadata(dir.join(".credentials.json")).ok().and_then(|m| Some((m.modified().ok()?, m.len())))
}

fn claude_status(home: Option<&Path>, env: &[(String, String)]) -> Result<Status, String> {
    let mut cmd = command("claude", home, env, "claude");
    let (_, out) = output(cmd.args(["auth", "status", "--json"]), WAIT)?;
    let v: Value = serde_json::from_str(out.trim()).map_err(|_| "claude auth status printed no JSON".to_string())?;
    if v["loggedIn"] != true {
        return Ok(Status { state: "signedOut", email: None, plan: None, providers: Vec::new() });
    }
    let plan = v["subscriptionType"].as_str().or(v["authMethod"].as_str()).map(str::to_string);
    Ok(Status { state: "ready", email: v["email"].as_str().map(str::to_string), plan, providers: Vec::new() })
}

// ── Codex ────────────────────────────────────────────────────────────────────────────────────

// A short-lived `codex app-server`: JSON-RPC lines on stdio.
struct AppServer {
    stdin: Mutex<Option<ChildStdin>>,
    waiting: Arc<Mutex<HashMap<u64, Sender<Result<Value, String>>>>>,
    next: AtomicU64,
    notes: Mutex<Receiver<Value>>,
}

impl AppServer {
    fn start(cmd: &mut Command) -> Result<(AppServer, Child), String> {
        cmd.args(["app-server", "-c", "notify=[]"]).current_dir(crate::home())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null());
        let mut child = spawn(cmd).map_err(|e| format!("start codex: {e}"))?;
        let (stdin, out) = (child.stdin.take().unwrap(), child.stdout.take().unwrap());
        let waiting: Arc<Mutex<HashMap<u64, Sender<Result<Value, String>>>>> = Arc::default();
        let (ntx, nrx) = channel();
        let w = waiting.clone();
        read_lines(out, move |l| {
            let Ok(v) = serde_json::from_str::<Value>(&l) else { return };
            match (v["id"].as_u64(), v.get("method")) {
                (Some(id), None) => {
                    if let Some(tx) = w.lock().unwrap().remove(&id) {
                        let _ = tx.send(match v.get("error") {
                            Some(e) => Err(e["message"].as_str().unwrap_or("codex refused").to_string()),
                            None => Ok(v["result"].clone()),
                        });
                    }
                }
                (None, Some(_)) => {
                    let _ = ntx.send(v);
                }
                _ => {}
            }
        });
        let srv = AppServer { stdin: Mutex::new(Some(stdin)), waiting, next: AtomicU64::new(1), notes: Mutex::new(nrx) };
        Ok((srv, child))
    }

    fn call(&self, method: &str, params: Value, wait: Duration) -> Result<Value, String> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        self.waiting.lock().unwrap().insert(id, tx);
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))?;
        rx.recv_timeout(wait).map_err(|_| format!("codex app-server did not answer {method}"))?
    }

    fn send(&self, v: Value) -> Result<(), String> {
        let mut g = self.stdin.lock().unwrap();
        let w = g.as_mut().ok_or("codex app-server has stopped")?;
        writeln!(w, "{v}").map_err(|_| "codex app-server has stopped".to_string())
    }

    fn hello(&self) -> Result<(), String> {
        self.call("initialize", json!({"clientInfo": {"name": "agent-host", "version": env!("CARGO_PKG_VERSION")}}), WAIT)?;
        self.send(json!({"jsonrpc": "2.0", "method": "initialized"}))
    }

    // A short-lived one for status or sign-out: started, asked, stopped.
    fn once<T>(home: Option<&Path>, env: &[(String, String)], f: impl FnOnce(&AppServer) -> Result<T, String>) -> Result<T, String> {
        let (srv, child) = AppServer::start(&mut command("codex", home, env, "codex"))?;
        let proc = Proc::new(child);
        let r = srv.hello().and_then(|_| f(&srv));
        proc.stop();
        r
    }
}

fn codex_login(run: &Arc<Run>, spec: &LoginSpec) -> Result<(), String> {
    let (srv, child) = AppServer::start(&mut spec_command(spec, "codex"))?;
    run.add(child);
    let (r, method, spec) = (run.clone(), spec.method.clone(), spec.clone());
    thread::spawn(move || {
        let go = || -> Result<Step, String> {
            srv.hello()?;
            let device = method == "device";
            let kind = if device { "chatgptDeviceCode" } else { "chatgpt" };
            let s = srv.call("account/login/start", json!({"type": kind}), WAIT)?;
            let login_id = s["loginId"].clone();
            if device {
                let (url, code) = (s["verificationUrl"].as_str(), s["userCode"].as_str());
                let (Some(url), Some(code)) = (url, code) else { return Err("codex sent no device code".into()) };
                r.step(Step::Device { url: url.into(), user_code: code.into() });
            } else {
                let url = s["authUrl"].as_str().ok_or("codex sent no sign-in address")?;
                let opened = open(&spec, url);
                r.step(Step::Waiting { url: url.into(), opened });
            }
            let notes = srv.notes.lock().unwrap();
            let done = loop {
                let n = notes.recv().map_err(|_| "codex app-server stopped".to_string())?;
                if n["method"] == "account/login/completed" && (n["params"]["loginId"].is_null() || n["params"]["loginId"] == login_id) {
                    break n["params"].clone();
                }
            };
            if done["success"] != true {
                return Err(done["error"].as_str().map(redact).unwrap_or_else(|| "Sign-in didn't finish".into()));
            }
            let who = codex_account(&srv).ok();
            Ok(Step::Done { email: who.as_ref().and_then(|w| w.email.clone()), plan: who.and_then(|w| w.plan) })
        };
        let step = go().unwrap_or_else(Step::Failed);
        r.end(step);
    });
    Ok(())
}

fn codex_account(srv: &AppServer) -> Result<Status, String> {
    let v = srv.call("account/read", json!({"refreshToken": false}), WAIT)?;
    let a = &v["account"];
    Ok(match a["type"].as_str() {
        Some(t) => Status { state: "ready", email: a["email"].as_str().map(str::to_string),
                            plan: a["planType"].as_str().or(Some(t)).map(str::to_string), providers: Vec::new() },
        // another model provider, set in config.toml, needs no OpenAI sign-in
        None if v["requiresOpenaiAuth"] == false => Status { state: "ready", email: None, plan: None, providers: Vec::new() },
        None => Status { state: "signedOut", email: None, plan: None, providers: Vec::new() },
    })
}

// ── pi ───────────────────────────────────────────────────────────────────────────────────────

// pi's package folder, from the `pi` command on the PATH the child gets.
fn pi_package(env: &[(String, String)]) -> Result<PathBuf, String> {
    let path = env.iter().find(|(k, _)| k == "PATH").map(|(_, v)| v.clone()).or_else(|| env::var("PATH").ok()).unwrap_or_default();
    let bin = env::split_paths(&path).map(|d| d.join("pi")).find(|p| p.is_file()).ok_or("pi is not installed")?;
    let real = fs::canonicalize(&bin).map_err(|e| e.to_string())?;
    real.ancestors().find(|d| {
        fs::read(d.join("package.json")).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|v| v["name"] == "@earendil-works/pi-coding-agent")
    }).map(Path::to_path_buf).ok_or_else(|| "pi's package was not found next to its command".into())
}

// The helper, written to `share` when it differs from this build's.
fn helper_file(share: &Path) -> Result<PathBuf, String> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(share).map_err(|e| format!("{}: {e}", share.display()))?;
    let path = share.join("pi-login.mjs");
    if fs::read_to_string(&path).ok().as_deref() != Some(HELPER) {
        let tmp = share.join(format!(".pi-login.mjs.{}", std::process::id()));
        fs::write(&tmp, HELPER).and_then(|_| fs::rename(&tmp, &path)).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(path)
}

fn pi_helper(spec: &LoginSpec, share: &Path, args: &[&str]) -> Result<Command, String> {
    let pkg = pi_package(&spec.env)?;
    let helper = helper_file(share)?;
    let mut cmd = spec_command(spec, "node");
    cmd.arg(helper).args(args).arg(pkg);
    Ok(cmd)
}

fn pi_login(run: &Arc<Run>, spec: &LoginSpec, share: &Path) -> Result<(), String> {
    let provider = spec.provider.clone().unwrap_or_default();
    let mut cmd = pi_helper(spec, share, &["login", &provider, &spec.method])?;
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = spawn(&mut cmd).map_err(|e| format!("start node: {e}"))?;
    let (out, err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    *run.input.lock().unwrap() = Input::Stdin(child.stdin.take().unwrap());
    let proc = run.add(child);
    let said = Arc::new(Mutex::new(None::<String>));
    let (r, said2, spec2) = (run.clone(), said.clone(), spec.clone());
    let orx = read_lines(out, move |l| {
        let Ok(v) = serde_json::from_str::<Value>(&l) else { return };
        match v["type"].as_str() {
            Some("auth_url") => {
                if let Some(url) = v["url"].as_str() {
                    let opened = open(&spec2, url);
                    r.step(Step::Waiting { url: url.into(), opened });
                }
            }
            Some("device_code") => {
                if let (Some(url), Some(code)) = (v["verificationUri"].as_str(), v["userCode"].as_str()) {
                    r.step(Step::Device { url: url.into(), user_code: code.into() });
                }
            }
            Some("error") => *said2.lock().unwrap() = v["message"].as_str().map(redact),
            _ => {}
        }
    });
    let errs = Arc::new(Mutex::new(Vec::<String>::new()));
    let e2 = errs.clone();
    let erx = read_lines(err, move |l| {
        if !l.trim().is_empty() {
            e2.lock().unwrap().push(redact(l.trim()));
        }
    });
    let r = run.clone();
    thread::spawn(move || {
        let st = proc.wait();
        drained(&orx);
        drained(&erx);
        match st {
            None => {}
            Some(s) if s.success() => r.end(Step::Done { email: None, plan: None }),
            Some(s) => {
                let why = said.lock().unwrap().clone().or_else(|| errs.lock().unwrap().last().cloned())
                    .unwrap_or_else(|| format!("pi's sign-in exited with {s}"));
                r.end(Step::Failed(why));
            }
        }
    });
    Ok(())
}

// The env variable a provider's key goes in for pi (plan §2.14).
fn pi_key_var(provider: &str) -> String {
    match provider {
        "google" => "GEMINI_API_KEY".into(),
        p => format!("{}_API_KEY", p.to_ascii_uppercase().replace('-', "_")),
    }
}

fn pi_status(home: Option<&Path>, env: &[(String, String)]) -> Result<Status, String> {
    let (_, out) = output(command("pi", home, env, "pi").args(["--offline", "--list-models"]), WAIT)?;
    let mut lines = out.lines();
    let mut ids: Vec<String> = Vec::new();
    if lines.next().is_some_and(|h| h.split_whitespace().next() == Some("provider")) {
        for l in lines {
            if let Some(p) = l.split_whitespace().next() {
                if !ids.iter().any(|i| i == p) {
                    ids.push(p.to_string());
                }
            }
        }
    }
    let mut providers = Vec::new();
    for id in ids {
        let mut cmd = command("pi", home, env, "pi");
        // --no-refresh: reads the stored sign-in as it is, never renews it
        let (_, out) = output(cmd.args(["auth", "check", "--provider", &id, "--json", "--no-refresh"]), WAIT)?;
        let v: Value = serde_json::from_str(out.trim()).unwrap_or(Value::Null);
        let via = match v["authType"].as_str() {
            Some("oauth") => "oauth",
            _ if env.iter().any(|(k, _)| *k == pi_key_var(&id)) => "key",
            _ => "file",
        };
        providers.push(Provider { id, via, state: if v["status"] == "ready" { "ready" } else { "signedOut" } });
    }
    let state = if providers.iter().any(|p| p.state == "ready") { "ready" } else if providers.is_empty() { "noProvider" } else { "signedOut" };
    Ok(Status { state, email: None, plan: None, providers })
}

// ── opencode ─────────────────────────────────────────────────────────────────────────────────

// One request on a connection of its own; the reply is read to its length or last chunk.
// `wait`: None waits as long as the server takes (a sign-in callback waits for the browser).
fn http(port: u16, auth: &str, method: &str, path: &str, body: Option<&Value>, wait: Option<Duration>) -> Result<Value, String> {
    let err = |e: std::io::Error| format!("opencode: {e}");
    let mut s = TcpStream::connect(("127.0.0.1", port)).map_err(err)?;
    let _ = s.set_read_timeout(wait);
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
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if !(200..300).contains(&status) {
        let msg = v["data"]["message"].as_str().or(v["message"].as_str()).or(v["error"].as_str()).map(redact)
            .unwrap_or_else(|| format!("opencode answered {status}"));
        return Err(msg);
    }
    Ok(v)
}

// The provider's sign-in method: its index among the provider's methods (opencode's own numbering).
// "device" takes the headless one.
fn opencode_method(methods: &Value, device: bool) -> Option<usize> {
    let list = methods.as_array()?;
    let headless = |m: &Value| m["label"].as_str().is_some_and(|l| { let l = l.to_ascii_lowercase(); l.contains("headless") || l.contains("device") });
    let oauth: Vec<usize> = (0..list.len()).filter(|&i| list[i]["type"] == "oauth").collect();
    oauth.iter().copied().find(|&i| headless(&list[i]) == device).or_else(|| oauth.first().copied())
}

// "Enter code: ABCD-1234", "… and enter code: ABCD-1234"
fn code_in(instructions: &str) -> Option<String> {
    let i = instructions.to_ascii_lowercase().rfind("code:")?;
    let code = instructions[i + 5..].split_whitespace().next()?;
    Some(code.trim_end_matches(['.', ',']).to_string()).filter(|c| !c.is_empty())
}

fn opencode_login(run: &Arc<Run>, spec: &LoginSpec) -> Result<(), String> {
    let provider = spec.provider.clone().unwrap_or_default();
    let port = TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map_err(|e| e.to_string())?.port();
    let mut secret = [0u8; 18];
    fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut secret)).map_err(|e| e.to_string())?;
    use base64::Engine;
    let password = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}")));
    let mut cmd = spec_command(spec, "opencode");
    cmd.args(["serve", "--hostname", "127.0.0.1", "--port", &port.to_string()])
        .env("OPENCODE_SERVER_PASSWORD", &password).current_dir(crate::home())
        .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = spawn(&mut cmd).map_err(|e| format!("start opencode: {e}"))?;
    let out = child.stdout.take().unwrap();
    run.add(child);
    let (ready_tx, ready) = channel::<()>();
    read_lines(out, move |l| {
        if l.contains("listening on") {
            let _ = ready_tx.send(());
        }
    });
    let (code_tx, codes) = channel::<String>();
    let (r, device, spec) = (run.clone(), spec.method == "device", spec.clone());
    thread::spawn(move || {
        let go = || -> Result<Step, String> {
            // here, not in start: accounts.rs holds its lock across that, and a --version not read before (the first
            // sign-in, or one after an update) runs the program, up to 15 s
            let v2 = crate::opencode_v2::major(&spec_command(&spec, "opencode")) >= 2;
            ready.recv_timeout(Duration::from_secs(30)).map_err(|_| "opencode serve did not start".to_string())?;
            if v2 {
                return opencode2_login(&r, &spec, port, &auth, &provider, device, code_tx, codes);
            }
            let all = http(port, &auth, "GET", "/provider/auth", None, Some(WAIT))?;
            let idx = opencode_method(&all[&provider], device).ok_or_else(|| format!("opencode has no sign-in for {provider}"))?;
            let base = format!("/provider/{provider}/oauth");
            let a = http(port, &auth, "POST", &format!("{base}/authorize"), Some(&json!({"method": idx})), Some(Duration::from_secs(60)))?;
            let url = a["url"].as_str().unwrap_or("").to_string();
            let by_code = a["method"] == "code";
            match a["instructions"].as_str().and_then(code_in) {
                Some(code) if !by_code => r.step(Step::Device { url, user_code: code }),
                _ => {
                    let opened = !url.is_empty() && open(&spec, &url);
                    r.step(Step::Waiting { url, opened });
                }
            }
            if !by_code {
                return match http(port, &auth, "POST", &format!("{base}/callback"), Some(&json!({"method": idx})), None)? {
                    Value::Bool(true) => Ok(Step::Done { email: None, plan: None }),
                    _ => Err("opencode did not accept the sign-in".into()),
                };
            }
            {
                let ended = r.ended.lock().unwrap();
                if *ended {
                    return Err("This sign-in has ended".into());
                }
                *r.input.lock().unwrap() = Input::Codes(code_tx);
            }
            r.ask_code(None);
            // A refused code (false, or opencode's 400) is asked for again, as Claude's is: opencode keeps the sign-in
            // it is waiting on until one goes through. A lost connection to opencode (http's "opencode: …") ends it.
            loop {
                let code = codes.recv().map_err(|_| "This sign-in has ended".to_string())?;
                match http(port, &auth, "POST", &format!("{base}/callback"), Some(&json!({"method": idx, "code": code})), None) {
                    Ok(Value::Bool(true)) => break,
                    Err(e) if e.starts_with("opencode: ") => return Err(e),
                    _ => r.ask_code(Some("opencode did not accept the code".into())),
                }
            }
            *r.input.lock().unwrap() = Input::None;
            Ok(Step::Done { email: None, plan: None })
        };
        let step = go().unwrap_or_else(Step::Failed);
        r.end(step);
    });
    Ok(())
}

// opencode 2's sign-in method among the integration's: its id, and answers to its form from the defaults (the
// first option where there is none). "device" takes a device or headless one; the browser one is not "legacy".
fn opencode2_method(methods: &Value, device: bool) -> Option<(String, Value)> {
    let oauth: Vec<&Value> = methods.as_array()?.iter().filter(|m| m["type"] == "oauth" && m["id"].is_string()).collect();
    let words = |m: &Value| format!("{} {}", m["id"].as_str().unwrap_or(""), m["label"].as_str().unwrap_or("")).to_ascii_lowercase();
    let headless = |m: &Value| { let w = words(m); w.contains("device") || w.contains("headless") };
    let m = oauth.iter().find(|m| headless(m) == device && !words(m).contains("legacy"))
        .or_else(|| oauth.iter().find(|m| headless(m) == device)).or_else(|| oauth.first())?;
    let mut answer = serde_json::Map::new();
    for f in m["form"].as_array().into_iter().flatten().filter(|f| f["when"].is_null()) {
        let Some(k) = f["key"].as_str() else { continue };
        let v = if f["default"].is_null() { f["options"][0]["value"].clone() } else { f["default"].clone() };
        if !v.is_null() {
            answer.insert(k.into(), v);
        }
    }
    Some((m["id"].as_str()?.to_string(), Value::Object(answer)))
}

#[allow(clippy::too_many_arguments)]
fn opencode2_login(r: &Arc<Run>, spec: &LoginSpec, port: u16, auth: &str, provider: &str, device: bool,
                   code_tx: Sender<String>, codes: Receiver<String>) -> Result<Step, String> {
    let all = http(port, auth, "GET", "/api/integration", None, Some(WAIT))?;
    let integ = all["data"].as_array().and_then(|l| l.iter().find(|i| i["id"] == provider || i["name"] == provider))
        .ok_or_else(|| format!("opencode has no sign-in for {provider}"))?;
    let id = integ["id"].as_str().unwrap_or(provider).to_string();
    let (method, answer) = opencode2_method(&integ["methods"], device).ok_or_else(|| format!("opencode has no sign-in for {provider}"))?;
    let mut body = json!({"methodID": method});
    if answer.as_object().is_some_and(|a| !a.is_empty()) {
        body["answer"] = answer;
    }
    let base = format!("/api/integration/{id}/connect/oauth");
    // an attempt, its page opened (or its device code shown): its id, and whether a pasted code finishes it
    let connect = || -> Result<(String, bool), String> {
        let a = http(port, auth, "POST", &base, Some(&body), Some(Duration::from_secs(60)))?["data"].take();
        let attempt = a["attemptID"].as_str().ok_or("opencode started no sign-in")?.to_string();
        let url = a["url"].as_str().unwrap_or("").to_string();
        let by_code = a["mode"] == "code";
        match a["instructions"].as_str().and_then(code_in) {
            Some(code) if !by_code => r.step(Step::Device { url, user_code: code }),
            _ => {
                let opened = !url.is_empty() && open(spec, &url);
                r.step(Step::Waiting { url, opened });
            }
        }
        Ok((attempt, by_code))
    };
    let (mut attempt, by_code) = connect()?;
    if by_code {
        {
            let ended = r.ended.lock().unwrap();
            if *ended {
                return Err("This sign-in has ended".into());
            }
            *r.input.lock().unwrap() = Input::Codes(code_tx);
        }
        r.ask_code(None);
        // A refused code fails its attempt for good (2 marks it failed, and takes no other code for it), so a new attempt
        // starts, with its own page, and its code is asked for. A lost connection to opencode (http's "opencode: …") ends it.
        loop {
            let code = codes.recv().map_err(|_| "This sign-in has ended".to_string())?;
            match http(port, auth, "POST", &format!("{base}/{attempt}/complete"), Some(&json!({"code": code})), Some(Duration::from_secs(60))) {
                Ok(_) => break,
                Err(e) if e.starts_with("opencode: ") => return Err(e),
                Err(_) => {
                    attempt = connect()?.0;
                    r.ask_code(Some("That code didn't work. Here's a new page to sign in again.".into()));
                }
            }
        }
        *r.input.lock().unwrap() = Input::None;
    }
    // the browser or the device page finishes it; the server's end (a cancel, the timeout) ends this too
    loop {
        if *r.ended.lock().unwrap() {
            return Err("This sign-in has ended".into());
        }
        let st = http(port, auth, "GET", &format!("{base}/{attempt}"), None, Some(WAIT))?["data"].take();
        match st["status"].as_str().unwrap_or("") {
            "pending" => thread::sleep(Duration::from_secs(1)),
            "complete" => return Ok(Step::Done { email: None, plan: None }),
            "failed" => return Err(st["message"].as_str().map(redact).unwrap_or_else(|| "opencode did not accept the sign-in".into())),
            other => return Err(format!("The sign-in {}", if other.is_empty() { "ended" } else { other })),
        }
    }
}

// `opencode auth list` of opencode 2: "OpenRouter  OpenRouter      stored" per credential: the integration, the
// credential's label and where it is. A key keeps the integration's name as its label, a signed-in account its own.
// Each with its label, which a sign-out names.
fn opencode2_providers(out: &str) -> Vec<(Provider, String)> {
    let mut found = Vec::new();
    for l in out.lines().map(plain) {
        let cols: Vec<&str> = l.split("  ").map(str::trim).filter(|c| !c.is_empty()).collect();
        let [name, label, .., kind] = cols.as_slice() else { continue };
        if l.split_whitespace().any(key_shaped) {
            continue;
        }
        let via = match *kind {
            "env" => "key",
            "stored" if label == name => "file",
            "stored" | "oauth" => "oauth",
            _ => continue,
        };
        found.push((Provider { id: name.to_string(), via, state: "ready" }, label.to_string()));
    }
    found
}

// `opencode auth list`: "●  OpenRouter \e[90mapi" per saved credential, then under "Environment"
// "●  Anthropic \e[90mANTHROPIC_API_KEY" per key variable set. Names and kinds only; a line with
// anything key-shaped in it is dropped.
fn opencode_providers(out: &str) -> Vec<Provider> {
    let mut found = Vec::new();
    for l in out.lines().map(plain) {
        let Some(rest) = l.trim_start().strip_prefix('●') else { continue };
        let words: Vec<&str> = rest.split_whitespace().collect();
        let Some((kind, name)) = words.split_last() else { continue };
        if name.is_empty() || words.iter().any(|w| key_shaped(w)) {
            continue;
        }
        let via = match *kind {
            "api" => "file",
            "oauth" | "wellknown" => "oauth",
            k if k.ends_with("_KEY") || k.ends_with("_TOKEN") => "key",
            _ => continue,
        };
        found.push(Provider { id: name.join(" "), via, state: "ready" });
    }
    found
}

fn opencode_status(env: &[(String, String)]) -> Result<Status, String> {
    let mut cmd = command("opencode", None, env, "opencode");
    // 2's CLI otherwise starts a background service that stays
    let providers = if crate::opencode_v2::major(&cmd) >= 2 {
        let (_, out) = output(cmd.args(["auth", "list", "--standalone"]), WAIT)?;
        opencode2_providers(&out).into_iter().map(|(p, _)| p).collect()
    } else {
        let (_, out) = output(cmd.args(["auth", "list"]), WAIT)?;
        opencode_providers(&out)
    };
    Ok(Status { state: if providers.is_empty() { "noProvider" } else { "ready" }, email: None, plan: None, providers })
}

// ── status and sign-out ──────────────────────────────────────────────────────────────────────

pub struct Status {
    pub state: &'static str,  // "ready" | "signedOut" | "noProvider"
    pub email: Option<String>,
    pub plan: Option<String>,  // Claude Code's subscriptionType (else authMethod), Codex's planType (else the account type)
    pub providers: Vec<Provider>,  // pi and opencode
}

pub struct Provider {
    pub id: String,           // pi: the provider id; opencode: the name `auth list` prints
    pub via: &'static str,    // "oauth" | "key" (from the env given) | "file" (a key in the agent's own file)
    pub state: &'static str,  // "ready" | "signedOut"
}

impl Status {
    pub fn to_json(&self) -> Value {
        let providers: Vec<Value> = self.providers.iter().map(|p| json!({"id": p.id, "via": p.via, "state": p.state})).collect();
        json!({"state": self.state, "email": self.email, "plan": self.plan, "providers": providers})
    }
}

/// The account's sign-in as the agent reports it. Err: the agent did not answer, so its state is
/// not known.
pub fn status(agent: &str, home: Option<&Path>, env: &[(String, String)]) -> Result<Status, String> {
    match agent {
        "claude" => claude_status(home, env),
        "codex" => AppServer::once(home, env, codex_account),
        "pi" => pi_status(home, env),
        "opencode" => opencode_status(env),
        a => Err(format!("unknown agent {a}")),
    }
}

/// Signs the account out through the agent. pi and opencode sign out of one provider.
pub fn logout(agent: &str, home: Option<&Path>, provider: Option<&str>, env: &[(String, String)]) -> Result<(), String> {
    logout_in(agent, home, provider, env, &crate::home().join(".local/share/agent-host"))
}

fn logout_in(agent: &str, home: Option<&Path>, provider: Option<&str>, env: &[(String, String)], share: &Path) -> Result<(), String> {
    let need = || provider.filter(|p| !p.is_empty()).ok_or_else(|| format!("{} signs out of a provider; name one", name(agent)));
    match agent {
        "claude" => {
            let (st, _) = output(command(agent, home, env, "claude").args(["auth", "logout"]), WAIT)?;
            if st.success() { Ok(()) } else { Err(format!("claude auth logout exited with {st}")) }
        }
        "codex" => AppServer::once(home, env, |s| s.call("account/logout", json!({}), WAIT).map(|_| ())),
        "pi" => {
            let spec = LoginSpec { agent: agent.into(), account: String::new(), home: home.map(Path::to_path_buf),
                                   method: String::new(), provider: None, env: env.to_vec() };
            let (st, out) = output(&mut pi_helper(&spec, share, &["logout", need()?])?, WAIT)?;
            if st.success() {
                return Ok(());
            }
            Err(out.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .find_map(|v| v["message"].as_str().map(redact)).unwrap_or_else(|| format!("pi's sign-out exited with {st}")))
        }
        "opencode" => {
            let mut cmd = command(agent, None, env, "opencode");
            if crate::opencode_v2::major(&cmd) >= 2 {
                // its credential by label: without one, 2 opens a picker
                let name = need()?;
                let (_, list) = output(command(agent, None, env, "opencode").args(["auth", "list", "--standalone"]), WAIT)?;
                let label = opencode2_providers(&list).into_iter().find(|(p, _)| p.id == name).map(|(_, l)| l).ok_or(format!("opencode has no sign-in for {name}"))?;
                let (st, out) = output(cmd.args(["auth", "logout", "--standalone", name, &label]), WAIT)?;
                return if st.success() { Ok(()) } else { Err(redact(plain(&out).trim())) };
            }
            let (_, out) = output(cmd.args(["auth", "logout", need()?]), WAIT)?;
            if plain(&out).contains("Logout successful") { Ok(()) } else { Err(redact(plain(&out).trim())) }
        }
        a => Err(format!("unknown agent {a}")),
    }
}

// ── text ─────────────────────────────────────────────────────────────────────────────────────

fn name(agent: &str) -> &str {
    match agent {
        "claude" => "Claude Code",
        "codex" => "Codex",
        a => a,
    }
}

fn plural(n: u64, unit: &str) -> String {
    if n == 1 { format!("1 {unit}") } else { format!("{n} {unit}s") }
}

/// `s` without terminal escapes (CSI, OSC including OSC-8 links, others) and carriage returns.
pub(crate) fn plain(s: &str) -> String {
    let mut out = String::new();
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '\x1b' => match it.next() {
                Some('[') => {
                    while let Some(c) = it.next() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    while let Some(c) = it.next() {
                        if c == '\x07' || (c == '\x1b' && it.next_if_eq(&'\\').is_some()) {
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\r' => {}
            c => out.push(c),
        }
    }
    out
}

// The target of an OSC-8 link in `s`, else its first http(s) word.
fn url_in(s: &str) -> Option<String> {
    let osc = s.split("\x1b]8;").skip(1).find_map(|p| {
        let target = p.split_once(';')?.1;
        let end = target.find(['\x07', '\x1b'])?;
        Some(target[..end].to_string()).filter(|u| u.starts_with("http"))
    });
    osc.or_else(|| plain(s).split_whitespace().find(|w| w.starts_with("https://") || w.starts_with("http://")).map(str::to_string))
}

fn key_shaped(w: &str) -> bool {
    w.starts_with("sk-") || w.starts_with("AIza")
        || (w.len() >= 24 && w.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) && w.chars().any(|c| c.is_ascii_digit()))
}

// Error text from a CLI with key-shaped words masked, to their last 4 characters (not bytes: a key the CLI masked
// itself, "sk-ant-…ab", has a 3-byte one there, and slicing inside it panicked the thread that would end the sign-in).
fn redact(s: &str) -> String {
    let tail = |w: &str| w.char_indices().rev().nth(3).map_or(0, |(i, _)| i);
    s.split(' ').map(|w| if key_shaped(w) { format!("••••{}", &w[tail(w)..]) } else { w.to_string() })
        .collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    // One at a time: they share the sign-in lock and port 1455.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn fakes(p: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fakes").join(p)
    }

    // A scratch folder, removed at the end of a test that passed.
    struct Scratch(PathBuf);

    impl std::ops::Deref for Scratch {
        type Target = PathBuf;
        fn deref(&self) -> &PathBuf {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            if !thread::panicking() {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }

    fn scratch(name: &str) -> Scratch {
        let d = env::temp_dir().join(format!("agent-host-signin-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        Scratch(d)
    }

    // The agents are the fakes in tests/fakes/bin; they note what they were given in `out`.
    fn fake_env(out: &Path, mode: &str) -> Vec<(String, String)> {
        let path = format!("{}:{}", fakes("bin").display(), env::var("PATH").unwrap_or_default());
        vec![("PATH".into(), path), ("FAKE_OUT".into(), out.to_string_lossy().into()), ("FAKE_MODE".into(), mode.into())]
    }

    fn spec(agent: &str, method: &str, provider: Option<&str>, home: Option<&Path>, out: &Path, mode: &str) -> LoginSpec {
        LoginSpec { agent: agent.into(), account: format!("test-{agent}"), home: home.map(Path::to_path_buf), method: method.into(),
                    provider: provider.map(str::to_string), env: fake_env(out, mode) }
    }

    fn begin(s: LoginSpec, out: &Path) -> (Login, Receiver<Step>) {
        begin_for(s, out, Duration::from_secs(60))
    }

    fn begin_for(s: LoginSpec, out: &Path, timeout: Duration) -> (Login, Receiver<Step>) {
        let (tx, rx) = channel();
        let tx = Mutex::new(tx);
        let login = start_in(s, Box::new(move |s| { let _ = tx.lock().unwrap().send(s); }), timeout, &out.join("share")).unwrap();
        (login, rx)
    }

    fn next(rx: &Receiver<Step>) -> Step {
        rx.recv_timeout(Duration::from_secs(15)).expect("a step")
    }

    // Every step up to and including the last one.
    fn rest(rx: &Receiver<Step>) -> Vec<Step> {
        let mut all = Vec::new();
        loop {
            let s = next(rx);
            let end = matches!(s, Step::Done { .. } | Step::Failed(_) | Step::Cancelled | Step::Busy { .. });
            all.push(s);
            if end {
                assert!(rx.recv_timeout(Duration::from_millis(300)).is_err(), "nothing after the last step");
                return all;
            }
        }
    }

    fn gone(pid: u32) -> bool {
        let until = Instant::now() + Duration::from_secs(3);
        while Instant::now() < until {
            if !Path::new(&format!("/proc/{pid}")).exists() {
                return true;
            }
            thread::sleep(Duration::from_millis(50));
        }
        false
    }

    fn pid_in(out: &Path, f: &str) -> u32 {
        fs::read_to_string(out.join(f)).unwrap().trim().parse().unwrap()
    }

    fn opened(out: &Path) -> Vec<String> {
        fs::read_to_string(out.join("opened")).unwrap_or_default().lines().map(str::to_string).collect()
    }

    fn done(email: Option<&str>, plan: Option<&str>) -> Step {
        Step::Done { email: email.map(str::to_string), plan: plan.map(str::to_string) }
    }

    const CLAUDE_URL: &str = "https://claude.example/oauth/authorize?code=true&state=s1";
    const CLAUDE_AUTO_URL: &str = "https://claude.example/oauth/authorize?code=true&state=s1&redirect_uri=http%3A%2F%2Flocalhost%3A39855%2Fcallback";

    #[test]
    fn text_helpers() {
        let line = "If the browser didn't open, visit: \x1b]8;;https://a.example/x?y=1\x07\x1b[94mhttps://a.example/x?y=1\x1b[39m\x1b]8;;\x07";
        assert_eq!(plain(line), "If the browser didn't open, visit: https://a.example/x?y=1");
        assert_eq!(url_in(line.split_once("visit:").unwrap().1).as_deref(), Some("https://a.example/x?y=1"));
        assert_eq!(url_in("  https://b.example/z  ").as_deref(), Some("https://b.example/z"));
        assert_eq!(url_in("\x1b]8;;https://c.example\x1b\\text\x1b]8;;\x1b\\").as_deref(), Some("https://c.example"));
        assert_eq!(url_in("no address"), None);
        assert_eq!(code_in("Enter code: OCDE-1234"), Some("OCDE-1234".into()));
        assert_eq!(code_in("Open https://x.ai/device on any device and enter code: AB12-CD34."), Some("AB12-CD34".into()));
        assert_eq!(code_in("Complete authorization in your browser."), None);
        assert_eq!(redact("refused sk-ant-api03-abcdefghijklmnop key"), "refused ••••mnop key");
        assert_eq!(redact("refused sk-ant-…ab and sk-é"), "refused ••••-…ab and ••••sk-é");
        assert_eq!(agent_of(b"node\0/usr/bin/pi\0--mode\0rpc"), "pi");
        assert_eq!(agent_of(b"node\0/x/pi-login.mjs\0login\0openai-codex"), "pi");
        assert_eq!(agent_of(b"/usr/lib/node_modules/@openai/codex/vendor/codex\0app-server"), "codex");
        assert_eq!(agent_of(b"/home/u/.local/lib/node_modules/opencode-ai/bin/opencode.exe\0serve"), "opencode");
        assert_eq!(agent_of(b"/usr/bin/firefox"), "");
        let methods = json!([{"type": "api"}, {"type": "oauth", "label": "ChatGPT Pro/Plus (browser)"}, {"type": "oauth", "label": "ChatGPT Pro/Plus (headless)"}]);
        assert_eq!((opencode_method(&methods, false), opencode_method(&methods, true)), (Some(1), Some(2)));
        assert_eq!(opencode_method(&json!([{"type": "oauth", "label": "Login with GitHub Copilot"}]), true), Some(0));
        assert_eq!(opencode_method(&json!([{"type": "api"}]), false), None);
    }

    #[test]
    fn steps_as_json() {
        assert_eq!(Step::Waiting { url: "u".into(), opened: true }.to_json(), json!({"step": "waiting", "url": "u", "opened": true}));
        assert_eq!(Step::Code { url: String::new(), message: None }.to_json(), json!({"step": "code"}));
        assert_eq!(Step::Code { url: "u".into(), message: Some("m".into()) }.to_json(), json!({"step": "code", "url": "u", "message": "m"}));
        assert_eq!(Step::Device { url: "u".into(), user_code: "C".into() }.to_json(), json!({"step": "device", "url": "u", "userCode": "C"}));
        assert_eq!(Step::Busy { holder: "codex".into() }.to_json(), json!({"step": "busy", "holder": "codex"}));
        assert_eq!(done(Some("e"), Some("max")).to_json(), json!({"step": "done", "who": {"email": "e", "plan": "max"}}));
        assert_eq!(Step::Failed("m".into()).to_json(), json!({"step": "failed", "message": "m"}));
        assert_eq!(Step::Cancelled.to_json(), json!({"step": "cancelled"}));
    }

    #[test]
    fn env_is_scrubbed() {
        let mut cmd = Command::new("true");
        scrub(&mut cmd, ["OPENROUTER_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CODEX_HOME", "PATH", "HOME"].map(String::from).into_iter());
        let removed: Vec<String> = cmd.get_envs().filter(|(_, v)| v.is_none()).map(|(k, _)| k.to_string_lossy().into()).collect();
        assert_eq!(removed, ["ANTHROPIC_AUTH_TOKEN", "CODEX_HOME", "OPENROUTER_API_KEY"]);
        let home = Path::new("/tmp/acct");
        let cmd = command("pi", Some(home), &[("OPENROUTER_API_KEY".into(), "x".into())], "pi");
        let set: HashMap<String, String> = cmd.get_envs().filter_map(|(k, v)| Some((k.to_string_lossy().into(), v?.to_string_lossy().into()))).collect();
        assert_eq!(set.get("PI_CODING_AGENT_DIR").map(String::as_str), Some("/tmp/acct"));
        assert_eq!(set.get("OPENROUTER_API_KEY").map(String::as_str), Some("x"), "the spec's env is added after the scrub");
    }

    #[test]
    fn claude_browser() {
        let _s = serial();
        let out = scratch("claude-browser");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "ok"), &out);
        assert!(login.pid().is_some());
        assert_eq!(rest(&rx), [Step::Waiting { url: CLAUDE_AUTO_URL.into(), opened: true }, done(Some("someone@example.com"), Some("max"))],
                   "the standing paste prompt is no step");
        assert_eq!(fs::read_to_string(out.join("claude.args")).unwrap(), "auth\nlogin\n--claudeai\n");
        assert!(fs::read_to_string(out.join("claude.env")).unwrap().lines().any(|l| l == "CLAUDE_CONFIG_DIR"));
        assert!(home.join("signed-in").exists(), "it signed in to the account's folder");
        assert_eq!(opened(&out), [CLAUDE_AUTO_URL], "the host opens the CLI's automatic address, not its manual page");
        assert_eq!(active(), None);
        assert_eq!(login.pid(), None);
    }

    #[test]
    fn claude_browser_without_the_helper_falls_back_to_the_manual_page() {
        let _s = serial();
        let out = scratch("claude-manual");
        let (login, rx) = begin(spec("claude", "browser", None, None, &out, "nobrowser"), &out);
        assert_eq!(rest(&rx), [Step::Waiting { url: CLAUDE_URL.into(), opened: true }, done(Some("someone@example.com"), Some("max"))]);
        assert_eq!(opened(&out), [CLAUDE_URL], "no automatic address: the printed manual page is opened");
        assert_eq!(login.pid(), None);
    }

    #[test]
    fn claude_console_and_codes() {
        let _s = serial();
        let out = scratch("claude-code");
        let (login, rx) = begin(spec("claude", "console", None, None, &out, "code"), &out);
        assert_eq!(next(&rx), Step::Waiting { url: CLAUDE_AUTO_URL.into(), opened: true });
        assert!(rx.recv_timeout(Duration::from_millis(300)).is_err(), "the standing paste prompt is no step");
        assert_eq!(active(), Some(("claude".into(), "test-claude".into())));
        assert!(login.code("").is_err());
        login.code("not-a-code").unwrap();
        // the page a code to paste comes from is the printed one, not the automatic one opened
        assert_eq!(next(&rx), Step::Code { url: CLAUDE_URL.into(), message: Some("Invalid code. Please make sure the full code was copied.".into()) });
        login.code("abc#def").unwrap();
        assert_eq!(rest(&rx), [done(Some("someone@example.com"), Some("max"))]);
        assert_eq!(fs::read_to_string(out.join("claude.args")).unwrap(), "auth\nlogin\n--console\n");
        assert!(!fs::read_to_string(out.join("claude.env")).unwrap().lines().any(|l| l == "CLAUDE_CONFIG_DIR"), "Personal is the agent's own folder");
        assert_eq!(login.code("abc#def"), Err("This sign-in has ended".into()));
    }

    #[test]
    fn claude_automatic_route_is_done_when_the_sign_in_is_saved() {
        let _s = serial();
        let out = scratch("claude-auto");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "auto"), &out);
        assert_eq!(next(&rx), Step::Waiting { url: CLAUDE_AUTO_URL.into(), opened: true });
        let pid = pid_in(&out, "claude.pid");
        assert_eq!(rest(&rx), [done(Some("someone@example.com"), Some("max"))], "the CLI printed nothing and never exited");
        assert!(gone(pid), "the CLI is stopped");
        assert_eq!(active(), None);
        drop(login);
    }

    #[test]
    fn claude_exit_0_after_a_saved_sign_in_is_done_without_the_line() {
        let _s = serial();
        let out = scratch("claude-silent");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (_login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "silent"), &out);
        assert_eq!(rest(&rx).last(), Some(&done(Some("someone@example.com"), Some("max"))));
    }

    #[test]
    fn claude_exit_0_with_nothing_saved_is_not_done() {
        let _s = serial();
        let out = scratch("claude-nothing");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (_login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "exit0"), &out);
        assert!(matches!(rest(&rx).last(), Some(Step::Failed(_))));
    }

    #[test]
    fn claude_cancel_after_the_sign_in_was_saved_is_done() {
        let _s = serial();
        let out = scratch("claude-cancel-saved");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "auto"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let until = Instant::now() + Duration::from_secs(5);
        while !home.join(".credentials.json").exists() && Instant::now() < until {
            thread::sleep(Duration::from_millis(20));
        }
        login.cancel();
        assert_eq!(rest(&rx), [done(Some("someone@example.com"), Some("max"))], "Try again's cancel finds it finished");
        assert_eq!(active(), None);
    }

    // A sign-in that has ended asks the CLI nothing more: accounts.rs drops its Login at the last step, and that ran
    // `claude auth status` again before the Done went out; the saved-sign-in watcher ran it too.
    #[test]
    fn a_finished_claude_sign_in_runs_no_more_status() {
        let _s = serial();
        let out = scratch("claude-finished");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (login, rx) = begin(spec("claude", "browser", None, Some(&home), &out, "ok"), &out);
        assert_eq!(rest(&rx).last(), Some(&done(Some("someone@example.com"), Some("max"))));
        let asked = || fs::read_to_string(out.join("claude.statuses")).map_or(0, |s| s.lines().count());
        assert_eq!(asked(), 1, "the one status the Done is read from");
        drop(login);
        thread::sleep(Duration::from_millis(400));
        assert_eq!(asked(), 1, "asked again after the end");
    }

    #[test]
    fn claude_fails() {
        let _s = serial();
        let out = scratch("claude-fail");
        let (_login, rx) = begin(spec("claude", "browser", None, None, &out, "fail"), &out);
        let steps = rest(&rx);
        assert_eq!(steps.last(), Some(&Step::Failed("Request failed with status code 403".into())));
    }

    #[test]
    fn cancel_timeout_and_drop_kill_the_child() {
        let _s = serial();
        let out = scratch("claude-cancel");
        let (login, rx) = begin(spec("claude", "browser", None, None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let pid = pid_in(&out, "claude.pid");
        assert_eq!(login.pid(), Some(pid));
        login.cancel();
        assert_eq!(rest(&rx), [Step::Cancelled]);
        assert!(gone(pid), "claude is killed on cancel");
        assert_eq!(active(), None, "the sign-in lock is released");
        login.cancel();

        let out = scratch("claude-timeout");
        let t = Instant::now();
        let (_login, rx) = begin_for(spec("claude", "browser", None, None, &out, "hang"), &out, Duration::from_secs(2));
        let steps = rest(&rx);
        assert_eq!(steps.last(), Some(&Step::Failed("Sign-in didn't finish in 2 seconds".into())));
        assert!(t.elapsed() < Duration::from_secs(6));
        assert!(gone(pid_in(&out, "claude.pid")), "claude is killed at the timeout");

        let out = scratch("claude-drop");
        let (login, rx) = begin(spec("claude", "browser", None, None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        drop(login);
        assert_eq!(rest(&rx).last(), Some(&Step::Cancelled));
        assert!(gone(pid_in(&out, "claude.pid")), "claude is killed when the Login is dropped");
    }

    #[test]
    fn one_sign_in_at_a_time() {
        let _s = serial();
        let out = scratch("busy-lock");
        let (first, rx) = begin(spec("claude", "browser", None, None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let (second, rx2) = begin(spec("codex", "device", None, None, &out, "ok"), &out);
        assert_eq!(rest(&rx2), [Step::Busy { holder: "claude".into() }]);
        assert_eq!(second.pid(), None);
        assert!(!out.join("codex.pid").exists(), "nothing was started");
        drop(second);
        assert_eq!(active(), Some(("claude".into(), "test-claude".into())), "dropping the busy one leaves the first alone");
        first.cancel();
        assert_eq!(rest(&rx).last(), Some(&Step::Cancelled));
    }

    #[test]
    fn chatgpt_waits_for_port_1455() {
        let _s = serial();
        let Ok(held) = TcpListener::bind(("127.0.0.1", CHATGPT_PORT)) else {
            eprintln!("port 1455 is in use on this machine; busy-port test skipped");
            return;
        };
        let out = scratch("busy-port");
        for (agent, method, provider) in [("codex", "browser", None), ("pi", "browser", Some("openai-codex")), ("opencode", "browser", Some("openai"))] {
            let (_l, rx) = begin(spec(agent, method, provider, None, &out, "ok"), &out);
            assert_eq!(rest(&rx), [Step::Busy { holder: String::new() }], "{agent}: a program that is no agent holds it");
        }
        // the device code does not use it
        let (_l, rx) = begin(spec("codex", "device", None, None, &out, "ok"), &out);
        assert!(matches!(rest(&rx).last(), Some(Step::Done { .. })));
        drop(held);

        // an agent holding it is named
        let mut codex = Command::new(fakes("bin/codex"));
        let mut codex = codex.arg("app-server").env("FAKE_BIND", "1").env("FAKE_OUT", out.as_path()).stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while listeners(CHATGPT_PORT).is_empty() && Instant::now() < until {
            thread::sleep(Duration::from_millis(50));
        }
        let (_l, rx) = begin(spec("pi", "browser", Some("openai-codex"), None, &out, "ok"), &out);
        let steps = rest(&rx);
        let _ = codex.kill();
        let _ = codex.wait();
        assert_eq!(steps, [Step::Busy { holder: "codex".into() }]);
    }

    #[test]
    fn codex_browser_and_device() {
        let _s = serial();
        let out = scratch("codex");
        let home = out.join("acct");
        fs::create_dir_all(&home).unwrap();
        let (login, rx) = begin(spec("codex", "browser", None, Some(&home), &out, "ok"), &out);
        assert_eq!(rest(&rx), [Step::Waiting { url: "https://auth.example/codex?state=s2".into(), opened: true },
                               done(Some("someone@example.com"), Some("plus"))]);
        assert_eq!(opened(&out), ["https://auth.example/codex?state=s2"], "the host opens the browser once");
        let env_names = fs::read_to_string(out.join("opened.env")).unwrap();
        assert!(!env_names.lines().any(|l| l == "CODEX_HOME" || l.ends_with("_API_KEY")), "the browser gets no account or key variable");
        assert!(fs::read_to_string(out.join("codex.env")).unwrap().lines().any(|l| l == "CODEX_HOME"));
        assert!(gone(pid_in(&out, "codex.pid")), "the app-server is stopped once done");
        assert_eq!(login.code("x"), Err("This sign-in has ended".into()));

        let out = scratch("codex-device");
        let (login, rx) = begin(spec("codex", "device", None, None, &out, "ok"), &out);
        assert_eq!(rest(&rx), [Step::Device { url: "https://auth.openai.com/codex/device".into(), user_code: "WXYZ-1234".into() },
                               done(Some("someone@example.com"), Some("plus"))]);
        assert!(opened(&out).is_empty(), "a device code is for another device");
        drop(login);

        let out = scratch("codex-fail");
        let (login, rx) = begin(spec("codex", "browser", None, None, &out, "fail"), &out);
        assert_eq!(next(&rx), Step::Waiting { url: "https://auth.example/codex?state=s2".into(), opened: true });
        assert_eq!(login.code("x"), Err("Codex takes no code here".into()));
        assert_eq!(rest(&rx), [Step::Failed("Token exchange failed".into())]);

        let out = scratch("codex-cancel");
        let (login, rx) = begin(spec("codex", "browser", None, None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let pid = pid_in(&out, "codex.pid");
        login.cancel();
        assert_eq!(rest(&rx), [Step::Cancelled]);
        assert!(gone(pid));
    }

    #[test]
    fn pi_through_the_helper() {
        let _s = serial();
        let out = scratch("pi");
        let home = out.join("acct");
        let (_l, rx) = begin(spec("pi", "browser", Some("openai-codex"), Some(&home), &out, "browser"), &out);
        assert_eq!(rest(&rx), [Step::Waiting { url: "https://auth.example/pi?state=s4".into(), opened: true }, done(None, None)],
                   "the paste prompt raced against the browser is no step");
        assert_eq!(opened(&out), ["https://auth.example/pi?state=s4"]);
        let seen: Value = serde_json::from_slice(&fs::read(out.join("pi-login.json")).unwrap()).unwrap();
        assert_eq!(seen["provider"], "openai-codex");
        assert_eq!(seen["type"], "oauth");
        assert_eq!(seen["authPath"], home.join("auth.json").to_string_lossy().as_ref());
        assert_eq!(fs::read_to_string(out.join("share/pi-login.mjs")).unwrap(), HELPER);
        assert_eq!(fs::metadata(out.join("share")).unwrap().permissions().mode() & 0o777, 0o700);

        let out = scratch("pi-code");
        let (login, rx) = begin(spec("pi", "browser", Some("openrouter"), None, &out, "code"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());
        login.code("good-code").unwrap();
        assert_eq!(rest(&rx), [done(None, None)]);

        let out = scratch("pi-device");
        let (_l, rx) = begin(spec("pi", "device", Some("openai-codex"), None, &out, "device"), &out);
        assert_eq!(rest(&rx), [Step::Device { url: "https://auth.example/pi-device".into(), user_code: "PIDE-VICE".into() }, done(None, None)]);
        let seen: Value = serde_json::from_slice(&fs::read(out.join("pi-login.json")).unwrap()).unwrap();
        assert_eq!(seen["chose"], "device_code");

        let out = scratch("pi-copilot");
        let (_l, rx) = begin(spec("pi", "browser", Some("github-copilot"), None, &out, "copilot"), &out);
        assert_eq!(rest(&rx), [Step::Device { url: "https://github.com/login/device".into(), user_code: "GHCO-PILT".into() }, done(None, None)]);
        let seen: Value = serde_json::from_slice(&fs::read(out.join("pi-login.json")).unwrap()).unwrap();
        assert_eq!(seen["domain"], "", "blank: github.com");

        let out = scratch("pi-fail");
        let (_l, rx) = begin(spec("pi", "browser", Some("xai"), None, &out, "fail"), &out);
        assert_eq!(rest(&rx), [Step::Failed("Token exchange failed: 400".into())]);

        let out = scratch("pi-cancel");
        let (login, rx) = begin(spec("pi", "browser", Some("openrouter"), None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let pid = login.pid().unwrap();
        login.cancel();
        assert_eq!(rest(&rx), [Step::Cancelled]);
        assert!(gone(pid));

        assert!(start_in(spec("pi", "browser", None, None, &out, "ok"), Box::new(|_| {}), Duration::from_secs(5), &out).is_err(), "pi needs a provider");
        assert!(start_in(spec("pi", "browser", Some("../x"), None, &out, "ok"), Box::new(|_| {}), Duration::from_secs(5), &out).is_err());
        assert_eq!(active(), None);
    }

    // The real agents, each only up to the sign-in address, then cancelled: account folders in a
    // scratch dir (opencode: its own, which an unfinished sign-in leaves as it was), xdg-open and
    // $BROWSER the fake, and run with no display (`env -u WAYLAND_DISPLAY -u DISPLAY cargo test`).
    #[test]
    #[ignore = "runs the real agents up to their sign-in address"]
    fn real_sign_ins_up_to_the_address() {
        let _s = serial();
        assert!(env::var_os("WAYLAND_DISPLAY").is_none() && env::var_os("DISPLAY").is_none(), "run with no display");
        let out = scratch("real");
        let opener = out.join("opener");
        fs::create_dir_all(&opener).unwrap();
        std::os::unix::fs::symlink(fakes("bin/xdg-open"), opener.join("xdg-open")).unwrap();
        let env = vec![("PATH".into(), format!("{}:{}", opener.display(), env::var("PATH").unwrap())),
                       ("BROWSER".into(), opener.join("xdg-open").to_string_lossy().into()), ("FAKE_OUT".into(), out.to_string_lossy().into())];
        for (agent, method, provider, home) in [("claude", "browser", None, true), ("codex", "browser", None, true),
                                                ("pi", "browser", Some("openai-codex"), true), ("opencode", "browser", Some("openai"), false)] {
            let home = home.then(|| out.join(agent));
            if let Some(h) = &home {
                fs::create_dir_all(h).unwrap();
            }
            let s = LoginSpec { agent: agent.into(), account: "real-test".into(), home: home.clone(), method: method.into(),
                                provider: provider.map(str::to_string), env: env.clone() };
            let t = Instant::now();
            let (login, rx) = begin(s, &out);
            let first = next(&rx);
            let pid = login.pid();
            let Step::Waiting { url, opened } = &first else { panic!("{agent}: {first:?}") };
            let addr = url.split('?').next().unwrap_or("");
            let keys: Vec<&str> = url.split_once('?').map(|q| q.1.split('&').filter_map(|kv| kv.split('=').next()).collect()).unwrap_or_default();
            eprintln!("{agent}: waiting after {} ms at {addr} (query keys {keys:?}), opened {opened}", t.elapsed().as_millis());
            assert!(url.starts_with("https://"));
            // time for a paste prompt to show, which is no step
            thread::sleep(Duration::from_secs(1));
            login.cancel();
            assert_eq!(rest(&rx), [Step::Cancelled], "{agent}");
            assert!(pid.is_some_and(gone), "{agent}: its process is gone");
            if let Some(h) = &home {
                let saved: Vec<String> = fs::read_dir(h).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into()).collect();
                eprintln!("{agent}: its scratch folder holds {saved:?}");
                // pi's runtime creates an empty auth.json ({}) when it starts
                let empty = |f: &str| fs::read(h.join(f)).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()) == Some(json!({}));
                assert!(!saved.iter().any(|f| f == ".credentials.json" || (f == "auth.json" && !empty(f))), "{agent}: nothing was signed in");
            }
        }
        eprintln!("opened: {}", opened(&out).iter().map(|u| u.split('?').next().unwrap_or("").to_string()).collect::<Vec<_>>().join(", "));
    }

    // Prints each real agent's sign-in as status() reads it (email shown only as present or not).
    #[test]
    #[ignore = "runs the real agents' status commands"]
    fn real_status() {
        for agent in ["claude", "codex", "pi", "opencode"] {
            let t = Instant::now();
            match status(agent, None, &[]) {
                Ok(s) => {
                    let providers: Vec<String> = s.providers.iter().map(|p| format!("{}:{}:{}", p.id, p.via, p.state)).collect();
                    eprintln!("{agent}: {} plan {:?} email {} providers {providers:?} in {} ms", s.state, s.plan,
                              if s.email.is_some() { "present" } else { "none" }, t.elapsed().as_millis());
                }
                Err(e) => eprintln!("{agent}: {e}"),
            }
        }
        for agent in ["claude", "codex", "pi", "opencode"] {
            let d = crate::install::detect(agent);
            eprintln!("{agent}: {} at {:?}", d.to_json(), d.path);
        }
        eprintln!("node present: {}", crate::install::node_present());
    }

    fn calls(out: &Path) -> Vec<Value> {
        fs::read_to_string(out.join("opencode-calls")).unwrap_or_default().lines().map(|l| serde_json::from_str(l).unwrap()).collect()
    }

    #[test]
    fn opencode_through_serve() {
        let _s = serial();
        let out = scratch("opencode");
        let (_l, rx) = begin(spec("opencode", "browser", Some("openai"), None, &out, "ok"), &out);
        assert_eq!(rest(&rx), [Step::Waiting { url: "https://auth.example/opencode?state=s3".into(), opened: true }, done(None, None)]);
        assert_eq!(opened(&out), ["https://auth.example/opencode?state=s3"]);
        assert_eq!(calls(&out), [json!({"what": "authorize", "provider": "openai", "body": {"method": 0}}),
                                 json!({"what": "callback", "provider": "openai", "body": {"method": 0}})]);
        assert!(gone(pid_in(&out, "opencode.pid")), "serve is stopped once done");

        let out = scratch("opencode-device");
        let (_l, rx) = begin(spec("opencode", "device", Some("openai"), None, &out, "ok"), &out);
        assert_eq!(rest(&rx), [Step::Device { url: "https://auth.example/device".into(), user_code: "OCDE-1234".into() }, done(None, None)]);
        assert_eq!(calls(&out)[0]["body"], json!({"method": 1}));
        assert!(opened(&out).is_empty());

        let out = scratch("opencode-code");
        let (login, rx) = begin(spec("opencode", "browser", Some("anthropic"), None, &out, "ok"), &out);
        assert_eq!(next(&rx), Step::Waiting { url: "https://claude.example/oauth".into(), opened: true });
        assert_eq!(next(&rx), Step::Code { url: "https://claude.example/oauth".into(), message: None }, "a code is the one way on");
        // a refused code, answered false or with opencode's 400, is asked for again
        for bad in ["bad#code", "refused#400"] {
            login.code(bad).unwrap();
            assert_eq!(next(&rx), Step::Code { url: "https://claude.example/oauth".into(), message: Some("opencode did not accept the code".into()) });
        }
        login.code("good#code").unwrap();
        assert_eq!(rest(&rx), [done(None, None)]);
        assert_eq!(calls(&out)[3]["body"], json!({"method": 1, "code": "good#code"}), "opencode numbers the api method too");

        let out = scratch("opencode-fail");
        let (_l, rx) = begin(spec("opencode", "browser", Some("openai"), None, &out, "fail"), &out);
        assert_eq!(rest(&rx).last(), Some(&Step::Failed("Token exchange failed: 400".into())));

        let out = scratch("opencode-none");
        let (_l, rx) = begin(spec("opencode", "browser", Some("modal"), None, &out, "ok"), &out);
        assert_eq!(rest(&rx), [Step::Failed("opencode has no sign-in for modal".into())]);

        let out = scratch("opencode-cancel");
        let (login, rx) = begin(spec("opencode", "browser", Some("openai"), None, &out, "hang"), &out);
        assert!(matches!(next(&rx), Step::Waiting { .. }));
        let pid = pid_in(&out, "opencode.pid");
        login.cancel();
        assert_eq!(rest(&rx), [Step::Cancelled]);
        assert!(gone(pid), "serve is killed on cancel while the callback waits");
    }

    // An opencode sign-in starts at once though `opencode --version` is slow: accounts.rs holds the accounts lock
    // across start, and the version was read there on a binary not seen before.
    #[test]
    fn an_opencode_sign_in_starts_without_waiting_for_its_version() {
        let _s = serial();
        let out = scratch("opencode-slow-version");
        // a binary of its own, so its version is not one read before, whose --version takes 2 s
        let bin = out.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let slow = format!("#!/bin/sh\n[ \"$1\" = --version ] && sleep 2\nexec {} \"$@\"\n", fakes("bin").join("opencode").display());
        fs::write(bin.join("opencode"), slow).unwrap();
        fs::set_permissions(bin.join("opencode"), fs::Permissions::from_mode(0o755)).unwrap();
        let mut s = spec("opencode", "browser", Some("openai"), None, &out, "ok");
        for (_, v) in s.env.iter_mut().filter(|(k, _)| k == "PATH") {
            *v = format!("{}:{v}", bin.display());
        }
        let t = Instant::now();
        let (_l, rx) = begin(s, &out);
        assert!(t.elapsed() < Duration::from_secs(1), "the start took {:?}", t.elapsed());
        assert_eq!(rest(&rx).last(), Some(&done(None, None)));
    }

    // opencode 2's code sign-in: a refused code has failed its attempt for good, so a new one starts, its page opened,
    // and its code is asked for with "That code didn't work…"; the next code goes to the new attempt.
    #[test]
    fn opencode2_asks_again_on_a_new_page() {
        let _s = serial();
        let out = scratch("opencode2-code");
        // a binary of its own that says it is 2 (major() keeps a version per binary)
        let bin = out.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let two = format!("#!/bin/sh\n[ \"$1\" = --version ] && {{ echo 'opencode v2.1.1'; exit 0; }}\nexec {} \"$@\"\n", fakes("bin").join("opencode").display());
        fs::write(bin.join("opencode"), two).unwrap();
        fs::set_permissions(bin.join("opencode"), fs::Permissions::from_mode(0o755)).unwrap();
        let mut s = spec("opencode", "browser", Some("anthropic"), None, &out, "ok");
        for (_, v) in s.env.iter_mut().filter(|(k, _)| k == "PATH") {
            *v = format!("{}:{v}", bin.display());
        }
        let (login, rx) = begin(s, &out);
        let page = |n: u32| format!("https://claude.example/oauth?attempt={n}");
        assert_eq!(next(&rx), Step::Waiting { url: page(1), opened: true });
        assert_eq!(next(&rx), Step::Code { url: page(1), message: None });
        login.code("bad#code").unwrap();
        assert_eq!(next(&rx), Step::Waiting { url: page(2), opened: true });
        assert_eq!(next(&rx), Step::Code { url: page(2), message: Some("That code didn't work. Here's a new page to sign in again.".into()) });
        login.code("good#code").unwrap();
        assert_eq!(rest(&rx), [done(None, None)]);
        assert_eq!(opened(&out), [page(1), page(2)]);
        let calls: Vec<Value> = calls(&out).into_iter().take(4).collect();
        assert_eq!(calls, [json!({"path": "", "body": {"methodID": "claude-pro"}}), json!({"path": "con_1/complete", "body": {"code": "bad#code"}}),
                           json!({"path": "", "body": {"methodID": "claude-pro"}}), json!({"path": "con_2/complete", "body": {"code": "good#code"}})]);
    }

    #[test]
    fn status_and_sign_out() {
        let _s = serial();
        let out = scratch("status");
        let env = fake_env(&out, "ok");
        let s = status("claude", None, &env).unwrap();
        assert_eq!((s.state, s.email.as_deref()), ("signedOut", None));
        fs::write(out.join("signed-in"), "").unwrap();
        let s = status("claude", None, &env).unwrap();
        assert_eq!((s.state, s.email.as_deref(), s.plan.as_deref()), ("ready", Some("someone@example.com"), Some("max")));
        logout("claude", None, None, &env).unwrap();
        assert!(!out.join("signed-in").exists());

        let s = status("codex", None, &env).unwrap();
        assert_eq!(s.state, "signedOut");
        fs::write(out.join("codex-signed-in"), "").unwrap();
        let s = status("codex", None, &env).unwrap();
        assert_eq!(s.to_json(), json!({"state": "ready", "email": "someone@example.com", "plan": "plus", "providers": []}));
        logout("codex", None, None, &env).unwrap();
        assert!(!out.join("codex-signed-in").exists());
        assert!(gone(pid_in(&out, "codex.pid")), "the status app-server is stopped");

        let mut env_pi = env.clone();
        env_pi.push(("FAKE_PI_AUTH".into(), "openai-codex:oauth,openrouter:api_key,modal:api_key".into()));
        env_pi.push(("MODAL_API_KEY".into(), "not-a-real-key".into()));
        let s = status("pi", None, &env_pi).unwrap();
        assert_eq!(s.to_json()["providers"], json!([{"id": "openai-codex", "via": "oauth", "state": "ready"},
                                                     {"id": "openrouter", "via": "file", "state": "ready"},
                                                     {"id": "modal", "via": "key", "state": "ready"}]));
        assert_eq!(s.state, "ready");
        assert_eq!(status("pi", None, &env).unwrap().state, "noProvider");
        assert!(logout("pi", None, None, &env).is_err(), "pi signs out of one provider");
        logout_in("pi", Some(&out.join("acct")), Some("openrouter"), &env, &out.join("share")).unwrap();
        let seen: Value = serde_json::from_slice(&fs::read(out.join("pi-logout.json")).unwrap()).unwrap();
        assert_eq!(seen, json!({"provider": "openrouter", "authPath": out.join("acct/auth.json").to_string_lossy()}));

        let s = status("opencode", None, &env).unwrap();
        assert_eq!(s.to_json()["providers"], json!([{"id": "OpenRouter", "via": "file", "state": "ready"},
                                                     {"id": "OpenAI", "via": "oauth", "state": "ready"},
                                                     {"id": "Anthropic", "via": "key", "state": "ready"}]),
                   "the key-shaped line is dropped");
        logout("opencode", None, Some("OpenRouter"), &env).unwrap();
        assert_eq!(fs::read_to_string(out.join("opencode-logout")).unwrap(), "OpenRouter");
    }
}
