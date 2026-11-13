//! Finding the agents, and installing a missing one for this user with no root (plan §2.15):
//!   Claude Code  `curl -fsSL https://claude.ai/install.sh | bash`
//!   Codex, pi, opencode  `npm i -g --prefix ~/.local --allow-scripts=<package> <package>` (npm 12 runs no install
//!                script by default, and opencode's fetches its program); with no npm, first
//!                `pkexec pacman -S --needed --noconfirm nodejs npm` through the desktop's polkit agent.
//! An agent found on PATH outside the home folder, or root-owned, is installed for everyone
//! (route "system") and is never installed or updated here, so two copies never end up on PATH.
//! One install runs at a time. Each step's output reaches `on` a line at a time; the last step
//! is Done, Failed, Cancelled or NeedsNode. Cancel kills the step's process group and the steps
//! after it never start. pacman, once pkexec has authorised it, runs as root and cannot be
//! signalled: cancel then returns at once and sends Cancelled, and pacman is left to finish,
//! holding the install lock until it ends, when Cancelled is sent once more so the client reads
//! the machine as it is then.

use crate::signin::{command, drained, output, plain, read_lines, spawn, Proc};
use serde_json::{json, Value};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, fs, thread};

pub struct Detected {
    pub installed: bool,
    pub version: String,
    pub route: &'static str,  // "user" | "system" | "none"
    pub path: Option<PathBuf>,
}

impl Detected {
    /// One agent in the `accounts` state's `agents` (plan §3.2).
    pub fn to_json(&self) -> Value {
        json!({"installed": self.installed, "version": self.version, "route": self.route})
    }
}

/// The agent as found on the host's PATH, and its --version.
pub fn detect(agent: &str) -> Detected {
    detect_in(agent, &env::var("PATH").unwrap_or_default(), &crate::home())
}

/// npm and node are both on the host's PATH.
pub fn node_present() -> bool {
    node_in(&env::var("PATH").unwrap_or_default())
}

fn find(name: &str, path: &str) -> Option<PathBuf> {
    env::split_paths(path).map(|d| d.join(name))
        .find(|p| fs::metadata(p).is_ok_and(|m| m.is_file() && m.mode() & 0o111 != 0))
}

fn node_in(path: &str) -> bool {
    find("npm", path).is_some() && find("node", path).is_some()
}

fn detect_in(agent: &str, path: &str, home: &Path) -> Detected {
    let Some(bin) = find(agent, path) else {
        return Detected { installed: false, version: String::new(), route: "none", path: None };
    };
    let root_owned = fs::symlink_metadata(&bin).is_ok_and(|m| m.uid() == 0);
    let route = if bin.starts_with(home) && !root_owned { "user" } else { "system" };
    Detected { installed: true, version: version_of(&bin, path), route, path: Some(bin) }
}

// "2.1.288 (Claude Code)", "codex-cli 0.159.0", "0.86.1": the first word that starts with a digit
fn version_of(bin: &Path, path: &str) -> String {
    let mut cmd = command("", None, &[("PATH".into(), path.into())], &bin.to_string_lossy());
    output(cmd.arg("--version"), Duration::from_secs(5)).ok().and_then(|(_, out)| version_in(&out)).unwrap_or_default()
}

// The first word that is a version, a "v" before it allowed: opencode 2 says "opencode v2.1.2", and the install that
// worked was reported as failed.
fn version_in(out: &str) -> Option<String> {
    out.split_whitespace().map(|w| w.strip_prefix('v').filter(|r| r.starts_with(|c: char| c.is_ascii_digit())).unwrap_or(w))
        .find(|w| w.starts_with(|c: char| c.is_ascii_digit())).map(str::to_string)
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstallStep {
    Running { line: String },
    NeedsNode,  // npm is missing; install again with `with_node`
    Done { version: String },
    Failed(String),
    Cancelled,
}

impl InstallStep {
    /// The `install` push's fields (plan §3.2), without the agent.
    pub fn to_json(&self) -> Value {
        match self {
            InstallStep::Running { line } => json!({"step": "running", "line": line}),
            InstallStep::NeedsNode => json!({"step": "needsNode"}),
            InstallStep::Done { version } => json!({"step": "done", "version": version}),
            InstallStep::Failed(m) => json!({"step": "failed", "line": m}),
            InstallStep::Cancelled => json!({"step": "cancelled"}),
        }
    }
}

pub struct Install {
    run: Arc<Run>,
}

impl Install {
    pub fn cancel(&self) {
        self.run.end(InstallStep::Cancelled);
    }

    /// The running step's process.
    pub fn pid(&self) -> Option<u32> {
        let s = self.run.state.lock().unwrap();
        if s.ended { None } else { s.proc.as_ref().map(|p| p.pid) }
    }
}

struct State {
    ended: bool,
    proc: Option<Arc<Proc>>,
    tx: Option<Sender<InstallStep>>,
    late: Option<Sender<InstallStep>>, // a step that outlived the cancel: where its end is told
}

struct Run {
    id: u64,
    state: Mutex<State>,
}

impl Run {
    fn step(&self, s: InstallStep) {
        let st = self.state.lock().unwrap();
        if !st.ended {
            if let Some(tx) = &st.tx {
                let _ = tx.send(s);
            }
        }
    }

    // The last step, once: the running process is stopped and the install lock released first.
    // A process that cannot be stopped keeps the lock; run_step releases it when it ends.
    fn end(&self, s: InstallStep) {
        let (proc, tx) = {
            let mut st = self.state.lock().unwrap();
            if st.ended {
                return;
            }
            st.ended = true;
            (st.proc.take(), st.tx.take())
        };
        if proc.map_or(true, |p| p.stop()) {
            release(self.id);
        } else {
            self.state.lock().unwrap().late = tx.clone();
        }
        if let Some(tx) = tx {
            let _ = tx.send(s);
        }
    }
}

// (run id, agent) of the install under way
static INSTALLING: Mutex<Option<(u64, String)>> = Mutex::new(None);

fn release(id: u64) {
    let mut a = INSTALLING.lock().unwrap();
    if a.as_ref().is_some_and(|x| x.0 == id) {
        *a = None;
    }
}

fn package(agent: &str) -> Option<&'static str> {
    match agent {
        "codex" => Some("@openai/codex"),
        "pi" => Some("@earendil-works/pi-coding-agent"),
        // opencode 2 (opencode_v2.rs): `opencode-ai` stopped at 1.19, whose Zen sign-in no longer works upstream
        "opencode" => Some("@opencode/cli"),
        _ => None,
    }
}

/// Installs a missing agent for this user. Err: it cannot be installed here (unknown, already
/// installed, installed for everyone, or another install is running).
pub fn install(agent: &str, with_node: bool, on: Box<dyn Fn(InstallStep) + Send>) -> Result<Install, String> {
    install_in(agent, with_node, on, &[("PATH".into(), env::var("PATH").unwrap_or_default())], &crate::home())
}

fn install_in(agent: &str, with_node: bool, on: Box<dyn Fn(InstallStep) + Send>, env: &[(String, String)], home: &Path) -> Result<Install, String> {
    if agent != "claude" && package(agent).is_none() {
        return Err(format!("unknown agent {agent}"));
    }
    let path = env.iter().find(|(k, _)| k == "PATH").map(|(_, v)| v.clone()).unwrap_or_default();
    let found = detect_in(agent, &path, home);
    match found.route {
        "system" => return Err(format!("{agent} is installed for everyone on this computer; it is updated with the system")),
        "user" => return Err(format!("{agent} is already installed")),
        _ => {}
    }
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    {
        let mut a = INSTALLING.lock().unwrap();
        if let Some((_, other)) = &*a {
            return Err(format!("Another install is running ({other})"));
        }
        *a = Some((id, agent.to_string()));
    }
    let (tx, rx) = channel::<InstallStep>();
    thread::spawn(move || {
        for s in rx {
            on(s);
        }
    });
    let run = Arc::new(Run { id, state: Mutex::new(State { ended: false, proc: None, tx: Some(tx), late: None }) });
    let local = home.join(".local");
    let mut steps: Vec<Vec<String>> = Vec::new();
    match package(agent) {
        None => steps.push(vec!["bash".into(), "-c".into(), "set -o pipefail; curl -fsSL https://claude.ai/install.sh | bash".into()]),
        Some(pkg) => {
            if !node_in(&path) {
                if !with_node {
                    run.end(InstallStep::NeedsNode);
                    return Ok(Install { run });
                }
                steps.push(["pkexec", "pacman", "-S", "--needed", "--noconfirm", "nodejs", "npm"].map(String::from).to_vec());
            }
            // 1.x under its old name holds the same `opencode` command, and npm will not link over another package's
            if agent == "opencode" && local.join("lib/node_modules/opencode-ai").exists() {
                steps.push(vec!["npm".into(), "rm".into(), "-g".into(), "--prefix".into(), local.to_string_lossy().into(), "opencode-ai".into()]);
            }
            steps.push(vec!["npm".into(), "i".into(), "-g".into(), "--prefix".into(), local.to_string_lossy().into(),
                            format!("--allow-scripts={pkg}"), pkg.into()]);
        }
    }
    let (r, env, bin) = (run.clone(), env.to_vec(), local.join("bin").join(agent));
    thread::spawn(move || {
        for argv in steps {
            if r.state.lock().unwrap().ended {
                return;
            }
            if let Err(e) = run_step(&r, &argv, &env) {
                r.end(InstallStep::Failed(e));
                return;
            }
        }
        let path = env.iter().find(|(k, _)| k == "PATH").map(|(_, v)| v.clone()).unwrap_or_default();
        match version_of(&bin, &path) {
            v if !v.is_empty() => r.end(InstallStep::Done { version: v }),
            _ => r.end(InstallStep::Failed(format!("{} did not answer --version after the install", bin.display()))),
        }
    });
    Ok(Install { run })
}

// npm's first line of why a run failed. It prints the error's code, syscall, errno and path first, and ends
// every failed run with where its log is, so its last line never says why (npm 12.2).
fn npm_cause(l: &str) -> bool {
    let Some(said) = l.strip_prefix("npm error ").or(l.strip_prefix("npm ERR! ")) else { return false };
    let field = said.split_whitespace().next().unwrap_or("");
    !["code", "syscall", "errno", "path"].contains(&field) && !said.starts_with("A complete log of this run")
}

// Runs one step to its end, its output a line at a time. Err: it failed, with npm's line of why, or its
// last line.
fn run_step(run: &Arc<Run>, argv: &[String], env: &[(String, String)]) -> Result<(), String> {
    let mut cmd = command("", None, env, &argv[0]);
    cmd.args(&argv[1..]).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    // started under the state lock: a cancel comes either before it or finds it in `proc`
    let (proc, out, err) = {
        let mut st = run.state.lock().unwrap();
        if st.ended {
            return Ok(());
        }
        let mut child = spawn(&mut cmd).map_err(|e| format!("{}: {e}", argv[0]))?;
        let (out, err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
        let proc = Proc::new(child);
        st.proc = Some(proc.clone());
        (proc, out, err)
    };
    let (last, cause) = (Arc::new(Mutex::new(String::new())), Arc::new(Mutex::new(String::new())));
    let feed = |r: Arc<Run>, last: Arc<Mutex<String>>, cause: Arc<Mutex<String>>| move |l: String| {
        // progress redrawn with \r: the last part is what shows
        let l = plain(l.rsplit('\r').next().unwrap_or("")).trim().to_string();
        if !l.is_empty() {
            if npm_cause(&l) {
                let mut c = cause.lock().unwrap();
                if c.is_empty() {
                    *c = l.clone();
                }
            }
            *last.lock().unwrap() = l.clone();
            r.step(InstallStep::Running { line: l });
        }
    };
    let orx = read_lines(out, feed(run.clone(), last.clone(), cause.clone()));
    let erx = read_lines(err, feed(run.clone(), last.clone(), cause.clone()));
    let st = proc.wait();
    drained(&orx);
    drained(&erx);
    let (ended, late) = {
        let mut s = run.state.lock().unwrap();
        s.proc = None;
        (s.ended, s.late.take())
    };
    if ended {
        // a step that outlived its cancel (pacman as root) held the install lock until now: the client reads what it
        // left (Node.js, now there) before a new install can start, so this goes first
        if let Some(tx) = late {
            let _ = tx.send(InstallStep::Cancelled);
        }
        release(run.id);
        return Ok(());
    }
    match st {
        None => Ok(()),
        Some(s) if s.success() => Ok(()),
        Some(s) => {
            let (last, cause) = (last.lock().unwrap().clone(), cause.lock().unwrap().clone());
            Err(if !cause.is_empty() { cause } else if last.is_empty() { format!("{} exited with {s}", argv[0]) } else { last })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::Receiver;
    use std::time::Instant;

    // One at a time: they share the install lock.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn fakes(p: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fakes").join(p)
    }

    // Removes the sandbox at the end of a test that passed.
    struct Gone(PathBuf);

    impl Drop for Gone {
        fn drop(&mut self) {
            if !thread::panicking() {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }

    // A home folder and a bin folder on PATH holding only the tools the fakes use (no node, no npm).
    fn sandbox(name: &str) -> (Gone, PathBuf, PathBuf) {
        let d = env::temp_dir().join(format!("agent-host-install-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        let bin = d.join("bin");
        fs::create_dir_all(&bin).unwrap();
        for t in ["sh", "bash", "cat", "mkdir", "chmod", "sleep", "ln", "dirname", "env", "cut", "python3", "rm"] {
            if let Some(p) = find(t, "/usr/bin:/bin") {
                std::os::unix::fs::symlink(p, bin.join(t)).unwrap();
            }
        }
        (Gone(d.clone()), d.join("home"), bin)
    }

    fn env_for(home: &Path, bin: &Path, extra: &[&str], mode: &str) -> Vec<(String, String)> {
        // as agent-host.service's PATH, ~/.local/bin first
        let mut path = vec![home.join(".local/bin").to_string_lossy().into_owned(), bin.to_string_lossy().into_owned()];
        path.extend(extra.iter().map(|p| fakes(p).to_string_lossy().into_owned()));
        let out = home.parent().unwrap().to_string_lossy().into_owned();
        vec![("PATH".into(), path.join(":")), ("HOME".into(), home.to_string_lossy().into()), ("FAKE_OUT".into(), out),
             ("FAKE_MODE".into(), mode.into()), ("FAKE_BIN".into(), bin.to_string_lossy().into())]
    }

    fn begin(agent: &str, with_node: bool, env: &[(String, String)], home: &Path) -> Result<(Install, Receiver<InstallStep>), String> {
        let (tx, rx) = channel();
        let tx = Mutex::new(tx);
        let i = install_in(agent, with_node, Box::new(move |s| { let _ = tx.lock().unwrap().send(s); }), env, home)?;
        Ok((i, rx))
    }

    fn rest(rx: &Receiver<InstallStep>) -> Vec<InstallStep> {
        let mut all = Vec::new();
        loop {
            let s = rx.recv_timeout(Duration::from_secs(15)).expect("a step");
            let end = !matches!(s, InstallStep::Running { .. });
            all.push(s);
            if end {
                return all;
            }
        }
    }

    fn lines(steps: &[InstallStep]) -> Vec<&str> {
        steps.iter().filter_map(|s| match s { InstallStep::Running { line } => Some(line.as_str()), _ => None }).collect()
    }

    #[test]
    fn detects_routes() {
        let (_d, home, bin) = sandbox("detect");
        let path = format!("{}:{}", bin.display(), fakes("bin").display());
        let d = detect_in("claude", &path, &home);
        assert_eq!(d.to_json(), json!({"installed": true, "version": "9.9.9", "route": "system"}), "outside this home folder");
        let d = detect_in("claude", &path, Path::new(env!("CARGO_MANIFEST_DIR")));
        assert_eq!((d.route, d.version.as_str()), ("user", "9.9.9"));
        let d = detect_in("codex", &path, &home);
        assert_eq!((d.installed, d.version.as_str()), (true, "0.0.1"));
        assert_eq!(detect_in("nothing-here", &path, &home).to_json(), json!({"installed": false, "version": "", "route": "none"}));
        assert!(!node_in(&bin.to_string_lossy()));
        assert!(node_in(&format!("{}:{}", bin.display(), fakes("node").display())));
        if let Some(p) = find("ls", "/usr/bin:/bin").filter(|p| fs::symlink_metadata(p).is_ok_and(|m| m.uid() == 0)) {
            assert_eq!(detect_in("ls", &p.parent().unwrap().to_string_lossy(), Path::new("/")).route, "system", "root-owned");
        }
    }

    #[test]
    fn installs_claude_with_its_script() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("claude");
        let env = env_for(&home, &bin, &["install"], "ok");
        let (_i, rx) = begin("claude", false, &env, &home).unwrap();
        let steps = rest(&rx);
        assert_eq!(lines(&steps), ["Setting up Claude Code...", "Claude Code successfully installed!"]);
        assert_eq!(steps.last(), Some(&InstallStep::Done { version: "9.9.9".into() }));
        assert!(home.join(".local/bin/claude").exists());
        assert!(begin("claude", false, &env, &home).is_err(), "now installed for this user");

        let (_d, home, bin) = sandbox("claude-fail");
        let (_i, rx) = begin("claude", false, &env_for(&home, &bin, &["install"], "fail"), &home).unwrap();
        assert_eq!(rest(&rx).last(), Some(&InstallStep::Failed("curl: (22) The requested URL returned error: 404".into())));
    }

    #[test]
    fn installs_with_npm() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("npm");
        let env = env_for(&home, &bin, &["node"], "ok");
        let (_i, rx) = begin("codex", false, &env, &home).unwrap();
        let steps = rest(&rx);
        assert_eq!(lines(&steps), ["reify: 100%", "added 1 package in 1s"], "a redrawn line shows its last part");
        assert_eq!(steps.last(), Some(&InstallStep::Done { version: "1.2.3".into() }));
        let args = fs::read_to_string(home.parent().unwrap().join("npm.args")).unwrap();
        assert_eq!(args.trim(), format!("i -g --prefix {} --allow-scripts=@openai/codex @openai/codex", home.join(".local").display()));
        assert!(home.join(".local/bin/codex").exists());

        let (_i, rx) = begin("opencode", false, &env_for(&home, &bin, &["node"], "fail"), &home).unwrap();
        assert_eq!(rest(&rx).last(), Some(&InstallStep::Failed("npm error 404 Not Found - GET https://registry.npmjs.org/@opencode/cli".into())));
        assert!(begin("gemini", false, &env, &home).is_err());
    }

    // npm 12.2's lines for a registry it could not reach (127.0.0.1:9, run here): the cause, not the log's place
    #[test]
    fn npm_says_why() {
        let said = ["npm error code ECONNREFUSED", "npm error syscall connect", "npm error errno ECONNREFUSED",
                    "npm error FetchError: request to http://127.0.0.1:9/@openai%2fcodex failed, reason: connect ECONNREFUSED 127.0.0.1:9",
                    "npm error     at ClientRequest.<anonymous> (/usr/lib/node_modules/npm/node_modules/minipass-fetch/lib/index.js:130:14)",
                    "npm error A complete log of this run can be found in: /tmp/npm/_logs/2026-11-12T23_34_47_382Z-debug-0.log"];
        assert_eq!(said.iter().find(|l| npm_cause(l)), Some(&said[3]));
        assert!(!npm_cause("added 1 package in 1s") && !npm_cause("npm error path /usr/lib/node_modules/@openai"));
        assert!(npm_cause("npm ERR! 404 Not Found - GET https://registry.npmjs.org/x"), "older npm");
    }

    #[test]
    fn versions_read() {
        assert_eq!(version_in("opencode v2.1.2\n").as_deref(), Some("2.1.2"));
        assert_eq!(version_in("1.18.31").as_deref(), Some("1.18.31"));
        assert_eq!(version_in("codex-cli 0.46.0").as_deref(), Some("0.46.0"));
        assert_eq!(version_in("verbose: on"), None);
    }

    #[test]
    fn opencode_1_under_its_old_name_goes_first() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("npm-oc");
        let old = home.join(".local/lib/node_modules/opencode-ai");
        fs::create_dir_all(&old).unwrap();
        let (_i, rx) = begin("opencode", false, &env_for(&home, &bin, &["node"], "ok"), &home).unwrap();
        assert_eq!(rest(&rx).last(), Some(&InstallStep::Done { version: "1.2.3".into() }));
        assert!(!old.exists(), "opencode-ai removed");
        let args = fs::read_to_string(home.parent().unwrap().join("npm.args")).unwrap();
        assert!(args.trim().ends_with("--allow-scripts=@opencode/cli @opencode/cli"), "then opencode 2: {args}");
    }

    #[test]
    fn needs_node_first() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("node");
        let env = env_for(&home, &bin, &["install"], "ok");
        let (i, rx) = begin("pi", false, &env, &home).unwrap();
        assert_eq!(rest(&rx), [InstallStep::NeedsNode]);
        assert_eq!(i.pid(), None);
        let (_i, rx) = begin("pi", true, &env, &home).unwrap();
        let steps = rest(&rx);
        assert_eq!(steps.last(), Some(&InstallStep::Done { version: "1.2.3".into() }));
        assert!(lines(&steps).contains(&"installing nodejs..."));
        let out = home.parent().unwrap();
        assert_eq!(fs::read_to_string(out.join("pkexec.args")).unwrap().trim(), "pacman -S --needed --noconfirm nodejs npm");
        assert!(fs::read_to_string(out.join("npm.args")).unwrap().ends_with("@earendil-works/pi-coding-agent\n"));
    }

    #[test]
    fn cancel_kills_the_step_and_one_at_a_time() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("cancel");
        let env = env_for(&home, &bin, &["node"], "hang");
        let (i, rx) = begin("codex", false, &env, &home).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(10)).unwrap(), InstallStep::Running { line: "npm http fetch GET 200".into() });
        let pid = i.pid().unwrap();
        assert_eq!(fs::read_to_string(home.parent().unwrap().join("npm.pid")).unwrap().trim(), pid.to_string());
        assert_eq!(begin("pi", false, &env, &home).err(), Some("Another install is running (codex)".into()));
        i.cancel();
        assert_eq!(rest(&rx), [InstallStep::Cancelled]);
        let until = Instant::now() + Duration::from_secs(3);
        while Path::new(&format!("/proc/{pid}")).exists() && Instant::now() < until {
            thread::sleep(Duration::from_millis(50));
        }
        assert!(!Path::new(&format!("/proc/{pid}")).exists(), "npm is killed");
        assert_eq!(i.pid(), None);
        assert!(INSTALLING.lock().unwrap().is_none());
    }

    #[test]
    fn cancel_leaves_a_root_step_to_finish() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let (_d, home, bin) = sandbox("root");
        let env = env_for(&home, &bin, &["install"], "root");
        let out = home.parent().unwrap().to_path_buf();
        let (i, rx) = begin("pi", true, &env, &home).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(10)).unwrap(), InstallStep::Running { line: "running as root".into() });
        let pid: u32 = fs::read_to_string(out.join("pkexec.pid")).unwrap().trim().parse().unwrap();
        let t = Instant::now();
        i.cancel();
        assert!(t.elapsed() < Duration::from_millis(500), "cancel does not wait for a step it cannot stop");
        assert_eq!(rest(&rx), [InstallStep::Cancelled]);
        assert!(Path::new(&format!("/proc/{pid}")).exists(), "pkexec runs on");
        assert_eq!(begin("codex", false, &env, &home).err(), Some("Another install is running (pi)".into()), "it holds the lock");
        let until = Instant::now() + Duration::from_secs(5);
        while INSTALLING.lock().unwrap().is_some() && Instant::now() < until {
            thread::sleep(Duration::from_millis(50));
        }
        assert!(INSTALLING.lock().unwrap().is_none(), "the lock is released when it ends");
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).ok(), Some(InstallStep::Cancelled), "and the client is told again, to read the agents as they are now");
        assert!(!Path::new(&format!("/proc/{pid}")).exists(), "and it is reaped");
        assert!(!out.join("npm.args").exists(), "the npm step never started");
    }

    #[test]
    fn steps_as_json() {
        assert_eq!(InstallStep::Running { line: "l".into() }.to_json(), json!({"step": "running", "line": "l"}));
        assert_eq!(InstallStep::NeedsNode.to_json(), json!({"step": "needsNode"}));
        assert_eq!(InstallStep::Done { version: "1".into() }.to_json(), json!({"step": "done", "version": "1"}));
        assert_eq!(InstallStep::Failed("m".into()).to_json(), json!({"step": "failed", "line": "m"}));
        assert_eq!(InstallStep::Cancelled.to_json(), json!({"step": "cancelled"}));
    }
}
