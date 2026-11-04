//! The models and commands each agent account offers, for the app's model list and `/` list:
//! read from the agent's own files and commands, normalised to the `catalog` event,
//!   {ev:"catalog", agent, account, cwd?, at, stale,
//!    models:[{id, label, group, detail?, about?, resolved?, efforts:[..], defaultEffort?, isDefault?, source:"offered"|"used"|"api"}],
//!    commands:[{name, description, hint?, aliases?, kind:"skill"|"command"}]}
//! `cwd` is the folder asked for (none: $HOME's commands). `resolved`, on Claude Code's own rows, is
//! the id a session started with that row reports in system/init.
//! and cached in ${AGENT_HOST_CACHE:-~/.cache/agent-host}/catalog: <account>.json holds the models,
//! <account>-<sha1(cwd)[:12]>.json one folder's commands. A cache is stale after 24 h, when the
//! agent's --version changed, or (Claude, pi) when the folder's commands were never read.
//!
//! Claude Code: the `initialize` reply's models (a session's own, or a probe that sends no message),
//! `additionalModelOptionsCache` from its .claude.json, ids its transcripts used that it no longer
//! offers, and Anthropic's model list when a key is given. Codex: models_cache.json, else the bundled
//! list; its skills come through note_commands. pi: `--list-models` and an rpc `get_commands`.
//! opencode: `models --verbose`; its commands come through note_commands.

use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};
use std::{env, process, thread};

const DAY_MS: u64 = 24 * 3600 * 1000;
const WAIT: Duration = Duration::from_secs(20);
const VERSION_WAIT: Duration = Duration::from_secs(5);
// opencode's variant keys come back unordered; this is their order
const EFFORTS: [&str; 9] = ["none", "off", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"];
const PI_EFFORTS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
// removed from every child's env before ctx.env is added (plan §2.14)
const SCRUB: [&str; 9] = ["ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "OPENCODE_AUTH_CONTENT", "CLAUDE_CONFIG_DIR",
                          "CODEX_HOME", "PI_CODING_AGENT_DIR", "PI_CODING_AGENT_SESSION_DIR", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"];

pub struct Ctx {
    pub agent: String,
    pub account: String,
    pub home: Option<PathBuf>,          // the account's folder (CLAUDE_CONFIG_DIR, CODEX_HOME, PI_CODING_AGENT_DIR); None: the agent's own
    pub env: Vec<(String, String)>,     // added to every child's env
    pub cwd: Option<String>,            // whose commands; None: $HOME
    pub anthropic_key: Option<String>,  // Claude: also list every model version from the API
}

impl Ctx {
    fn folder(&self) -> String {
        self.cwd.clone().unwrap_or_else(home_str)
    }
}

/// The cached catalog, or None when this account has none yet. Reads files only: the agent's
/// --version is compared as last read, and read again on a thread at most once a minute, so a
/// change counts from the first call after that read ends.
pub fn cached(ctx: &Ctx) -> Option<Value> {
    cached_in(&root(), ctx)
}

/// Reads every source again and writes the cache. Blocking: each command it runs is killed at 20 s.
pub fn refresh(ctx: &Ctx) -> Result<Value, String> {
    refresh_in(&root(), ctx, WAIT)
}

/// A Claude session's `initialize` reply: its models for the account, its commands for the cwd.
pub fn note_claude_init(account: &str, cwd: &str, init: &Value) {
    note_claude_init_in(&root(), account, cwd, init)
}

/// Commands an agent's server listed: Codex `skills/list`, opencode `GET /command`, pi `get_commands`.
pub fn note_commands(agent: &str, account: &str, cwd: &str, raw: &Value) {
    note_commands_in(&root(), agent, account, cwd, raw)
}

fn root() -> PathBuf {
    env::var_os("AGENT_HOST_CACHE").map(PathBuf::from).unwrap_or_else(|| crate::home().join(".cache/agent-host")).join("catalog")
}

fn home_str() -> String {
    crate::home().to_string_lossy().into_owned()
}

fn cached_in(root: &Path, ctx: &Ctx) -> Option<Value> {
    let f = read_json(&models_file(root, &ctx.account)).filter(|f| f["agent"] == ctx.agent.as_str())?;
    let unread = matches!(ctx.agent.as_str(), "claude" | "pi") && !commands_file(root, &ctx.account, &ctx.folder()).exists();
    let stale = crate::now_ms().saturating_sub(f["refreshed"].as_u64().unwrap_or(0)) > DAY_MS
        || version_seen(ctx).is_some_and(|v| f["version"].as_str() != Some(v.as_str()))
        || unread;
    Some(event(root, ctx, &f, stale))
}

fn refresh_in(root: &Path, ctx: &Ctx, wait: Duration) -> Result<Value, String> {
    let path = models_file(root, &ctx.account);
    let old = read_json(&path).filter(|f| f["agent"] == ctx.agent.as_str()).unwrap_or(Value::Null);
    let cwd = ctx.folder();
    let now = crate::now_ms();
    let mut f = json!({"agent": ctx.agent, "account": ctx.account, "version": version(ctx), "at": now, "refreshed": now});
    match ctx.agent.as_str() {
        "claude" => {
            let init = claude_probe(ctx, &cwd, wait)?;
            write_commands(root, &ctx.account, &cwd, claude_commands(&init["commands"]));
            let was = &old["claude"];
            let fresh = |at: &Value| now.saturating_sub(at.as_u64().unwrap_or(0)) < DAY_MS;
            let (used, used_at) = if fresh(&was["usedAt"]) {
                (was["used"].clone(), was["usedAt"].clone())
            } else {
                (json!(used_ids(&claude_dir(ctx).join("projects"))), json!(now))
            };
            let api = match &ctx.anthropic_key {
                Some(key) => match anthropic_models(ctx, key) {
                    Ok(list) => list,
                    Err(e) => {
                        eprintln!("[catalog] Anthropic's model list: {e}");
                        was["api"].clone()
                    }
                },
                None => json!([]),
            };
            f["claude"] = json!({"init": trim_init(&init["models"]), "extra": claude_extra(&claude_json(ctx)),
                                 "used": used, "usedAt": used_at, "api": api});
            f["models"] = json!(compose_claude(&f["claude"]));
        }
        "codex" => {
            let dir = ctx.home.clone().unwrap_or_else(|| crate::home().join(".codex"));
            let list = read_json(&dir.join("models_cache.json")).filter(|v| v["models"].as_array().is_some_and(|a| !a.is_empty()));
            let list = match list {
                Some(v) => v,
                None => output_json(command(ctx, "codex").args(["debug", "models", "--bundled"]), wait)?,
            };
            let default = fs::read_to_string(dir.join("config.toml")).ok().and_then(|t| toml_top(&t, "model"));
            f["models"] = json!(codex_models(&list, default.as_deref()));
        }
        "pi" => {
            let table = output(command(ctx, "pi").args(["--offline", "--list-models"]), wait)?;
            let dir = ctx.home.clone().unwrap_or_else(|| crate::home().join(".pi/agent"));
            let settings = read_json(&dir.join("settings.json")).unwrap_or(Value::Null);
            let default = settings["defaultProvider"].as_str().zip(settings["defaultModel"].as_str());
            f["models"] = json!(pi_models(&table, default, &provider_names()));
            match pi_get_commands(ctx, &cwd, wait) {
                Ok(raw) => write_commands(root, &ctx.account, &cwd, pi_commands(&raw)),
                Err(e) => eprintln!("[catalog] pi's commands: {e}"),
            }
        }
        "opencode" => {
            let cmd = command(ctx, "opencode");
            if crate::opencode_v2::major(&cmd) >= 2 {
                f["models"] = json!(opencode2_models(&crate::opencode_v2::models(cmd, wait)?, &provider_names()));
            } else {
                let text = output(command(ctx, "opencode").args(["models", "--verbose"]), wait)?;
                f["models"] = json!(opencode_models(&text, &provider_names()));
            }
        }
        a => return Err(format!("no model list for {a}")),
    }
    write_json(&path, &f);
    Ok(event(root, ctx, &f, false))
}

fn note_claude_init_in(root: &Path, account: &str, cwd: &str, init: &Value) {
    if init["models"].as_array().is_some_and(|a| !a.is_empty()) {
        let path = models_file(root, account);
        // never refreshed: stale, so the first catalog request reads the other sources
        let mut f = read_json(&path).filter(|f| f["agent"] == "claude")
            .unwrap_or_else(|| json!({"agent": "claude", "account": account, "version": "", "refreshed": 0}));
        f["claude"]["init"] = trim_init(&init["models"]);
        f["models"] = json!(compose_claude(&f["claude"]));
        f["at"] = json!(crate::now_ms());
        write_json(&path, &f);
    }
    if init["commands"].is_array() {
        write_commands(root, account, cwd, claude_commands(&init["commands"]));
    }
}

fn note_commands_in(root: &Path, agent: &str, account: &str, cwd: &str, raw: &Value) {
    let list = match agent {
        "claude" => claude_commands(raw),
        "codex" => codex_skills(raw),
        "pi" => pi_commands(raw),
        "opencode" => opencode_commands(raw),
        _ => return,
    };
    write_commands(root, account, cwd, list);
}

// The models file plus the folder's commands (else the home folder's) as the catalog event.
fn event(root: &Path, ctx: &Ctx, f: &Value, stale: bool) -> Value {
    let commands = read_json(&commands_file(root, &ctx.account, &ctx.folder()))
        .or_else(|| read_json(&commands_file(root, &ctx.account, &home_str())))
        .map(|c| c["commands"].clone())
        .unwrap_or(json!([]));
    let mut e = json!({"ev": "catalog", "agent": ctx.agent, "account": ctx.account, "at": f["at"], "stale": stale,
                       "models": f["models"], "commands": commands});
    if let Some(cwd) = &ctx.cwd {
        e["cwd"] = json!(cwd);
    }
    e
}

// ---- files ----

fn safe(name: &str) -> String {
    let s: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect();
    if s.is_empty() || s.starts_with('.') { format!("_{s}") } else { s }
}

fn models_file(root: &Path, account: &str) -> PathBuf {
    root.join(format!("{}.json", safe(account)))
}

fn commands_file(root: &Path, account: &str, cwd: &str) -> PathBuf {
    root.join(format!("{}-{}.json", safe(account), &sha1_hex(cwd.as_bytes())[..12]))
}

fn write_commands(root: &Path, account: &str, cwd: &str, list: Vec<Value>) {
    let f = json!({"account": account, "cwd": cwd, "at": crate::now_ms(), "commands": list});
    write_json(&commands_file(root, account, cwd), &f);
}

fn read_json(path: &Path) -> Option<Value> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

// tmp + rename, so a reader never sees half a file
fn write_json(path: &Path, v: &Value) {
    static N: AtomicU64 = AtomicU64::new(0);
    let Some(dir) = path.parent() else { return };
    let tmp = dir.join(format!(".tmp-{}-{}", process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let done = fs::create_dir_all(dir).and_then(|_| fs::write(&tmp, v.to_string())).and_then(|_| fs::rename(&tmp, path));
    if let Err(e) = done {
        eprintln!("[catalog] {}: {e}", path.display());
        let _ = fs::remove_file(&tmp);
    }
}

// ---- processes ----

// An agent (or curl) with the account's env: key and account variables removed, then ctx.home
// as the agent's folder variable and ctx.env added.
// `claude` as the catalog runs it, on the default account (results.rs)
pub(crate) fn claude_command() -> Command {
    command(&Ctx { agent: "claude".into(), account: String::new(), home: None, env: Vec::new(), cwd: None, anthropic_key: None }, "claude")
}

fn command(ctx: &Ctx, program: &str) -> Command {
    let mut cmd = Command::new(program);
    for (k, _) in env::vars_os() {
        let k = k.to_string_lossy();
        if k.ends_with("_API_KEY") || SCRUB.contains(&k.as_ref()) {
            cmd.env_remove(k.as_ref());
        }
    }
    let var = match ctx.agent.as_str() {
        "claude" => Some("CLAUDE_CONFIG_DIR"),
        "codex" => Some("CODEX_HOME"),
        "pi" => Some("PI_CODING_AGENT_DIR"),
        _ => None,
    };
    if let (Some(var), Some(home)) = (var, &ctx.home) {
        cmd.env(var, home);
    }
    cmd.envs(ctx.env.iter().map(|(k, v)| (k, v)));
    cmd
}

// Its stdout when it exits 0 within `wait`; at `wait` it is killed (its own pid only).
fn output(cmd: &mut Command, wait: Duration) -> Result<String, String> {
    let name = cmd.get_program().to_string_lossy().into_owned();
    let deadline = Instant::now() + wait;
    let mut child = cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| format!("{name}: {e}"))?;
    let mut out = child.stdout.take().ok_or_else(|| format!("{name}: no stdout"))?;
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = out.read_to_end(&mut buf);
        let _ = tx.send(buf);
    });
    let buf = rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    match (reap(child, deadline), buf) {
        (Some(s), Ok(buf)) if s.success() => Ok(String::from_utf8_lossy(&buf).into_owned()),
        (Some(s), Ok(_)) => Err(format!("{name} exited with {s}")),
        _ => Err(format!("{name} did not finish within {} s", wait.as_secs())),
    }
}

fn output_json(cmd: &mut Command, wait: Duration) -> Result<Value, String> {
    let name = cmd.get_program().to_string_lossy().into_owned();
    serde_json::from_str(&output(cmd, wait)?).map_err(|_| format!("{name} printed no JSON"))
}

// `<agent> --version` per agent and PATH: when the last read started, and what it printed once it ended.
fn versions() -> &'static Mutex<HashMap<String, (Instant, Option<String>)>> {
    static SEEN: OnceLock<Mutex<HashMap<String, (Instant, Option<String>)>>> = OnceLock::new();
    SEEN.get_or_init(Default::default)
}

fn version_key(ctx: &Ctx) -> String {
    let path = ctx.env.iter().find(|(k, _)| k == "PATH").map(|(_, v)| v.as_str()).unwrap_or("");
    format!("{}\n{path}", ctx.agent)
}

fn read_version(mut cmd: Command) -> String {
    output(cmd.arg("--version"), VERSION_WAIT).ok()
        .and_then(|s| s.lines().next().map(|l| l.trim().to_string()))
        .unwrap_or_default()
}

// Reads `<agent> --version` now, for refresh.
fn version(ctx: &Ctx) -> String {
    let v = read_version(command(ctx, &ctx.agent));
    versions().lock().unwrap().insert(version_key(ctx), (Instant::now(), Some(v.clone())));
    v
}

// The version last read, without waiting: None until a first read ends. When the last read
// started a minute ago or more (or none did), another starts on its own thread.
fn version_seen(ctx: &Ctx) -> Option<String> {
    let key = version_key(ctx);
    let mut seen = versions().lock().unwrap();
    let last = seen.get(&key).and_then(|(_, v)| v.clone());
    if seen.get(&key).is_none_or(|(at, _)| at.elapsed() >= Duration::from_secs(60)) {
        seen.insert(key.clone(), (Instant::now(), last.clone()));
        let cmd = command(ctx, &ctx.agent);
        thread::spawn(move || {
            let v = read_version(cmd);
            versions().lock().unwrap().insert(key, (Instant::now(), Some(v)));
        });
    }
    last
}

// Sends one JSON line and waits for the line `answer` picks, until `deadline`.
fn ask(child: &mut Child, line: &Value, deadline: Instant, answer: impl Fn(&Value) -> bool + Send + 'static) -> Option<Value> {
    let out = child.stdout.take()?;
    let (tx, rx) = channel();
    thread::spawn(move || {
        for l in BufReader::new(out).lines().map_while(Result::ok) {
            let v: Value = serde_json::from_str(&l).unwrap_or(Value::Null);
            if answer(&v) {
                let _ = tx.send(v);
            }
        }
    });
    writeln!(child.stdin.as_mut()?, "{line}").ok()?;
    rx.recv_timeout(deadline.saturating_duration_since(Instant::now())).ok()
}

// Waits for the child to end until `deadline`, then kills it (its own pid only): None.
fn reap(mut child: Child, deadline: Instant) -> Option<ExitStatus> {
    while Instant::now() < deadline {
        if let Ok(Some(s)) = child.try_wait() {
            return Some(s);
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

// The `initialize` reply of a `claude -p` that is sent no message, so nothing is inferred. It
// keeps no transcript and runs none of the user's hooks: it is not a session.
fn claude_probe(ctx: &Ctx, cwd: &str, wait: Duration) -> Result<Value, String> {
    let deadline = Instant::now() + wait;
    let mut child = command(ctx, "claude")
        .args(["-p", "--verbose", "--input-format", "stream-json", "--output-format", "stream-json",
               "--no-session-persistence", "--settings", r#"{"disableAllHooks":true}"#])
        .current_dir(cwd)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| format!("start claude: {e}"))?;
    let req = json!({"type": "control_request", "request_id": "catalog-init", "request": {"subtype": "initialize"}});
    let reply = ask(&mut child, &req, deadline,
                    |v| v["type"] == "control_response" && v["response"]["request_id"] == "catalog-init");
    drop(child.stdin.take());
    reap(child, deadline);
    let r = &reply.ok_or_else(|| format!("Claude Code did not answer within {} s", wait.as_secs()))?["response"];
    if r["subtype"] != "success" {
        return Err(r["error"].as_str().unwrap_or("Claude Code refused initialize").to_string());
    }
    Ok(r["response"].clone())
}

fn pi_get_commands(ctx: &Ctx, cwd: &str, wait: Duration) -> Result<Value, String> {
    let deadline = Instant::now() + wait;
    let mut child = command(ctx, "pi")
        .args(["--mode", "rpc", "--offline", "--no-session", "--no-extensions"])
        .current_dir(cwd)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| format!("start pi: {e}"))?;
    let reply = ask(&mut child, &json!({"id": "catalog", "type": "get_commands"}), deadline,
                    |v| v["type"] == "response" && v["id"] == "catalog");
    reap(child, Instant::now());
    let r = reply.ok_or("pi did not answer get_commands")?;
    if r["success"] != true {
        return Err(r["error"].as_str().unwrap_or("pi refused get_commands").to_string());
    }
    Ok(r)
}

// Anthropic's GET /v1/models; the key goes to curl on stdin, never in its arguments.
fn anthropic_models(ctx: &Ctx, key: &str) -> Result<Value, String> {
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err("the key has characters no key has".into());
    }
    let mut child = command(ctx, "curl")
        .args(["-sS", "--max-time", "20", "--config", "-"])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| format!("start curl: {e}"))?;
    let conf = format!("url = \"https://api.anthropic.com/v1/models?limit=1000\"\nheader = \"x-api-key: {key}\"\n\
                        header = \"anthropic-version: 2023-06-01\"\n");
    let wrote = child.stdin.take().map(|mut i| i.write_all(conf.as_bytes()));
    let out = child.wait_with_output().map_err(|e| format!("curl: {e}"))?;
    if !matches!(wrote, Some(Ok(()))) {
        return Err("curl took no config".into());
    }
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|_| "no answer from api.anthropic.com".to_string())?;
    match v["data"].as_array() {
        Some(list) => Ok(json!(list.iter().map(|m| json!({"id": m["id"], "display_name": m["display_name"]})).collect::<Vec<_>>())),
        None => Err(v["error"]["message"].as_str().unwrap_or("api.anthropic.com refused the request").to_string()),
    }
}

// ---- Claude ----

fn claude_dir(ctx: &Ctx) -> PathBuf {
    ctx.home.clone().unwrap_or_else(|| crate::home().join(".claude"))
}

fn claude_json(ctx: &Ctx) -> PathBuf {
    match &ctx.home {
        Some(h) => h.join(".claude.json"),
        None => crate::home().join(".claude.json"),
    }
}

// `additionalModelOptionsCache` only; nothing else in the file is kept.
fn claude_extra(path: &Path) -> Value {
    let Some(mut all) = read_json(path) else { return json!([]) };
    let list = all.get_mut("additionalModelOptionsCache").map(Value::take).unwrap_or(Value::Null);
    json!(list.as_array().into_iter().flatten()
        .map(|m| json!({"value": m["value"], "label": m["label"], "description": m["description"]}))
        .collect::<Vec<_>>())
}

fn trim_init(models: &Value) -> Value {
    json!(models.as_array().into_iter().flatten()
        .map(|m| json!({"value": m["value"], "resolvedModel": m["resolvedModel"], "displayName": m["displayName"],
                        "description": m["description"], "supportedEffortLevels": m["supportedEffortLevels"]}))
        .collect::<Vec<_>>())
}

fn one_m(id: &str) -> bool {
    id.to_ascii_lowercase().ends_with("[1m]")
}

fn base(id: &str) -> &str {
    if one_m(id) { &id[..id.len() - 4] } else { id }
}

fn strs(v: &Value) -> Vec<Value> {
    v.as_array().into_iter().flatten().filter(|s| s.is_string()).cloned().collect()
}

/// Claude Code's own list: the `initialize` models, each extra (`additionalModelOptionsCache`)
/// after the model it is a variant of. A description "Fable 5.1 · Most capable …" gives the
/// default row its detail and a shorter label ("Fable") its full name. `resolved` is the model's
/// `resolvedModel`; an extra takes the one of the model it varies (a [1m] session reports no [1m]).
pub fn claude_models(init: &Value, extra: &Value) -> Vec<Value> {
    let init: Vec<&Value> = init.as_array().into_iter().flatten().filter(|m| m["value"].is_string()).collect();
    // (label, detail, about)
    let words = |id: &str, label: &str, m: &Value| -> (String, Option<String>, String) {
        let d = m["description"].as_str().unwrap_or("");
        let (name, rest) = d.split_once(" · ").map_or((None, d), |(n, r)| (Some(n), r));
        match name {
            Some(n) if id == "default" => (label.into(), Some(n.into()), rest.into()),
            Some(n) if n.starts_with(label) => (n.into(), None, rest.into()),
            _ => (label.into(), None, d.into()),
        }
    };
    let row = |id: &str, (label, detail, about): (String, Option<String>, String), resolved: &Value, efforts: Vec<Value>| {
        let mut r = json!({"id": id, "label": label, "group": "Claude Code offers", "efforts": efforts, "source": "offered"});
        if resolved.is_string() {
            r["resolved"] = resolved.clone();
        }
        if let Some(d) = detail.or_else(|| one_m(id).then(|| "1M".to_string())) {
            r["detail"] = json!(d);
        }
        if !about.is_empty() {
            r["about"] = json!(about);
        }
        if id == "default" {
            r["isDefault"] = json!(true);
        }
        r
    };
    let mut out: Vec<Value> = init.iter().map(|m| {
        let id = m["value"].as_str().unwrap();
        let (label, mut detail, about) = words(id, m["displayName"].as_str().unwrap_or(id), m);
        if id == "default" && detail.is_none() {
            detail = init.iter().find(|o| o["value"] != "default" && o["resolvedModel"] == m["resolvedModel"])
                .and_then(|o| o["displayName"].as_str().map(String::from));
        }
        row(id, (label, detail, about), &m["resolvedModel"], strs(&m["supportedEffortLevels"]))
    }).collect();
    for e in extra.as_array().into_iter().flatten() {
        let Some(id) = e["value"].as_str() else { continue };
        if out.iter().any(|r| r["id"] == id) {
            continue;
        }
        let of = |m: &Value| m["value"] != "default" && (m["value"] == base(id) || m["resolvedModel"] == base(id));
        let model = init.iter().rev().find(|m| of(m));
        let efforts = model.map(|m| strs(&m["supportedEffortLevels"])).unwrap_or_default();
        let resolved = model.map_or(json!(base(id)), |m| m["resolvedModel"].clone());
        // after the row it varies and any extras already placed after that row
        let at = model.and_then(|m| out.iter().position(|r| r["id"] == m["value"])).map_or(out.len(), |i| i + 1);
        let at = (at..out.len()).find(|&i| out[i]["id"].as_str().map(base) != Some(base(id))).unwrap_or(out.len());
        out.insert(at, row(id, words(id, e["label"].as_str().unwrap_or(id), e), &resolved, efforts));
    }
    out
}

// Offered, then ids the transcripts used that are not offered, then the API's other versions.
fn compose_claude(c: &Value) -> Vec<Value> {
    let mut out = claude_models(&c["init"], &c["extra"]);
    let mut known: HashSet<String> = HashSet::new();
    for m in c["init"].as_array().into_iter().flatten().chain(c["extra"].as_array().into_iter().flatten()) {
        for k in [&m["value"], &m["resolvedModel"]] {
            if let Some(k) = k.as_str() {
                known.insert(base(k).to_string());
                known.insert(k.to_string());
            }
        }
    }
    for id in c["used"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        if known.insert(id.to_string()) {
            out.push(json!({"id": id, "label": id, "group": "Used here before", "efforts": [], "source": "used"}));
        }
    }
    for m in c["api"].as_array().into_iter().flatten() {
        let Some(id) = m["id"].as_str() else { continue };
        if known.insert(id.to_string()) {
            out.push(json!({"id": id, "label": m["display_name"].as_str().unwrap_or(id), "group": "All versions",
                            "efforts": [], "source": "api"}));
        }
    }
    out
}

// `"model":"claude-…"` ids in the last 64 KB of the newest 300 transcripts, newest first.
fn used_ids(projects: &Path) -> Vec<String> {
    let mut files: Vec<(SystemTime, PathBuf)> = Vec::new();
    for dir in fs::read_dir(projects).into_iter().flatten().flatten() {
        for f in fs::read_dir(dir.path()).into_iter().flatten().flatten() {
            let p = f.path();
            if p.extension().is_some_and(|e| e == "jsonl") {
                if let Ok(t) = f.metadata().and_then(|m| m.modified()) {
                    files.push((t, p));
                }
            }
        }
    }
    files.sort_by(|a, b| b.0.cmp(&a.0));
    let mut out: Vec<String> = Vec::new();
    let pat = b"\"model\":\"claude-";
    for (_, p) in files.into_iter().take(300) {
        let Ok(mut file) = File::open(&p) else { continue };
        let len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut buf = Vec::new();
        if file.seek(SeekFrom::Start(len.saturating_sub(65536))).and_then(|_| file.read_to_end(&mut buf)).is_err() {
            continue;
        }
        let mut i = 0;
        while let Some(at) = buf[i..].windows(pat.len()).position(|w| w == pat) {
            let start = i + at + 9;
            let end = buf[start..].iter().position(|&b| b == b'"').map_or(buf.len(), |e| start + e);
            let id = String::from_utf8_lossy(&buf[start..end]).into_owned();
            if id != "<synthetic>" && id.len() < 80 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._[]".contains(&b))
                && !out.contains(&id) {
                out.push(id);
            }
            i = end;
        }
    }
    out
}

/// Claude Code's `initialize` commands: built-in ones are commands, the rest skills.
pub fn claude_commands(raw: &Value) -> Vec<Value> {
    raw.as_array().into_iter().flatten().filter(|c| c["name"].is_string()).map(|c| {
        let mut r = json!({"name": c["name"], "description": c["description"].as_str().unwrap_or(""),
                           "kind": if c["builtin"] == true { "command" } else { "skill" }});
        if let Some(h) = c["argumentHint"].as_str().filter(|h| !h.is_empty()) {
            r["hint"] = json!(h);
        }
        if c["aliases"].as_array().is_some_and(|a| !a.is_empty()) {
            r["aliases"] = c["aliases"].clone();
        }
        r
    }).collect()
}

// ---- Codex ----

// `key = "value"` before the first table of a TOML file.
fn toml_top(text: &str, key: &str) -> Option<String> {
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            break;
        }
        let Some(v) = line.strip_prefix(key).map(str::trim_start).and_then(|r| r.strip_prefix('=')) else { continue };
        return v.trim().strip_prefix('"')?.split('"').next().map(String::from);
    }
    None
}

/// models_cache.json (or the bundled list) by priority, hidden models left out. The default is
/// config.toml's `model`, else the first.
pub fn codex_models(list: &Value, default: Option<&str>) -> Vec<Value> {
    let mut ms: Vec<&Value> = list["models"].as_array().into_iter().flatten()
        .filter(|m| m["slug"].is_string() && m["visibility"] != "hide").collect();
    ms.sort_by_key(|m| m["priority"].as_i64().unwrap_or(i64::MAX));
    let default = default.filter(|d| ms.iter().any(|m| m["slug"] == *d)).or_else(|| ms.first().and_then(|m| m["slug"].as_str()));
    ms.iter().map(|m| {
        let id = m["slug"].as_str().unwrap();
        let efforts: Vec<&Value> = m["supported_reasoning_levels"].as_array().into_iter().flatten().map(|l| &l["effort"]).filter(|e| e.is_string()).collect();
        let mut r = json!({"id": id, "label": m["display_name"].as_str().unwrap_or(id), "group": "Codex", "efforts": efforts, "source": "offered"});
        if let Some(d) = m["description"].as_str().filter(|d| !d.is_empty()) {
            r["about"] = json!(d);
        }
        if m["default_reasoning_level"].is_string() {
            r["defaultEffort"] = m["default_reasoning_level"].clone();
        }
        if Some(id) == default {
            r["isDefault"] = json!(true);
        }
        r
    }).collect()
}

/// `skills/list`: the enabled skills of every folder it answered for.
pub fn codex_skills(raw: &Value) -> Vec<Value> {
    let mut seen = HashSet::new();
    raw["data"].as_array().into_iter().flatten().flat_map(|e| e["skills"].as_array().into_iter().flatten())
        .filter(|s| s["enabled"] != false && s["name"].as_str().is_some_and(|n| seen.insert(n.to_string())))
        .map(|s| {
            let d = [&s["interface"]["shortDescription"], &s["shortDescription"], &s["description"]]
                .into_iter().find_map(|d| d.as_str().filter(|d| !d.is_empty())).unwrap_or("");
            json!({"name": s["name"], "description": d, "kind": "skill"})
        }).collect()
}

// ---- pi and opencode ----

/// Provider names ("openrouter" → "OpenRouter") from opencode's copy of models.dev, when there is one.
pub fn provider_names() -> HashMap<String, String> {
    let cache = env::var_os("XDG_CACHE_HOME").map(PathBuf::from).unwrap_or_else(|| crate::home().join(".cache"));
    let Some(Value::Object(all)) = read_json(&cache.join("opencode/models.json")) else { return HashMap::new() };
    all.into_iter().filter_map(|(k, v)| Some((k, v["name"].as_str()?.to_string()))).collect()
}

fn provider(id: &str, names: &HashMap<String, String>) -> String {
    names.get(id).cloned().unwrap_or_else(|| id.to_string())
}

/// `pi --list-models`: provider, model, context, max-out, thinking, images.
pub fn pi_models(table: &str, default: Option<(&str, &str)>, names: &HashMap<String, String>) -> Vec<Value> {
    table.lines().skip_while(|l| !l.starts_with("provider")).skip(1).filter_map(|l| {
        let cols: Vec<&str> = l.split_whitespace().collect();
        if cols.len() < 6 {
            return None;
        }
        let (prov, model, thinking) = (cols[0], cols[1..cols.len() - 4].join(" "), cols[cols.len() - 2]);
        let efforts: Vec<&str> = if thinking == "yes" { PI_EFFORTS.to_vec() } else { vec![] };
        let mut r = json!({"id": format!("{prov}/{model}"), "label": model, "group": provider(prov, names),
                           "efforts": efforts, "source": "offered"});
        if default == Some((prov, model.as_str())) {
            r["isDefault"] = json!(true);
        }
        Some(r)
    }).collect()
}

/// `get_commands`: `skill:<name>` entries are skills.
pub fn pi_commands(raw: &Value) -> Vec<Value> {
    let list = if raw["data"].is_object() { &raw["data"]["commands"] } else { &raw["commands"] };
    list.as_array().into_iter().flatten().filter_map(|c| {
        let name = c["name"].as_str()?;
        let skill = name.starts_with("skill:") || c["source"] == "skill";
        Some(json!({"name": name, "description": c["description"].as_str().unwrap_or(""), "kind": if skill { "skill" } else { "command" }}))
    }).collect()
}

/// `opencode models --verbose`: a `provider/model` line, then the model as indented JSON.
/// Variant names are the efforts.
pub fn opencode_models(text: &str, names: &HashMap<String, String>) -> Vec<Value> {
    let mut out = Vec::new();
    let mut block: Option<String> = None;
    for line in text.lines() {
        if line.starts_with('{') {
            block = Some(String::new());
        }
        let Some(b) = block.as_mut() else { continue };
        b.push_str(line);
        b.push('\n');
        if line != "}" {
            continue;
        }
        let m: Value = serde_json::from_str(&block.take().unwrap()).unwrap_or(Value::Null);
        let (Some(prov), Some(id)) = (m["providerID"].as_str(), m["id"].as_str()) else { continue };
        let mut efforts: Vec<&String> = m["variants"].as_object().map(|v| v.keys().collect()).unwrap_or_default();
        efforts.sort_by_key(|e| (EFFORTS.iter().position(|x| *x == e.as_str()).unwrap_or(EFFORTS.len()), e.to_string()));
        out.push(json!({"id": format!("{prov}/{id}"), "label": m["name"].as_str().unwrap_or(id), "group": provider(prov, names),
                        "efforts": efforts, "source": "offered"}));
    }
    out
}

/// opencode 2's `GET /api/model`: each model with its variants (`[{id, settings}]`) as the efforts; a model it
/// turned off is left out.
pub fn opencode2_models(list: &[Value], names: &HashMap<String, String>) -> Vec<Value> {
    list.iter().filter(|m| m["enabled"] != false).filter_map(|m| {
        let (prov, id) = (m["providerID"].as_str()?, m["id"].as_str()?);
        let mut efforts: Vec<String> = m["variants"].as_array().into_iter().flatten().filter_map(|v| v["id"].as_str().map(str::to_string)).collect();
        efforts.sort_by_key(|e| (EFFORTS.iter().position(|x| *x == e.as_str()).unwrap_or(EFFORTS.len()), e.clone()));
        Some(json!({"id": format!("{prov}/{id}"), "label": m["name"].as_str().unwrap_or(id), "group": provider(prov, names),
                    "efforts": efforts, "source": "offered"}))
    }).collect()
}

/// `GET /command`: entries from skills are skills.
pub fn opencode_commands(raw: &Value) -> Vec<Value> {
    raw.as_array().into_iter().flatten().filter(|c| c["name"].is_string()).map(|c| {
        json!({"name": c["name"], "description": c["description"].as_str().unwrap_or(""),
               "kind": if c["source"] == "skill" { "skill" } else { "command" }})
    }).collect()
}

// ---- sha1, for the cwd in a commands file's name ----

pub(crate) fn sha1_hex(data: &[u8]) -> String {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let t = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(*wi);
            (e, d, c, b, a) = (d, c, b.rotate_left(30), a, t);
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e]) {
            *x = x.wrapping_add(y);
        }
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/catalog").join(name)
    }

    fn fixture_json(name: &str) -> Value {
        read_json(&fixture(name)).unwrap()
    }

    fn scratch(name: &str) -> PathBuf {
        let d = env::temp_dir().join(format!("agent-host-catalog-{}-{name}", process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn names() -> HashMap<String, String> {
        [("openrouter", "OpenRouter"), ("opencode", "OpenCode Zen"), ("modal", "Modal")].map(|(a, b)| (a.into(), b.into())).into()
    }

    fn find<'a>(list: &'a [Value], id: &str) -> &'a Value {
        list.iter().find(|m| m["id"] == id).unwrap_or_else(|| panic!("no {id}"))
    }

    fn ids<'a>(list: &'a [Value], source: &str) -> Vec<&'a str> {
        list.iter().filter(|m| m["source"] == source).filter_map(|m| m["id"].as_str()).collect()
    }

    fn alive(pid_file: &Path) -> bool {
        Path::new(&format!("/proc/{}", fs::read_to_string(pid_file).unwrap().trim())).exists()
    }

    // The agents are the fakes in tests/fixtures/catalog/bin; they note what they were given in `out`.
    fn fake(agent: &str, home: Option<PathBuf>, cwd: &Path, out: &Path, mode: &str) -> Ctx {
        let path = format!("{}:{}", fixture("bin").display(), env::var("PATH").unwrap_or_default());
        Ctx { agent: agent.into(), account: format!("test-{agent}"), home, cwd: Some(cwd.to_string_lossy().into()),
              env: vec![("PATH".into(), path), ("FAKE_OUT".into(), out.to_string_lossy().into()), ("FAKE_MODE".into(), mode.into())],
              anthropic_key: None }
    }

    #[test]
    fn sha1_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(sha1_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"), "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
    }

    #[test]
    fn claude_offered() {
        let extra = claude_extra(&fixture("claude-json.json"));
        assert!(!extra.to_string().contains("someone@example.com"), "only additionalModelOptionsCache is kept");
        let ms = claude_models(&fixture_json("claude-init.json")["models"], &extra);
        assert_eq!(ms.len(), 13);
        assert!(ms.iter().all(|m| m["group"] == "Claude Code offers" && m["source"] == "offered"));
        let d = &ms[0];
        assert_eq!((d["id"].as_str(), d["isDefault"].as_bool(), d["detail"].as_str()), (Some("default"), Some(true), Some("Opus 5.5")));
        assert_eq!(d["about"], "Best for everyday, complex tasks");
        assert_eq!(ms.iter().filter(|m| m["isDefault"] == true).count(), 1);
        let f = find(&ms, "claude-fable-5-1[1m]");
        assert_eq!((f["label"].as_str(), f["detail"].as_str()), (Some("Fable 5.1"), Some("1M")));
        assert_eq!(f["about"], "Most capable for your hardest and longest-running tasks");
        assert_eq!(f["efforts"], json!(["low", "medium", "high", "xhigh", "max"]));
        let at = ms.iter().position(|m| m["id"] == "fable").unwrap();
        assert_eq!(ms[at + 1]["id"], "claude-fable-5-1[1m]", "the 1M row follows the model it varies");
        assert_eq!(find(&ms, "opus")["label"], "Opus 5.5");
        assert_eq!(find(&ms, "opus")["about"], "For complex work and everyday tasks");
        assert_eq!(find(&ms, "haiku")["efforts"], json!([]));
        assert_eq!(find(&ms, "claude-opus-4-6")["efforts"], json!(["low", "medium", "high", "max"]));
        // what a session started with each row reports in system/init
        assert_eq!(d["resolved"], "claude-opus-5-5");
        assert_eq!(find(&ms, "fable")["resolved"], "claude-fable-5-1");
        assert_eq!(f["resolved"], "claude-fable-5-1");
        assert_eq!(find(&ms, "haiku")["resolved"], "claude-haiku-4-5-20251001");
        assert!(ms.iter().all(|m| m["resolved"].is_string()));
    }

    #[test]
    fn claude_used_and_api() {
        let home = scratch("used");
        let p = home.join("projects/-tmp-a");
        fs::create_dir_all(&p).unwrap();
        // the first line is more than 64 KB from the end, so it is not read
        let text = format!("{{\"model\":\"claude-ancient-1\"}}\n{}\n{}\n{}\n{}\n", "x".repeat(70000),
                           r#"{"message":{"model":"claude-opus-4-1"}}"#, r#"{"message":{"model":"<synthetic>"}}"#,
                           r#"{"message":{"model":"claude-opus-5-5"}}"#);
        fs::write(p.join("a.jsonl"), text).unwrap();
        fs::write(p.join("b.txt"), r#"{"model":"claude-not-a-transcript"}"#).unwrap();
        let used = used_ids(&home.join("projects"));
        assert_eq!(used, ["claude-opus-4-1", "claude-opus-5-5"]);
        let api = fixture_json("anthropic-models.json")["data"].clone();
        let c = json!({"init": trim_init(&fixture_json("claude-init.json")["models"]),
                       "extra": claude_extra(&fixture("claude-json.json")), "used": used, "api": api});
        let ms = compose_claude(&c);
        // claude-opus-5-5 is offered: "opus" resolves to it
        assert_eq!(ids(&ms, "used"), ["claude-opus-4-1"]);
        assert_eq!(find(&ms, "claude-opus-4-1")["group"], "Used here before");
        assert_eq!(ids(&ms, "api"), ["claude-opus-4-1-20250805", "claude-3-5-haiku-20241022"]);
        assert_eq!(find(&ms, "claude-3-5-haiku-20241022")["label"], "Claude Haiku 3.5");
        assert_eq!(find(&ms, "claude-3-5-haiku-20241022")["group"], "All versions");
        assert!(ms.iter().filter(|m| m["source"] != "offered").all(|m| m.get("resolved").is_none()));
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn claude_command_kinds() {
        let cs = claude_commands(&fixture_json("claude-init.json")["commands"]);
        assert_eq!(cs.len(), 150);
        let c = cs.iter().find(|c| c["name"] == "clear").unwrap();
        assert_eq!((c["kind"].as_str(), &c["aliases"]), (Some("command"), &json!(["reset", "new"])));
        let s = cs.iter().find(|c| c["name"] == "align-grid").unwrap();
        assert_eq!(s["kind"], "skill");
        assert!(s.get("hint").is_none() && s.get("aliases").is_none());
        assert_eq!(cs.iter().find(|c| c["name"] == "loop").unwrap()["hint"], "[interval] [prompt]");
    }

    #[test]
    fn codex_list() {
        let cache = fixture_json("codex-models_cache.json");
        assert!(cache.get("identity").is_none());
        let ms = codex_models(&cache, Some("gpt-6-luna"));
        // by priority, hidden models (gpt-reserve, codex-auto-review) left out
        assert_eq!(ids(&ms, "offered"), ["gpt-6-luna", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]);
        assert_eq!(find(&ms, "gpt-5.6-terra")["efforts"], json!(["low", "medium", "high", "xhigh", "max", "ultra"]));
        assert_eq!(find(&ms, "gpt-5.5")["efforts"], json!(["low", "medium", "high", "xhigh"]));
        assert_eq!(find(&ms, "gpt-5.5")["defaultEffort"], "medium");
        assert_eq!(find(&ms, "gpt-5.5")["label"], "GPT-5.5");
        assert_eq!(find(&ms, "gpt-5.6-terra")["about"], "Older balanced model for straightforward work.");
        assert_eq!(find(&ms, "gpt-6-luna")["isDefault"], true);
        assert_eq!(ms.iter().filter(|m| m["isDefault"] == true).count(), 1);
        assert_eq!(codex_models(&cache, Some("gpt-5.5"))[3]["isDefault"], true);
        // no model in config.toml, or a hidden one: the first
        assert_eq!(codex_models(&cache, None)[0]["isDefault"], true);
        assert_eq!(codex_models(&cache, Some("gpt-reserve"))[0]["isDefault"], true);
        let toml = "model = \"gpt-6-luna\"\nmodel_reasoning_effort = \"medium\"\n\n[profiles.x]\nmodel = \"other\"\n";
        assert_eq!(toml_top(toml, "model").as_deref(), Some("gpt-6-luna"));
        assert_eq!(toml_top("[x]\nmodel = \"a\"\n", "model"), None);
        let sk = codex_skills(&fixture_json("codex-skills.json"));
        assert_eq!(sk.len(), 7);
        assert!(sk.iter().all(|s| s["kind"] == "skill"));
        let s = sk.iter().find(|s| s["name"] == "imagegen").unwrap();
        assert_eq!(s["description"], "Generate or edit images for websites, games, and more");
    }

    #[test]
    fn pi_list() {
        let table = fs::read_to_string(fixture("pi-list-models.txt")).unwrap();
        let ms = pi_models(&table, Some(("openrouter", "~anthropic/claude-opus-latest")), &names());
        assert_eq!(ms.len(), 414);
        let m = find(&ms, "modal/deepseek-ai/DeepSeek-V4.1-Flash");
        assert_eq!((m["label"].as_str(), m["group"].as_str()), (Some("deepseek-ai/DeepSeek-V4.1-Flash"), Some("Modal")));
        assert_eq!(m["efforts"], json!(["off", "minimal", "low", "medium", "high", "xhigh", "max"]));
        assert_eq!(find(&ms, "openrouter/amazon/nova-lite-v1")["efforts"], json!([]));
        assert_eq!(find(&ms, "openrouter/amazon/nova-lite-v1")["group"], "OpenRouter");
        assert_eq!(find(&ms, "openrouter/~anthropic/claude-opus-latest")["isDefault"], true);
        assert_eq!(ms.iter().filter(|m| m["isDefault"] == true).count(), 1);
        let cs = pi_commands(&fixture_json("pi-commands.json"));
        assert_eq!(cs.len(), 82);
        assert_eq!(cs.iter().find(|c| c["name"] == "skill:align-grid").unwrap()["kind"], "skill");
        assert_eq!(cs.iter().find(|c| c["name"] == "llama").unwrap()["kind"], "command");
    }

    #[test]
    fn opencode_list() {
        let ms = opencode_models(&fs::read_to_string(fixture("opencode-models.txt")).unwrap(), &names());
        assert_eq!(ms.len(), 30);
        let b = find(&ms, "opencode/big-pickle");
        assert_eq!((b["label"].as_str(), b["group"].as_str()), (Some("Big Pickle"), Some("OpenCode Zen")));
        assert_eq!(b["efforts"], json!([]));
        assert_eq!(find(&ms, "opencode/fledge-alpha-free")["efforts"], json!(["low", "high", "max"]));
        assert_eq!(find(&ms, "openrouter/~openai/gpt-luna-latest")["efforts"], json!(["none", "low", "medium", "high", "xhigh", "max"]));
        assert_eq!(find(&ms, "opencode/muse-spark-1.3-contributor-free")["efforts"], json!(["minimal", "low", "medium", "high", "xhigh"]));
        assert_eq!(find(&ms, "modal/someone--ep-glm-5-3-server.us-west.modal.direct")["group"], "Modal");
        let cs = opencode_commands(&fixture_json("opencode-commands.json"));
        assert_eq!(cs.len(), 99);
        assert_eq!(cs.iter().find(|c| c["name"] == "init").unwrap()["kind"], "command");
        assert_eq!(cs.iter().find(|c| c["name"] == "context7-mcp").unwrap()["kind"], "skill");
    }

    #[test]
    fn notes_fill_the_cache() {
        let (root, out, cwd) = (scratch("notes"), scratch("notes-out"), scratch("notes-cwd"));
        let ctx = fake("claude", None, &cwd, &out, "");
        assert!(cached_in(&root, &ctx).is_none());
        let init = fixture_json("claude-init.json");
        let reply = json!({"models": init["models"], "commands": init["commands"], "account": {"email": "someone@example.com"}});
        note_claude_init_in(&root, &ctx.account, &ctx.folder(), &reply);
        let c = cached_in(&root, &ctx).unwrap();
        assert_eq!(c["stale"], true, "never refreshed");
        assert_eq!((c["ev"].as_str(), c["agent"].as_str(), c["account"].as_str()), (Some("catalog"), Some("claude"), Some("test-claude")));
        assert_eq!(c["models"].as_array().unwrap().len(), 12);
        assert_eq!(c["commands"].as_array().unwrap().len(), 150);
        for f in fs::read_dir(&root).unwrap().flatten() {
            assert!(!fs::read_to_string(f.path()).unwrap().contains("someone@example.com"), "the reply's account is not kept");
        }
        assert_eq!(c["cwd"], ctx.folder());
        let other = fake("claude", None, Path::new("/nowhere"), &out, "");
        assert_eq!(cached_in(&root, &other).unwrap()["commands"], json!([]));
        // no folder asked: $HOME's commands, and no cwd
        let mut home = fake("claude", None, &cwd, &out, "");
        home.cwd = None;
        note_claude_init_in(&root, &home.account, &home_str(), &json!({"commands": [{"name": "h", "description": ""}]}));
        let c = cached_in(&root, &home).unwrap();
        assert!(c.get("cwd").is_none());
        assert_eq!(c["commands"][0]["name"], "h");
        // Codex skills arrive for a folder; Codex has no models until a refresh
        let cx = fake("codex", None, Path::new("/w"), &out, "");
        note_commands_in(&root, "codex", &cx.account, "/w", &fixture_json("codex-skills.json"));
        assert!(root.join(format!("test-codex-{}.json", &sha1_hex(b"/w")[..12])).exists());
        assert!(cached_in(&root, &cx).is_none());
        for d in [root, out, cwd] {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[test]
    fn claude_refresh() {
        let (root, out, cwd, home) = (scratch("cr"), scratch("cr-out"), scratch("cr-cwd"), scratch("cr-home"));
        fs::copy(fixture("claude-json.json"), home.join(".claude.json")).unwrap();
        fs::create_dir_all(home.join("projects/p")).unwrap();
        fs::write(home.join("projects/p/s.jsonl"), "{\"model\":\"claude-opus-4-1\"}\n").unwrap();
        let mut ctx = fake("claude", Some(home.clone()), &cwd, &out, "");
        ctx.anthropic_key = Some("sk-ant-test-0000000000".into());
        let c = refresh_in(&root, &ctx, WAIT).unwrap();
        assert_eq!(c["stale"], false);
        assert_eq!(c["cwd"], cwd.to_str().unwrap(), "the folder the commands are for");
        let ms = c["models"].as_array().unwrap();
        assert_eq!(find(ms, "claude-fable-5-1[1m]")["label"], "Fable 5.1");
        assert_eq!(ids(ms, "used"), ["claude-opus-4-1"]);
        assert_eq!(ids(ms, "api"), ["claude-opus-4-1-20250805", "claude-3-5-haiku-20241022"]);
        assert_eq!(c["commands"].as_array().unwrap().len(), 150);
        // what the probe was given
        let args = fs::read_to_string(out.join("claude.args")).unwrap();
        assert!(args.contains("--no-session-persistence") && args.contains("disableAllHooks"));
        assert_eq!(fs::read_to_string(out.join("claude.cwd")).unwrap().trim(), cwd.to_str().unwrap());
        let env_names = fs::read_to_string(out.join("claude.env")).unwrap();
        assert!(env_names.lines().any(|l| l == "CLAUDE_CONFIG_DIR"));
        assert!(!env_names.lines().any(|l| l == "CLAUDECODE" || l.ends_with("_API_KEY")));
        // it ended when its stdin closed, and was reaped
        assert!(!alive(&out.join("claude.pid")));
        // the key went to curl on stdin only
        assert!(!fs::read_to_string(out.join("curl.args")).unwrap().contains("sk-ant"));
        assert!(fs::read_to_string(out.join("curl.config")).unwrap().contains("x-api-key: sk-ant-test-0000000000"));
        assert!(!fs::read_to_string(models_file(&root, &ctx.account)).unwrap().contains("sk-ant"));
        // fresh until the agent's version changes
        assert_eq!(cached_in(&root, &ctx).unwrap()["stale"], false);
        let mut f = read_json(&models_file(&root, &ctx.account)).unwrap();
        f["version"] = json!("9.9.8 (Claude Code)");
        write_json(&models_file(&root, &ctx.account), &f);
        assert_eq!(cached_in(&root, &ctx).unwrap()["stale"], true);
        // a folder whose commands were never read is stale too
        let elsewhere = fake("claude", Some(home.clone()), &home, &out, "");
        let mut f = read_json(&models_file(&root, &ctx.account)).unwrap();
        f["version"] = json!("9.9.9 (Claude Code)");
        write_json(&models_file(&root, &ctx.account), &f);
        assert_eq!(cached_in(&root, &elsewhere).unwrap()["stale"], true);
        for d in [root, out, cwd, home] {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[test]
    fn claude_probe_gives_up() {
        let (root, out, cwd) = (scratch("cg"), scratch("cg-out"), scratch("cg-cwd"));
        let ctx = fake("claude", None, &cwd, &out, "silent");
        let t = Instant::now();
        assert!(refresh_in(&root, &ctx, Duration::from_secs(2)).is_err());
        assert!(t.elapsed() < Duration::from_secs(4));
        assert!(!alive(&out.join("claude.pid")), "killed at the deadline");
        assert!(cached_in(&root, &ctx).is_none());
        for d in [root, out, cwd] {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[test]
    fn codex_pi_opencode_refresh() {
        let (root, out, cwd) = (scratch("rf"), scratch("rf-out"), scratch("rf-cwd"));
        // Codex with no models_cache.json: the bundled list; config.toml names the default
        let cx_home = scratch("rf-codex");
        fs::write(cx_home.join("config.toml"), "model = \"gpt-5.5\"\n").unwrap();
        let cx = fake("codex", Some(cx_home.clone()), &cwd, &out, "");
        let c = refresh_in(&root, &cx, WAIT).unwrap();
        assert_eq!(find(c["models"].as_array().unwrap(), "gpt-5.5")["isDefault"], true);
        assert_eq!(c["commands"], json!([]));
        // pi: its settings name the default; get_commands' process is killed once it answers
        let pi_home = scratch("rf-pi");
        fs::write(pi_home.join("settings.json"), r#"{"defaultProvider":"modal","defaultModel":"deepseek-ai/DeepSeek-V4.1-Flash"}"#).unwrap();
        let pi = fake("pi", Some(pi_home.clone()), &cwd, &out, "");
        let c = refresh_in(&root, &pi, WAIT).unwrap();
        assert_eq!(c["models"].as_array().unwrap().len(), 414);
        assert_eq!(find(c["models"].as_array().unwrap(), "modal/deepseek-ai/DeepSeek-V4.1-Flash")["isDefault"], true);
        assert_eq!(c["commands"].as_array().unwrap().len(), 82);
        assert!(!alive(&out.join("pi.pid")));
        assert_eq!(cached_in(&root, &pi).unwrap()["stale"], false);
        let oc = fake("opencode", None, &cwd, &out, "");
        let c = refresh_in(&root, &oc, WAIT).unwrap();
        assert_eq!(c["models"].as_array().unwrap().len(), 30);
        assert_eq!(cached_in(&root, &oc).unwrap()["stale"], false);
        for d in [root, out, cwd, cx_home, pi_home] {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[test]
    fn hung_commands_are_killed() {
        let (root, out, cwd, own) = (scratch("hang"), scratch("hang-out"), scratch("hang-cwd"), scratch("hang-path"));
        // its own PATH, so its versions are not the other tests' opencode
        let mut ctx = fake("opencode", None, &cwd, &out, "hang");
        ctx.env[0].1 = format!("{}:{}", own.display(), ctx.env[0].1);
        let t = Instant::now();
        let e = refresh_in(&root, &ctx, Duration::from_secs(2)).unwrap_err();
        assert!(e.contains("did not finish"), "{e}");
        assert!(t.elapsed() < Duration::from_secs(4));
        assert!(!alive(&out.join("opencode.pid")), "killed at the deadline");
        // cached() does not wait for --version; the read on its thread is killed at 5 s
        write_json(&models_file(&root, &ctx.account), &json!({"agent": "opencode", "version": "0.0.1", "refreshed": crate::now_ms(), "models": []}));
        versions().lock().unwrap().remove(&version_key(&ctx));
        ctx.env[2].1 = "hangversion".into();
        let t = Instant::now();
        assert_eq!(cached_in(&root, &ctx).unwrap()["stale"], false, "no version read yet");
        assert!(t.elapsed() < Duration::from_millis(500), "{:?}", t.elapsed());
        let pid = out.join("opencode-version.pid");
        while !pid.exists() && t.elapsed() < Duration::from_secs(3) {
            thread::sleep(Duration::from_millis(20));
        }
        while alive(&pid) && t.elapsed() < Duration::from_secs(8) {
            thread::sleep(Duration::from_millis(100));
        }
        assert!(!alive(&pid), "killed after {:?}", t.elapsed());
        thread::sleep(Duration::from_millis(100));
        // the failed read ("") differs from the cache's version
        assert_eq!(cached_in(&root, &ctx).unwrap()["stale"], true);
        for d in [root, out, cwd, own] {
            let _ = fs::remove_dir_all(d);
        }
    }

    // The real Claude Code, signed in as this machine's user: cargo test -- --ignored --nocapture real_claude_probe
    #[test]
    #[ignore = "runs the real claude"]
    fn real_claude_probe() {
        let (root, cwd) = (scratch("real"), scratch("real-cwd"));
        let ctx = Ctx { agent: "claude".into(), account: "real-probe".into(), home: None, env: vec![],
                        cwd: Some(cwd.to_string_lossy().into()), anthropic_key: None };
        let t = Instant::now();
        let c = refresh_in(&root, &ctx, WAIT).unwrap();
        eprintln!("refresh (probe included) took {:?}", t.elapsed());
        for m in c["models"].as_array().unwrap() {
            eprintln!("  {:<24} {:<22} {:<10} {:<26} {:<18} {}", m["id"].as_str().unwrap(), m["label"].as_str().unwrap(),
                      m["detail"].as_str().unwrap_or(""), m["resolved"].as_str().unwrap_or(""), m["group"].as_str().unwrap(), m["efforts"]);
        }
        eprintln!("{} commands, cwd {}", c["commands"].as_array().unwrap().len(), c["cwd"]);
        for d in [root, cwd] {
            let _ = fs::remove_dir_all(d);
        }
    }

    // The real codex, pi and opencode: cargo test -- --ignored --nocapture real_other_agents
    #[test]
    #[ignore = "runs the real agents"]
    fn real_other_agents() {
        let (root, cwd) = (scratch("real-others"), scratch("real-others-cwd"));
        for agent in ["codex", "pi", "opencode"] {
            let ctx = Ctx { agent: agent.into(), account: format!("real-{agent}"), home: None, env: vec![],
                            cwd: Some(cwd.to_string_lossy().into()), anthropic_key: None };
            let t = Instant::now();
            let c = refresh_in(&root, &ctx, WAIT).unwrap();
            let ms = c["models"].as_array().unwrap();
            let groups: HashSet<&str> = ms.iter().filter_map(|m| m["group"].as_str()).collect();
            eprintln!("{agent}: {} models in {:?}, groups {groups:?}, {} commands, default {:?}", ms.len(), t.elapsed(),
                      c["commands"].as_array().unwrap().len(), ms.iter().find(|m| m["isDefault"] == true).map(|m| &m["id"]));
            assert!(!ms.is_empty());
        }
        for d in [root, cwd] {
            let _ = fs::remove_dir_all(d);
        }
    }
}
