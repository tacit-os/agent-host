//! Agent accounts, API keys and run defaults, and the sign-ins and installs behind them (plan
//! §2.12-§2.15, §3.2, §3.3, §4).
//!
//! Each agent has a Personal account, its own folder (~/.claude, ~/.codex, ~/.pi/agent, opencode's
//! own), which is never moved, edited or removed. An added account has a folder of its own under
//! ~/.local/share/agent-host/accounts/<id> (mode 0700) with Personal's shared files linked in,
//! passed at spawn as CLAUDE_CONFIG_DIR, CODEX_HOME or PI_CODING_AGENT_DIR; an added opencode
//! account is a key only. The index is ~/.config/agent-host/accounts.json (AGENT_HOST_CONFIG
//! overrides the folder), 0600, written whole through a rename, and holds no secret: keys live in
//! the keyring (keys.rs).
//!
//! Every agent process is built by `command`: the key and account variables of the host's own env
//! are removed, then the account's folder and the keys it calls for are added. Claude Code takes
//! its key on fd 3 (keys::pass_fd3), never in its env.
//!
//! Clients that sent `accounts {watch:true}` get the `accounts`, `signin`, `install` and `catalog`
//! pushes; no other connection does.

use crate::{catalog, install, keys, now_ms, signin};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

pub const AGENTS: [&str; 4] = ["claude", "codex", "pi", "opencode"];
const CHECK_EVERY: Duration = Duration::from_secs(600);

// id, name, pi's variable, opencode's variable (plan §2.14)
const PROVIDERS: [(&str, &str, &str, &str); 4] = [
    ("anthropic", "Anthropic", "ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"),
    ("openai", "OpenAI", "OPENAI_API_KEY", "OPENAI_API_KEY"),
    ("openrouter", "OpenRouter", "OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
    ("google", "Google", "GEMINI_API_KEY", "GOOGLE_GENERATIVE_AI_API_KEY"),
];

// What Personal shares with an added account, by link (plan §2.13).
fn shared_names(agent: &str) -> &'static [&'static str] {
    match agent {
        "claude" => &["settings.json", "CLAUDE.md", "skills", "agents", "commands", "plugins", "projects"],
        "codex" => &["config.toml", "AGENTS.md", "sessions"],
        "pi" => &["settings.json", "models.json"],
        _ => &[],
    }
}

// The credential files an account's folder holds, deleted outright when it is removed.
fn credential_files(agent: &str) -> &'static [&'static str] {
    match agent {
        "claude" => &[".credentials.json"],
        "codex" | "pi" => &["auth.json"],
        _ => &[],
    }
}

fn personal_dir(agent: &str) -> PathBuf {
    let h = crate::home();
    match agent {
        "claude" => h.join(".claude"),
        "codex" => h.join(".codex"),
        "pi" => h.join(".pi/agent"),
        _ => h.join(".local/share/opencode"),
    }
}

struct State {
    file: Value,                          // accounts.json
    signin: Option<Value>,                // the sign-in under way, as its latest step
    logins: HashMap<String, signin::Login>,
    install: Option<Value>,               // the install under way, as its latest step
    installs: HashMap<String, install::Install>,
    checking: HashSet<String>,            // accounts whose sign-in is being read
    agents: Value,                        // install::detect per agent
    node: bool,
    keyring: &'static str,
    refreshing: HashSet<String>,          // catalog reads under way
    checked_all: Option<Instant>,
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();
static CHECKED: Condvar = Condvar::new();
static SHARED: OnceLock<crate::Shared> = OnceLock::new();

fn st() -> MutexGuard<'static, State> {
    STATE.get_or_init(|| Mutex::new(State {
        file: load(), signin: None, logins: HashMap::new(), install: None, installs: HashMap::new(), checking: HashSet::new(),
        agents: json!({}), node: false, keyring: "absent", refreshing: HashSet::new(), checked_all: None,
    })).lock().unwrap()
}

// ── the file ─────────────────────────────────────────────────────────────────────────────────

fn config_dir() -> PathBuf {
    std::env::var_os("AGENT_HOST_CONFIG").map(PathBuf::from).unwrap_or_else(|| crate::home().join(".config/agent-host"))
}

fn accounts_root() -> PathBuf {
    crate::home().join(".local/share/agent-host/accounts")
}

fn load() -> Value {
    let mut f: Value = fs::read(config_dir().join("accounts.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or(json!({}));
    if !f.is_object() {
        f = json!({});
    }
    f["version"] = json!(1);
    for (k, empty) in [("accounts", json!([])), ("keys", json!([])), ("default", json!({})), ("status", json!({})), ("sessions", json!({}))] {
        if f[k].is_null() {
            f[k] = empty;
        }
    }
    if !f["defaults"]["everywhere"].is_object() {
        f["defaults"]["everywhere"] = json!({});
    }
    if !f["defaults"]["folders"].is_object() {
        f["defaults"]["folders"] = json!({});
    }
    f
}

fn save(f: &Value) -> Result<(), String> {
    let dir = config_dir();
    fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = dir.join("accounts.json");
    let tmp = dir.join(format!(".accounts.{}.json", std::process::id()));
    let mut w = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp).map_err(|e| e.to_string())?;
    w.write_all(serde_json::to_string_pretty(f).unwrap_or_default().as_bytes()).and_then(|_| w.sync_all()).map_err(|e| e.to_string())?;
    fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
}

// ── accounts ─────────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Acct {
    pub id: String,
    pub agent: String,
    pub label: String,
    pub personal: bool,
    pub home: Option<PathBuf>, // None: the agent's own folder
    pub use_: String,          // "plan" | "console" | "key"
    pub key: Option<String>,
}

pub fn personal_id(agent: &str) -> String {
    format!("{agent}-personal")
}

/// The id of a session's account (None: Personal).
pub fn account_id(agent: &str, account: Option<&str>) -> String {
    account.map(str::to_string).unwrap_or_else(|| personal_id(agent))
}

fn from_entry(e: &Value) -> Option<Acct> {
    let agent = e["agent"].as_str().filter(|a| AGENTS.contains(a))?.to_string();
    let id = e["id"].as_str()?.to_string();
    let personal = id == personal_id(&agent);
    Some(Acct { label: e["label"].as_str().unwrap_or(if personal { "Personal" } else { "" }).to_string(),
                home: if personal { None } else { e["home"].as_str().map(PathBuf::from) },
                use_: e["use"].as_str().unwrap_or("plan").to_string(), key: e["key"].as_str().map(str::to_string), id, agent, personal })
}

// Personal first for each agent, then the added ones; a Personal entry in the file only changes
// its label, use or key.
fn all(f: &Value) -> Vec<Acct> {
    let stored: Vec<Acct> = f["accounts"].as_array().into_iter().flatten().filter_map(from_entry).collect();
    let mut out = Vec::new();
    for agent in AGENTS {
        let pid = personal_id(agent);
        out.push(stored.iter().find(|a| a.id == pid).cloned().unwrap_or(Acct {
            id: pid, agent: agent.into(), label: "Personal".into(), personal: true, home: None, use_: "plan".into(), key: None,
        }));
        out.extend(stored.iter().filter(|a| a.agent == agent && !a.personal).cloned());
    }
    out
}

fn find(f: &Value, id: &str) -> Option<Acct> {
    all(f).into_iter().find(|a| a.id == id)
}

fn default_of(f: &Value, agent: &str) -> String {
    f["default"][agent].as_str().filter(|id| find(f, id).is_some_and(|a| a.agent == agent))
        .map(str::to_string).unwrap_or_else(|| personal_id(agent))
}

fn set_entry(f: &mut Value, a: &Acct) {
    let mut e = json!({"id": a.id, "agent": a.agent, "label": a.label, "home": a.home.as_ref().map(|h| h.to_string_lossy()), "use": a.use_});
    if let Some(k) = &a.key {
        e["key"] = json!(k);
    }
    let list = f["accounts"].as_array_mut().unwrap();
    match list.iter_mut().find(|x| x["id"] == a.id.as_str()) {
        Some(x) => *x = e,
        None => list.push(e),
    }
}

fn key_entry<'a>(f: &'a Value, id: &str) -> Option<&'a Value> {
    f["keys"].as_array().into_iter().flatten().find(|k| k["id"] == id)
}

fn provider_var(provider: &str, agent: &str) -> Option<&'static str> {
    PROVIDERS.iter().find(|p| p.0 == provider).map(|p| if agent == "pi" { p.2 } else { p.3 })
}

fn provider_name(provider: &str) -> &str {
    PROVIDERS.iter().find(|p| p.0 == provider).map(|p| p.1).unwrap_or(provider)
}

// The providers an account's key may be for.
fn key_fits(agent: &str, provider: &str) -> bool {
    match agent {
        "claude" => provider == "anthropic",
        "codex" => matches!(provider, "openai" | "openrouter"),
        _ => PROVIDERS.iter().any(|p| p.0 == provider),
    }
}

// ── what a child gets ────────────────────────────────────────────────────────────────────────

pub struct Spawn {
    pub home: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub args: Vec<String>,
    pub fd3: Option<String>, // Claude Code's key, for keys::pass_fd3
    pub anthropic_key: Option<String>,
}

// The account's folder and the keys it calls for (plan §2.14). `keys`: false leaves the keys out.
fn spawn_env(agent: &str, account: Option<&str>, with_keys: bool) -> Result<Spawn, String> {
    let id = account_id(agent, account);
    let (acct, f) = {
        let s = st();
        (find(&s.file, &id), s.file.clone())
    };
    let acct = acct.filter(|a| a.agent == agent).ok_or(format!("no such {agent} account: {id}"))?;
    let mut sp = Spawn { home: acct.home.clone(), env: Vec::new(), args: Vec::new(), fd3: None, anthropic_key: None };
    if agent == "pi" && !acct.personal {
        sp.env.push(("PI_CODING_AGENT_SESSION_DIR".into(), personal_dir("pi").join("sessions").to_string_lossy().into()));
    }
    if !with_keys {
        return Ok(sp);
    }
    let mut add = |provider: &str, secret: String, own: bool| match (agent, own) {
        ("claude", _) => {
            sp.anthropic_key = Some(secret.clone());
            sp.fd3 = Some(secret);
        }
        ("codex", _) if provider == "openrouter" => {
            for a in ["model_provider=\"openrouter\"", "model_providers.openrouter.name=\"OpenRouter\"",
                      "model_providers.openrouter.base_url=\"https://openrouter.ai/api/v1\"",
                      "model_providers.openrouter.env_key=\"OPENROUTER_API_KEY\""] {
                sp.args.extend(["-c".to_string(), a.to_string()]);
            }
            sp.env.push(("OPENROUTER_API_KEY".into(), secret));
        }
        ("codex", _) => sp.env.push(("CODEX_API_KEY".into(), secret)),
        _ => {
            if let Some(var) = provider_var(provider, agent) {
                if provider == "anthropic" {
                    sp.anthropic_key = Some(secret.clone());
                }
                sp.env.retain(|(k, _)| k != var);
                sp.env.push((var.into(), secret));
            }
        }
    };
    let mut used: Vec<String> = Vec::new();
    if matches!(agent, "pi" | "opencode") {
        for k in f["keys"].as_array().into_iter().flatten().filter(|k| k["use"].as_array().is_some_and(|u| u.iter().any(|x| x == agent))) {
            let (Some(kid), Some(p)) = (k["id"].as_str(), k["provider"].as_str()) else { continue };
            // a key the keyring will not give is left out (account_json shows that provider as
            // locked); only the account's own key below is one a start cannot do without
            match keys::lookup(kid) {
                Ok(secret) => add(p, secret, false),
                Err(e) => {
                    eprintln!("{agent} starts without key {kid}: {e}");
                    continue;
                }
            }
            used.push(kid.into());
        }
    }
    if acct.use_ == "key" {
        let kid = acct.key.as_deref().ok_or("This account has no key")?;
        let k = key_entry(&f, kid).ok_or(format!("This account's key ({kid}) is gone"))?;
        let p = k["provider"].as_str().unwrap_or("");
        if !used.iter().any(|u| u == kid) {
            add(p, keys::lookup(kid).map_err(|e| if e.contains("no such key") { format!("No {} key", provider_name(p)) } else { e })?, true);
        }
    }
    Ok(sp)
}

/// An agent's command for an account (None: Personal), its env scrubbed and the account's added.
/// With it, Claude Code's key for fd 3. Err: a key it needs cannot be read (the keyring is locked).
pub fn command_with(agent: &str, account: Option<&str>) -> Result<(Command, Option<String>), String> {
    let sp = spawn_env(agent, account, true)?;
    let mut cmd = signin::command(agent, sp.home.as_deref(), &sp.env, agent);
    cmd.args(&sp.args);
    if sp.fd3.is_some() {
        cmd.env("CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR", "3");
    }
    Ok((cmd, sp.fd3))
}

pub fn command(agent: &str, account: Option<&str>) -> Result<Command, String> {
    command_with(agent, account).map(|c| c.0)
}

// ── state ────────────────────────────────────────────────────────────────────────────────────

fn folders_of(f: &Value, a: &Acct) -> Vec<String> {
    let mut out: Vec<String> = f["defaults"]["folders"].as_object().into_iter().flatten()
        .filter(|(_, v)| v["run"][&a.agent]["account"] == a.id.as_str()).map(|(k, _)| k.clone()).collect();
    out.sort();
    out
}

// An account as the `accounts` state lists it.
fn account_json(s: &State, a: &Acct) -> Value {
    let f = &s.file;
    let status = &f["status"][&a.id];
    let mut v = json!({"id": a.id, "agent": a.agent, "label": a.label, "personal": a.personal, "default": default_of(f, &a.agent) == a.id,
                       "use": a.use_, "folders": folders_of(f, a)});
    if let Some(k) = &a.key {
        v["key"] = json!(k);
    }
    for k in ["email", "plan", "reason"] {
        if status[k].is_string() {
            v[k] = status[k].clone();
        }
    }
    if status["since"].is_u64() {
        v["since"] = status["since"].clone();
    }
    let mut providers: Vec<Value> = status["providers"].as_array().cloned().unwrap_or_default();
    let state = if s.signin.as_ref().is_some_and(|x| x["account"] == a.id.as_str()) {
        "signingIn".to_string()
    } else if a.use_ == "key" {
        match a.key.as_deref().and_then(|k| key_entry(f, k)) {
            None => "noKey".into(),
            Some(k) if k["check"]["state"] == "refused" => "keyRefused".into(),
            Some(_) if s.keyring == "locked" => "locked".into(),
            Some(_) if s.keyring == "absent" => "noKey".into(),
            Some(_) => "ready".into(),
        }
    } else {
        let mut state = status["state"].as_str().unwrap_or("checking").to_string();
        // A key pi keeps in its own file (via "file") is listed but does not make pi ready: pi
        // reads "Needs a provider" until a provider is signed in, or given a key, here (folders
        // plan Q9). pi itself takes such a key for a start with no model.
        let counts = |p: &Value| p["state"] == "ready" && (a.agent != "pi" || p["via"] != "file");
        if a.agent == "pi" && state == "ready" && !providers.iter().any(counts) {
            state = if providers.iter().any(|p| p["via"] != "file") { "signedOut" } else { "noProvider" }.into();
        }
        // pi and opencode keys the keyring would not give while it was locked
        if matches!(a.agent.as_str(), "pi" | "opencode") && s.keyring == "locked" {
            for k in f["keys"].as_array().into_iter().flatten().filter(|k| k["use"].as_array().is_some_and(|u| u.iter().any(|x| x == a.agent.as_str()))) {
                let p = k["provider"].as_str().unwrap_or("");
                if !providers.iter().any(|x| x["id"] == p && counts(x)) {
                    providers.retain(|x| x["id"] != p || x["via"] == "file");
                    providers.push(json!({"id": p, "via": "key", "state": "locked"}));
                }
            }
            if state != "ready" && state != "checking" && providers.iter().any(|x| x["state"] == "locked") {
                state = "locked".into();
            }
        }
        state
    };
    v["state"] = json!(state);
    if matches!(a.agent.as_str(), "pi" | "opencode") {
        v["providers"] = json!(providers);
    }
    v
}

fn state_of(s: &State) -> Value {
    let f = &s.file;
    let accounts: Vec<Value> = all(f).iter().map(|a| account_json(s, a)).collect();
    let keys: Vec<Value> = f["keys"].as_array().into_iter().flatten().map(|k| {
        let mut v = json!({"id": k["id"], "provider": k["provider"], "label": k["label"], "last4": k["last4"], "use": k["use"],
                           "state": k["check"]["state"].as_str().unwrap_or("unchecked")});
        if k["check"]["at"].is_u64() {
            v["checked"] = k["check"]["at"].clone();
        }
        v
    }).collect();
    let mut found = Vec::new();
    for a in all(f).iter().filter(|a| a.personal) {
        for p in f["status"][&a.id]["providers"].as_array().into_iter().flatten().filter(|p| p["via"] == "file") {
            found.push(json!({"agent": a.agent, "provider": p["id"], "where": "auth.json"}));
        }
    }
    let providers: Vec<Value> = PROVIDERS.iter().map(|p| json!({"id": p.0, "name": p.1, "env": {"pi": p.2, "opencode": p.3}})).collect();
    json!({"ev": "accounts", "keyring": s.keyring, "agents": s.agents, "node": s.node, "accounts": accounts, "keys": keys,
           "found": found, "providers": providers, "defaults": f["defaults"], "signin": s.signin, "install": s.install})
}

pub fn state_msg() -> Value {
    state_of(&st())
}

/// To every connection that watches accounts. Never called with the host or the accounts lock held.
fn push(msg: &Value) {
    if let Some(sh) = SHARED.get() {
        sh.lock().unwrap().push(msg);
    }
}

fn push_state() {
    let m = state_msg();
    push(&m);
}

// ── resolution (plan §3.2) ───────────────────────────────────────────────────────────────────

/// The agent, account, model, effort and mode a new session in `cwd` gets, and where each came
/// from: "explicit", "folder:<path>", "everywhere" or "agent" (the agent's own).
pub fn resolve(cwd: &str, agent: Option<&str>) -> Value {
    let f = st().file.clone();
    let d = &f["defaults"];
    let home = crate::home().to_string_lossy().into_owned();
    let mut best: Option<(&String, &Value)> = None;
    for (path, v) in d["folders"].as_object().into_iter().flatten() {
        let under = path == cwd || (*path != home && path != "/" && cwd.starts_with(&format!("{}/", path.trim_end_matches('/'))));
        if under && best.is_none_or(|b| path.len() > b.0.len()) {
            best = Some((path, v));
        }
    }
    let mut chain: Vec<(String, &Value)> = best.map(|(p, v)| vec![(format!("folder:{p}"), v)]).unwrap_or_default();
    chain.push(("everywhere".into(), &d["everywhere"]));
    let mut out = json!({"ev": "resolved", "from": {}});
    match agent {
        Some(a) => {
            out["agent"] = json!(a);
            out["from"]["agent"] = json!("explicit");
        }
        None => {
            let (from, a) = chain.iter().find_map(|(n, v)| v["agent"].as_str().filter(|a| AGENTS.contains(a)).map(|a| (n.clone(), a.to_string())))
                .unwrap_or(("agent".into(), "claude".into()));
            out["agent"] = json!(a);
            out["from"]["agent"] = json!(from);
        }
    }
    let ag = out["agent"].as_str().unwrap_or("claude").to_string();
    for field in ["account", "model", "effort"] {
        let found = chain.iter().find_map(|(n, v)| v["run"][&ag][field].as_str().map(|x| (n.clone(), x.to_string())));
        let found = found.filter(|(_, x)| field != "account" || find(&f, x).is_some_and(|a| a.agent == ag));
        match found {
            Some((n, x)) => {
                out[field] = json!(x);
                out["from"][field] = json!(n);
            }
            None => {
                out[field] = if field == "account" { json!(default_of(&f, &ag)) } else { Value::Null };
                out["from"][field] = json!("agent");
            }
        }
    }
    match chain.iter().find_map(|(n, v)| v["mode"].as_str().map(|m| (n.clone(), m.to_string()))) {
        Some((n, m)) => {
            out["mode"] = json!(m);
            out["from"]["mode"] = json!(n);
        }
        None => {
            out["mode"] = Value::Null;
            out["from"]["mode"] = json!("agent");
        }
    }
    out
}

/// A start or fork with its omitted fields filled in. A new session takes its folder's defaults,
/// then everywhere's, then the agent's own; a resume or a fork keeps the account its session ran
/// under. Personal and "default" become absent fields: the agent's own sign-in and settings.
pub fn fill(req: &Value, agent: &str) -> Result<Value, String> {
    let mut r = req.clone();
    let from = if req["op"] == "fork" { req["id"].as_str() } else { req["resume"].as_str() };
    match from {
        Some(id) => {
            if r["account"].is_null() {
                if let Some(a) = st().file["sessions"][id].as_str() {
                    r["account"] = json!(a);
                }
            }
        }
        None => {
            let res = resolve(req["cwd"].as_str().unwrap_or(""), Some(agent));
            for k in ["account", "model", "effort", "mode"] {
                if r[k].is_null() && res[k].is_string() {
                    r[k] = res[k].clone();
                }
            }
        }
    }
    for k in ["model", "effort"] {
        if r[k] == "default" {
            r[k] = Value::Null;
        }
    }
    if let Some(id) = r["account"].as_str().map(str::to_string) {
        let a = find(&st().file, &id).ok_or(format!("no such account: {id}"))?;
        if a.agent != agent {
            return Err(format!("{id} is a {} account", a.agent));
        }
        if a.personal {
            r["account"] = Value::Null;
        }
    }
    Ok(r)
}

/// A session started under an added account, so a resume keeps it.
pub fn note_session(id: &str, account: &str) {
    let mut s = st();
    if s.file["sessions"][id] != account {
        s.file["sessions"][id] = json!(account);
        let _ = save(&s.file);
    }
}

// ── sign-in state ────────────────────────────────────────────────────────────────────────────

fn set_status(s: &mut State, id: &str, st_: &signin::Status) {
    let old = s.file["status"][id].clone();
    let mut v = st_.to_json();
    v["since"] = if old["state"] == st_.state { old["since"].clone() } else { json!(now_ms()) };
    v["checked"] = json!(now_ms());
    s.file["status"][id] = v;
}

/// Reads an account's sign-in through its agent, at most one read per account at a time (a
/// second waits for the first), and pushes the state.
pub fn check(id: &str) {
    check_in(id, false);
}

/// After a sign-in or sign-out: a read already under way began before it and may hold the old
/// sign-in, so this one waits for it and then reads again.
fn check_fresh(id: &str) {
    check_in(id, true);
}

fn check_in(id: &str, fresh: bool) {
    {
        let mut s = st();
        if s.checking.contains(id) {
            while s.checking.contains(id) {
                s = CHECKED.wait(s).unwrap();
            }
            if !fresh {
                return;
            }
        }
        // another fresh read may have started while this one woke: wait for that too
        while s.checking.contains(id) {
            s = CHECKED.wait(s).unwrap();
        }
        s.checking.insert(id.to_string());
    }
    let read = || -> Option<(String, Result<signin::Status, String>)> {
        let (a, installed) = {
            let s = st();
            let a = find(&s.file, id)?;
            let installed = s.agents[&a.agent]["installed"] != false;
            (a, installed)
        };
        if a.use_ == "key" {
            return None;
        }
        if !installed {
            return Some((a.agent.clone(), Err("Not installed".into())));
        }
        // a pi or opencode key the locked keyring will not give is shown as locked (account_json)
        let sp = spawn_env(&a.agent, Some(&a.id), true).or_else(|_| spawn_env(&a.agent, Some(&a.id), false)).ok()?;
        Some((a.agent.clone(), signin::status(&a.agent, sp.home.as_deref(), &sp.env)))
    };
    let got = read();
    {
        let mut s = st();
        match got {
            Some((_, Ok(status))) => set_status(&mut s, id, &status),
            Some((_, Err(e))) => {
                if s.file["status"][id]["state"].is_null() {
                    s.file["status"][id] = json!({"state": "signedOut", "since": now_ms()});
                }
                s.file["status"][id]["reason"] = json!(keys::redact(&e));
                s.file["status"][id]["checked"] = json!(now_ms());
            }
            None => {}
        }
        let _ = save(&s.file);
        s.checking.remove(id);
        CHECKED.notify_all();
    }
    push_state();
}

/// Every account at once, each on a thread of its own; returns when all have answered.
pub fn check_all() {
    refresh_keyring();
    let ids: Vec<String> = all(&st().file).into_iter().map(|a| a.id).collect();
    let ts: Vec<_> = ids.into_iter().map(|id| thread::spawn(move || check(&id))).collect();
    for t in ts {
        let _ = t.join();
    }
    st().checked_all = Some(Instant::now());
}

fn refresh_keyring() {
    let k = keys::keyring();
    st().keyring = k;
}

fn refresh_agents() {
    let found: Vec<(&str, Value)> = AGENTS.iter().map(|a| (*a, thread::spawn(move || install::detect(a).to_json())))
        .map(|(a, t)| (a, t.join().unwrap_or(Value::Null))).collect();
    let node = install::node_present();
    let mut s = st();
    for (a, v) in found {
        s.agents[a] = v;
    }
    s.node = node;
}

/// Whether the agent's default account can run, for the hello: true, false, or null when that
/// cannot be told; with the reason when it cannot. Read when the last read is older than 5 min.
pub fn signed_in(agent: &str) -> (Value, String) {
    let (id, stale) = {
        let s = st();
        let id = default_of(&s.file, agent);
        let at = s.file["status"][&id]["checked"].as_u64().unwrap_or(0);
        (id, now_ms().saturating_sub(at) > 300_000)
    };
    if stale {
        check(&id);
    }
    let m = state_msg();
    let a = m["accounts"].as_array().into_iter().flatten().find(|a| a["id"] == id.as_str()).cloned().unwrap_or(Value::Null);
    match a["state"].as_str().unwrap_or("") {
        "ready" => (json!(true), String::new()),
        "signingIn" | "checking" => (Value::Null, "Checking".into()),
        "expired" => (json!(false), "Signed out".into()),
        "noProvider" => (json!(false), "Needs a provider".into()),
        "locked" => (json!(false), "The keyring is locked".into()),
        "noKey" => (json!(false), "No key".into()),
        "keyRefused" => (json!(false), "The key was refused".into()),
        _ => (json!(false), a["reason"].as_str().filter(|r| *r == "Not installed").unwrap_or("Not signed in").into()),
    }
}

// ── auth failures in a session (exit.reason) ─────────────────────────────────────────────────

// Each agent's words for a sign-in that is gone or worn out, from its own binary's strings
// (Claude Code 2.1.289, codex-cli 0.159, pi 0.86, opencode 1.18).
const EXPIRED: [(&str, &str); 12] = [
    ("claude", "OAuth token has expired"), ("claude", "OAuth access token has expired"), ("claude", "Login expired"),
    ("claude", "login has expired"), ("claude", "Session expired. Please run /login"), ("claude", "Refresh token"),
    ("codex", "Your access token could not be refreshed"), ("codex", "Your authentication session could not be refreshed"),
    ("pi", "to re-authenticate"), ("pi", "Your session has expired"), ("pi", "Token is expired"),
    ("opencode", "Your session has expired"),
];
const SIGNED_OUT: [(&str, &str); 14] = [
    ("claude", "Please run /login"), ("claude", "Not logged in"), ("claude", "Invalid API key"), ("claude", "OAuth token revoked"),
    ("claude", "authentication_error"), ("codex", "Not logged in"), ("codex", "401 Unauthorized"), ("codex", "Incorrect API key"),
    ("codex", "Please sign in again"), ("pi", "No API key found for"), ("pi", "No API key for"), ("pi", "Authentication failed for"),
    ("opencode", "ProviderAuthError"), ("opencode", "API key is missing"),
];

/// "expired" or "signedOut" when `text` is the agent saying its sign-in no longer works.
pub fn auth_failure(agent: &str, text: &str) -> Option<&'static str> {
    if EXPIRED.iter().any(|(a, w)| *a == agent && text.contains(w)) {
        return Some("expired");
    }
    SIGNED_OUT.iter().any(|(a, w)| *a == agent && text.contains(w)).then_some("signedOut")
}

/// A session of this account was refused: its sign-in is marked so (a key account's key as
/// refused) and the state pushed. Called with the host locked: the push runs on a thread.
pub fn refused(agent: &str, account: Option<&str>, reason: &'static str) {
    let id = account_id(agent, account);
    let mut s = st();
    let Some(a) = find(&s.file, &id) else { return };
    if a.use_ == "key" {
        if let Some(k) = a.key.as_deref() {
            if let Some(e) = s.file["keys"].as_array_mut().and_then(|l| l.iter_mut().find(|x| x["id"] == k)) {
                e["check"] = json!({"state": "refused", "at": now_ms()});
            }
        }
    } else {
        let since = now_ms();
        s.file["status"][&id]["state"] = json!(reason);
        s.file["status"][&id]["since"] = json!(since);
    }
    let _ = save(&s.file);
    drop(s);
    thread::spawn(push_state);
}

// ── ops ──────────────────────────────────────────────────────────────────────────────────────

pub const OPS: [&str; 19] = ["accounts", "resolve", "catalog", "defaults_set", "account_check", "account_add", "account_login",
                             "account_code", "account_cancel", "account_logout", "account_remove", "account_set", "key_set",
                             "key_use", "key_import", "key_check", "key_remove", "install", "install_cancel"];

/// One accounts op for connection `cid`, on a thread of its own, holding no lock while it waits.
pub fn op(cid: u64, req: &Value) -> Result<Value, String> {
    let ok = json!({"ev": "ok"});
    let s = |k: &str| req[k].as_str().ok_or(format!("{} needs {k}", req["op"].as_str().unwrap_or("")));
    match req["op"].as_str().unwrap_or("") {
        "accounts" => {
            if req["watch"] == true {
                if let Some(sh) = SHARED.get() {
                    if let Some(c) = sh.lock().unwrap().clients.get_mut(&cid) {
                        c.accounts = true;
                    }
                }
                // the host starts with the session, before the lock's password opens the keyring: read it again,
                // and once it has opened, check again what was read without its keys
                let was = st().keyring;
                refresh_keyring();
                if was == "locked" && st().keyring == "open" {
                    thread::spawn(check_all);
                }
            }
            Ok(state_msg())
        }
        "resolve" => Ok(resolve(s("cwd")?, req["agent"].as_str().filter(|a| AGENTS.contains(a)))),
        "catalog" => catalog_op(req),
        "defaults_set" => {
            defaults_set(req)?;
            push_state();
            Ok(ok)
        }
        "account_check" => {
            match req["account"].as_str() {
                Some(id) => {
                    refresh_keyring();
                    find(&st().file, id).ok_or(format!("no such account: {id}"))?;
                    let id = id.to_string();
                    thread::spawn(move || check(&id));
                }
                None => {
                    thread::spawn(|| {
                        refresh_agents();
                        check_all();
                    });
                }
            }
            Ok(ok)
        }
        "account_add" => account_add(req),
        "account_login" => login(req).map(|_| ok),
        "account_code" => {
            let id = s("account")?;
            let st_ = st();
            let l = st_.logins.get(id).ok_or("No sign-in is running for this account")?;
            l.code(s("code")?)?;
            Ok(ok)
        }
        "account_cancel" => {
            let l = st().logins.remove(s("account")?);
            // dropped outside the lock: its last step comes back through it
            if let Some(l) = l {
                l.cancel();
            }
            Ok(ok)
        }
        "account_logout" => {
            let id = s("account")?.to_string();
            let a = find(&st().file, &id).ok_or(format!("no such account: {id}"))?;
            let sp = spawn_env(&a.agent, Some(&id), true).or_else(|_| spawn_env(&a.agent, Some(&id), false))?;
            signin::logout(&a.agent, sp.home.as_deref(), req["provider"].as_str(), &sp.env)?;
            check_fresh(&id);
            after_signin(&a.agent);
            Ok(ok)
        }
        "account_remove" => remove(s("account")?).map(|_| ok),
        "account_set" => {
            account_set(req)?;
            push_state();
            Ok(ok)
        }
        "key_set" => key_set(req),
        "key_use" => {
            let (id, agent) = (s("id")?, s("agent")?);
            if !matches!(agent, "pi" | "opencode") {
                return Err(format!("{agent} uses a key only as an account's own"));
            }
            let mut st_ = st();
            let k = st_.file["keys"].as_array_mut().and_then(|l| l.iter_mut().find(|k| k["id"] == id)).ok_or("no such key")?;
            let mut use_: Vec<Value> = k["use"].as_array().cloned().unwrap_or_default();
            use_.retain(|x| x != agent);
            if req["on"] == true {
                use_.push(json!(agent));
            }
            k["use"] = json!(use_);
            save(&st_.file)?;
            let pid = personal_id(agent);
            drop(st_);
            // pi and opencode list a provider by its key
            thread::spawn(move || check(&pid));
            Ok(ok)
        }
        // A key pi or opencode keeps in its own file, into the keyring and given to that agent: the
        // Accounts page's "Use here" on a key it listed as "not connected here".
        "key_import" => {
            let (agent, provider) = (s("agent")?, s("provider")?);
            let r = key_import(agent, provider)?;
            if let Some(id) = r["id"].as_str() {
                let mut st_ = st();
                if let Some(k) = st_.file["keys"].as_array_mut().and_then(|l| l.iter_mut().find(|k| k["id"] == id)) {
                    k["use"] = json!([agent]);
                }
                save(&st_.file)?;
                let pid = personal_id(agent);
                drop(st_);
                thread::spawn(move || check(&pid));
            }
            Ok(r)
        }
        "key_check" => {
            let id = s("id")?;
            refresh_keyring();
            let (provider, last4) = {
                let st_ = st();
                let k = key_entry(&st_.file, id).ok_or("no such key")?;
                (k["provider"].as_str().unwrap_or("").to_string(), k["last4"].clone())
            };
            let state = match keys::lookup(id) {
                Ok(secret) => keys::check(&provider, &secret),
                Err(e) => return Err(e),
            };
            {
                let mut st_ = st();
                if let Some(k) = st_.file["keys"].as_array_mut().and_then(|l| l.iter_mut().find(|k| k["id"] == id)) {
                    k["check"] = json!({"state": state, "at": now_ms()});
                }
                save(&st_.file)?;
            }
            push_state();
            Ok(json!({"ev": "key", "id": id, "last4": last4, "state": state}))
        }
        "key_remove" => {
            let id = s("id")?;
            let used: Vec<String> = all(&st().file).into_iter().filter(|a| a.key.as_deref() == Some(id)).map(|a| a.id).collect();
            if !used.is_empty() && req["force"] != true {
                return Ok(json!({"ev": "error", "message": "This key is used by an account.", "usedBy": used}));
            }
            key_entry(&st().file, id).ok_or("no such key")?;
            keys::clear(id)?;
            {
                let mut st_ = st();
                if let Some(l) = st_.file["keys"].as_array_mut() {
                    l.retain(|k| k["id"] != id);
                }
                save(&st_.file)?;
            }
            refresh_keyring();
            push_state();
            Ok(ok)
        }
        "install" => install_op(s("agent")?, req["node"] == true).map(|_| ok),
        "install_cancel" => {
            let i = st().installs.remove(s("agent")?);
            if let Some(i) = i {
                i.cancel();
            }
            Ok(ok)
        }
        o => Err(format!("unknown op: {o}")),
    }
}

fn defaults_set(req: &Value) -> Result<(), String> {
    let scope = req["scope"].as_str().ok_or("defaults_set needs scope")?;
    if scope != "everywhere" && !scope.starts_with('/') {
        return Err("scope is \"everywhere\" or a folder's full path".into());
    }
    let mut s = st();
    let f = s.file.clone();
    let d = &mut s.file["defaults"];
    let target = if scope == "everywhere" { &mut d["everywhere"] } else { &mut d["folders"][scope] };
    if !target.is_object() {
        *target = json!({});
    }
    let t = target.as_object_mut().unwrap();
    for k in ["agent", "mode"] {
        match req.get(k) {
            Some(Value::Null) => {
                t.remove(k);
            }
            Some(Value::String(v)) => {
                if k == "agent" && !AGENTS.contains(&v.as_str()) {
                    return Err(format!("unknown agent: {v}"));
                }
                t.insert(k.into(), json!(v));
            }
            _ => {}
        }
    }
    for (ag, fields) in req["run"].as_object().into_iter().flatten() {
        if !AGENTS.contains(&ag.as_str()) {
            return Err(format!("unknown agent: {ag}"));
        }
        let run = t.entry("run").or_insert(json!({}));
        if fields.is_null() {
            run.as_object_mut().map(|r| r.remove(ag));
            continue;
        }
        let entry = run[ag.as_str()].as_object().cloned().unwrap_or_default();
        let mut entry = Value::Object(entry);
        for (k, v) in fields.as_object().into_iter().flatten() {
            if !matches!(k.as_str(), "account" | "model" | "effort") {
                continue;
            }
            match v.as_str() {
                None => {
                    entry.as_object_mut().unwrap().remove(k);
                }
                Some(x) => {
                    if k == "account" && !find(&f, x).is_some_and(|a| a.agent == *ag) {
                        return Err(format!("no such {ag} account: {x}"));
                    }
                    entry[k] = json!(x);
                }
            }
        }
        if entry.as_object().unwrap().is_empty() {
            run.as_object_mut().map(|r| r.remove(ag));
        } else {
            run[ag.as_str()] = entry;
        }
    }
    if t.get("run").is_some_and(|r| r.as_object().is_some_and(|o| o.is_empty())) {
        t.remove("run");
    }
    if scope != "everywhere" && t.is_empty() {
        s.file["defaults"]["folders"].as_object_mut().map(|o| o.remove(scope));
    }
    save(&s.file)
}

fn new_id(f: &Value, agent: &str, label: &str) -> String {
    let slug: String = label.to_lowercase().chars().filter(|c| c.is_ascii_alphanumeric()).take(24).collect();
    let base = format!("{agent}-{}", if slug.is_empty() || slug == "personal" { "account".into() } else { slug });
    let mut id = base.clone();
    let mut n = 1;
    while find(f, &id).is_some() || accounts_root().join(&id).exists() {
        n += 1;
        id = format!("{base}{n}");
    }
    id
}

// The account's folder, Personal's shared files linked in. The links are only read through.
fn make_home(agent: &str, id: &str) -> Result<PathBuf, String> {
    let root = accounts_root();
    fs::DirBuilder::new().recursive(true).mode(0o700).create(&root).map_err(|e| format!("{}: {e}", root.display()))?;
    let dir = root.join(id);
    fs::DirBuilder::new().mode(0o700).create(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let own = personal_dir(agent);
    // the history folders made first: linked only when there, an account added before Personal's first session kept
    // its conversations in its own folder, where Praxis's history (history.rs, Personal's) never looks
    let history: &[&str] = match agent {
        "claude" => &["projects"],
        "codex" => &["sessions"],
        _ => &[],
    };
    for name in history {
        let _ = fs::DirBuilder::new().recursive(true).mode(0o700).create(own.join(name));
    }
    for name in shared_names(agent) {
        let target = own.join(name);
        if target.exists() {
            std::os::unix::fs::symlink(&target, dir.join(name)).map_err(|e| format!("{name}: {e}"))?;
        }
    }
    Ok(dir)
}

fn account_add(req: &Value) -> Result<Value, String> {
    let agent = req["agent"].as_str().filter(|a| AGENTS.contains(a)).ok_or("account_add needs a known agent")?;
    let label = req["label"].as_str().map(str::trim).filter(|l| !l.is_empty()).ok_or("account_add needs a label")?;
    let use_ = req["use"].as_str().unwrap_or("plan");
    let ok_use = match agent {
        "claude" => matches!(use_, "plan" | "console" | "key"),
        "opencode" => use_ == "key",
        _ => matches!(use_, "plan" | "key"),
    };
    if !ok_use {
        return Err(if agent == "opencode" { "A second opencode account is a key".into() } else { format!("{agent} has no {use_} accounts") });
    }
    let key = req["key"].as_str().map(str::to_string);
    let mut s = st();
    if use_ == "key" {
        let k = key.as_deref().and_then(|k| key_entry(&s.file, k)).ok_or("A key account needs one of the saved keys")?;
        let p = k["provider"].as_str().unwrap_or("");
        if !key_fits(agent, p) {
            return Err(format!("{agent} cannot use a {} key", provider_name(p)));
        }
    }
    let id = new_id(&s.file, agent, label);
    let home = if agent == "opencode" { None } else { Some(make_home(agent, &id)?) };
    let a = Acct { id: id.clone(), agent: agent.into(), label: label.into(), personal: false, home, use_: use_.into(),
                   key: if use_ == "key" { key } else { None } };
    set_entry(&mut s.file, &a);
    if use_ != "key" {
        s.file["status"][&id] = json!({"state": "signedOut", "since": now_ms(), "checked": now_ms()});
    }
    save(&s.file)?;
    drop(s);
    push_state();
    Ok(json!({"ev": "account", "id": id}))
}

fn account_set(req: &Value) -> Result<(), String> {
    let id = req["account"].as_str().ok_or("account_set needs account")?;
    let mut s = st();
    let mut a = find(&s.file, id).ok_or(format!("no such account: {id}"))?;
    if let Some(l) = req["label"].as_str().map(str::trim) {
        if l.is_empty() {
            return Err("An account needs a name".into());
        }
        a.label = l.into();
    }
    if let Some(u) = req["use"].as_str() {
        let ok = match a.agent.as_str() {
            "claude" => matches!(u, "plan" | "console" | "key"),
            "opencode" if !a.personal => u == "key",
            _ => matches!(u, "plan" | "key"),
        };
        if !ok {
            return Err(format!("{} has no {u} accounts", a.agent));
        }
        a.use_ = u.into();
    }
    match req.get("key") {
        Some(Value::String(k)) => {
            let e = key_entry(&s.file, k).ok_or("no such key")?;
            let p = e["provider"].as_str().unwrap_or("");
            if !key_fits(&a.agent, p) {
                return Err(format!("{} cannot use a {} key", a.agent, provider_name(p)));
            }
            a.key = Some(k.clone());
        }
        Some(Value::Null) => a.key = None,
        _ => {}
    }
    set_entry(&mut s.file, &a);
    if req["default"] == true {
        s.file["default"][&a.agent] = json!(a.id);
    }
    save(&s.file)
}

// The key `agent` keeps for `provider` in its own file (pi: ~/.pi/agent/auth.json, opencode:
// $XDG_DATA_HOME/opencode/auth.json), stored as key_set stores a pasted one.
fn key_import(agent: &str, provider: &str) -> Result<Value, String> {
    let home = PathBuf::from(std::env::var("HOME").map_err(|_| "HOME is not set")?);
    let path = match agent {
        "pi" => home.join(".pi/agent/auth.json"),
        "opencode" => std::env::var("XDG_DATA_HOME").map(PathBuf::from).unwrap_or_else(|_| home.join(".local/share")).join("opencode/auth.json"),
        _ => return Err(format!("{agent} keeps no keys of its own")),
    };
    let text = fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let secret = v[provider]["key"].as_str().ok_or(format!("{agent}'s own file has no {provider} key"))?;
    key_set(&json!({"provider": provider, "secret": secret}))
}

fn key_set(req: &Value) -> Result<Value, String> {
    let provider = req["provider"].as_str().filter(|p| PROVIDERS.iter().any(|x| x.0 == *p)).ok_or("key_set needs a known provider")?;
    let secret = req["secret"].as_str().map(str::trim).unwrap_or("");
    if !keys::well_formed(secret) {
        return Err("That does not look like a key".into());
    }
    let last4 = keys::last4(secret);
    refresh_keyring();
    match st().keyring {
        "locked" => return Err("The keyring is locked".into()),
        "absent" => return Err("There is no keyring to keep keys in".into()),
        _ => {}
    }
    let state = keys::check(provider, secret);
    if state == "refused" {
        return Ok(json!({"ev": "key", "id": null, "last4": last4, "state": "refused"}));
    }
    let id = format!("key-{provider}-{}", &crate::new_id()[..8]);
    let label = req["label"].as_str().map(str::trim).filter(|l| !l.is_empty()).map(str::to_string)
        .unwrap_or_else(|| format!("{} key", provider_name(provider)));
    keys::store(&id, provider, &format!("Tacit: {label}"), secret)?;
    {
        let mut s = st();
        s.file["keys"].as_array_mut().unwrap().push(json!({"id": id, "provider": provider, "label": label, "last4": last4,
            "added": now_ms(), "use": [], "check": {"state": state, "at": now_ms()}}));
        save(&s.file)?;
    }
    push_state();
    Ok(json!({"ev": "key", "id": id, "last4": last4, "state": state}))
}

// ── sign-in ──────────────────────────────────────────────────────────────────────────────────

fn login(req: &Value) -> Result<(), String> {
    let id = req["account"].as_str().ok_or("account_login needs account")?.to_string();
    let a = find(&st().file, &id).ok_or(format!("no such account: {id}"))?;
    if a.use_ == "key" {
        return Err("A key account does not sign in".into());
    }
    let method = req["method"].as_str().unwrap_or(if a.use_ == "console" { "console" } else { "browser" }).to_string();
    let provider = req["provider"].as_str().map(str::to_string);
    let sp = spawn_env(&a.agent, Some(&id), true).or_else(|_| spawn_env(&a.agent, Some(&id), false))?;
    let spec = signin::LoginSpec { agent: a.agent.clone(), account: id.clone(), home: sp.home.clone(), method, provider: provider.clone(), env: sp.env };
    let (aid, agent) = (id.clone(), a.agent.clone());
    let on = Box::new(move |step: signin::Step| {
        let mut m = step.to_json();
        m["ev"] = json!("signin");
        m["account"] = json!(aid);
        if let Some(p) = &provider {
            m["provider"] = json!(p);
        }
        let last = matches!(step, signin::Step::Done { .. } | signin::Step::Failed(_) | signin::Step::Cancelled | signin::Step::Busy { .. });
        let gone = {
            let mut s = st();
            if last {
                if s.signin.as_ref().is_some_and(|x| x["account"] == aid.as_str()) {
                    s.signin = None;
                }
                s.logins.remove(&aid)
            } else {
                let mut cur = m.clone();
                cur.as_object_mut().unwrap().remove("ev");
                s.signin = Some(cur);
                None
            }
        };
        drop(gone);
        push(&m);
        push_state();
        if let signin::Step::Done { .. } = step {
            let (aid, agent) = (aid.clone(), agent.clone());
            thread::spawn(move || {
                check_fresh(&aid);
                first_ready(&agent);
                after_signin(&agent);
                refresh_catalog(&agent, &aid);
            });
        }
    });
    let mut s = st();
    if s.logins.contains_key(&id) {
        return Err("A sign-in for this account is already running".into());
    }
    // held while it starts: its steps wait for the Login to be in place
    let l = signin::start(spec, on)?;
    s.logins.insert(id, l);
    Ok(())
}

// After a sign-in or sign-out: the hello's agents again, and the account's idle Codex server
// closed, so the next thread reads the new auth.json.
fn after_signin(agent: &str) {
    if agent == "codex" {
        if let Some(sh) = SHARED.get() {
            crate::codex::drop_idle(&mut sh.lock().unwrap());
        }
    }
    thread::spawn(crate::probe_agents);
}

// The first agent signed in on a machine with none ready is made the default agent everywhere.
fn first_ready(agent: &str) {
    let m = state_msg();
    let ready: Vec<&str> = m["accounts"].as_array().into_iter().flatten().filter(|a| a["state"] == "ready" && a["default"] == true)
        .filter_map(|a| a["agent"].as_str()).collect();
    if ready == [agent] {
        let mut s = st();
        if s.file["defaults"]["everywhere"]["agent"].is_null() {
            s.file["defaults"]["everywhere"]["agent"] = json!(agent);
            let _ = save(&s.file);
            drop(s);
            push_state();
        }
    }
}

fn remove(id: &str) -> Result<(), String> {
    let a = find(&st().file, id).ok_or(format!("no such account: {id}"))?;
    if a.personal {
        return Err("Personal can't be removed.".into());
    }
    let l = st().logins.remove(id);
    if let Some(l) = l {
        l.cancel();
    }
    // 1. its live sessions closed
    if let Some(sh) = SHARED.get() {
        let mut h = sh.lock().unwrap();
        let open: Vec<String> = h.sessions.values().filter(|s| s.account.as_deref() == Some(id) && s.to_child.is_some()).map(|s| s.id.clone()).collect();
        for sid in open {
            crate::close(&mut h, &sid, "closed");
            h.announce(&sid);
        }
        if a.agent == "codex" {
            crate::codex::drop_idle(&mut h);
        }
    }
    // 2. signed out through the agent (a key account has nothing to sign out of)
    if a.use_ != "key" && a.agent != "opencode" {
        if let Ok(sp) = spawn_env(&a.agent, Some(id), false) {
            if let Err(e) = signin::logout(&a.agent, sp.home.as_deref(), None, &sp.env) {
                eprintln!("[accounts] {id}: sign-out before removal: {}", keys::redact(&e));
            }
        }
    }
    // 3. its credential files deleted outright, 4. its folder to the Trash
    if let Some(dir) = &a.home {
        if dir.starts_with(accounts_root()) {
            for f in credential_files(&a.agent) {
                let p = dir.join(f);
                if p.is_file() && !p.is_symlink() {
                    fs::remove_file(&p).map_err(|e| format!("{}: {e}", p.display()))?;
                }
            }
            if dir.exists() {
                let ok = Command::new("gio").args(["trash", "--"]).arg(dir).status().is_ok_and(|s| s.success());
                if !ok {
                    return Err(format!("{} could not go to the Trash", dir.display()));
                }
            }
        }
    }
    {
        let mut s = st();
        let f = &mut s.file;
        if let Some(l) = f["accounts"].as_array_mut() {
            l.retain(|x| x["id"] != id);
        }
        f["status"].as_object_mut().map(|o| o.remove(id));
        if f["default"][&a.agent] == id {
            f["default"].as_object_mut().map(|o| o.remove(&a.agent));
        }
        if let Some(o) = f["sessions"].as_object_mut() {
            o.retain(|_, v| v != id);
        }
        // defaults naming it fall back to the agent's default account
        let unset = |v: &mut Value| {
            if v["run"][&a.agent]["account"] != id {
                return;
            }
            v["run"][&a.agent].as_object_mut().map(|r| r.remove("account"));
            if v["run"][&a.agent].as_object().is_some_and(|r| r.is_empty()) {
                v["run"].as_object_mut().map(|r| r.remove(&a.agent));
            }
            if v["run"].as_object().is_some_and(|r| r.is_empty()) {
                v.as_object_mut().map(|r| r.remove("run"));
            }
        };
        unset(&mut f["defaults"]["everywhere"]);
        if let Some(o) = f["defaults"]["folders"].as_object_mut() {
            o.values_mut().for_each(unset);
            o.retain(|_, v| v.as_object().is_some_and(|r| !r.is_empty()));
        }
        save(&s.file)?;
    }
    push_state();
    Ok(())
}

// ── installs ─────────────────────────────────────────────────────────────────────────────────

fn install_op(agent: &str, node: bool) -> Result<(), String> {
    if !AGENTS.contains(&agent) {
        return Err(format!("unknown agent {agent}"));
    }
    let ag = agent.to_string();
    let on = Box::new(move |step: install::InstallStep| {
        let mut m = step.to_json();
        m["ev"] = json!("install");
        m["agent"] = json!(ag);
        let running = matches!(step, install::InstallStep::Running { .. });
        let gone = {
            let mut s = st();
            if running {
                let mut cur = m.clone();
                cur.as_object_mut().unwrap().remove("ev");
                s.install = Some(cur);
                None
            } else {
                s.install = None;
                s.installs.remove(&ag)
            }
        };
        drop(gone);
        push(&m);
        if !running {
            let ag = ag.clone();
            thread::spawn(move || {
                refresh_agents();
                push_state();
                crate::probe_agents();
                check(&personal_id(&ag));
            });
        }
    });
    let mut s = st();
    let i = install::install(agent, node, on)?;
    s.installs.insert(agent.to_string(), i);
    Ok(())
}

// ── catalog ──────────────────────────────────────────────────────────────────────────────────

fn ctx(agent: &str, account: &str, cwd: Option<&str>) -> Result<catalog::Ctx, String> {
    let a = find(&st().file, account).filter(|a| a.agent == agent).ok_or(format!("no such {agent} account: {account}"))?;
    let acct = (!a.personal).then_some(account);
    let sp = spawn_env(agent, acct, true).or_else(|_| spawn_env(agent, acct, false))?;
    Ok(catalog::Ctx { agent: agent.into(), account: account.into(), home: sp.home, env: sp.env, cwd: cwd.map(str::to_string),
                      anthropic_key: sp.anthropic_key })
}

// A fresh read, pushed to watchers; one per account and folder at a time.
fn refresh_into(c: catalog::Ctx) -> Result<Value, String> {
    let key = format!("{}|{}|{}", c.agent, c.account, c.cwd.as_deref().unwrap_or(""));
    if !st().refreshing.insert(key.clone()) {
        return Err("busy".into());
    }
    let r = catalog::refresh(&c);
    st().refreshing.remove(&key);
    let mut v = r?;
    v["ev"] = json!("catalog");
    push(&v);
    Ok(v)
}

fn refresh_catalog(agent: &str, account: &str) {
    if let Ok(c) = ctx(agent, account, None) {
        let _ = refresh_into(c);
    }
}

fn catalog_op(req: &Value) -> Result<Value, String> {
    let agent = req["agent"].as_str().filter(|a| AGENTS.contains(a)).ok_or("catalog needs a known agent")?;
    let account = req["account"].as_str().map(str::to_string).unwrap_or_else(|| default_of(&st().file, agent));
    let cwd = req["cwd"].as_str().filter(|c| !c.is_empty());
    if st().agents[agent]["installed"] == false {
        return Err(format!("{agent} is not installed"));
    }
    let c = ctx(agent, &account, cwd)?;
    match catalog::cached(&c) {
        Some(mut v) => {
            if v["stale"] == true || req["refresh"] == true {
                let c2 = ctx(agent, &account, cwd)?;
                thread::spawn(move || {
                    if let Err(e) = refresh_into(c2) {
                        if e != "busy" {
                            eprintln!("[catalog] {}", keys::redact(&e));
                        }
                    }
                });
            }
            v["ev"] = json!("catalog");
            Ok(v)
        }
        None => refresh_into(c).or_else(|e| if e == "busy" { Err("The model list is being read; it comes when it is done".into()) } else { Err(e) }),
    }
}

/// A live session's commands changed or its agent listed them: kept in the catalog, which
/// pushes nothing until it is asked.
pub fn note_commands(agent: &str, account: Option<&str>, cwd: &str, raw: Value) {
    let (agent, id, cwd) = (agent.to_string(), account_id(agent, account), cwd.to_string());
    thread::spawn(move || catalog::note_commands(&agent, &id, &cwd, &raw));
}

pub fn note_claude_init(account: Option<&str>, cwd: &str, init: Value) {
    let (id, cwd) = (account_id("claude", account), cwd.to_string());
    thread::spawn(move || catalog::note_claude_init(&id, &cwd, &init));
}

// ── start ────────────────────────────────────────────────────────────────────────────────────

/// At host start: the agents found, every account read, then read again every 10 min while a
/// client watches.
pub fn start(shared: crate::Shared) {
    let _ = SHARED.set(shared.clone());
    refresh_keyring();
    thread::spawn(move || {
        refresh_agents();
        check_all();
        loop {
            thread::sleep(Duration::from_secs(60));
            let watched = shared.lock().unwrap().clients.values().any(|c| c.accounts);
            let due = st().checked_all.is_none_or(|t| t.elapsed() >= CHECK_EVERY);
            if watched && due {
                refresh_agents();
                check_all();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_lines() {
        assert_eq!(auth_failure("claude", "API Error: 401 Invalid API key · Please run /login"), Some("signedOut"));
        assert_eq!(auth_failure("claude", "OAuth token has expired. Please obtain a new token"), Some("expired"));
        assert_eq!(auth_failure("codex", "Your access token could not be refreshed because your refresh token has expired."), Some("expired"));
        assert_eq!(auth_failure("pi", "No API key found for openrouter."), Some("signedOut"));
        assert_eq!(auth_failure("pi", "Please run /login"), None);
        assert_eq!(auth_failure("claude", "all good"), None);
    }
}
