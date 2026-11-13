//! Past sessions: every Claude Code, Codex, pi and opencode session on disk, indexed for the Claude
//! app's list, searched with rg (opencode: SQL), and turned into a readable message list.
//!
//! Transcripts only grow, so the index remembers how far it read each file and reads only what
//! was appended since. Metadata lines ({"type":"ai-title"...}) start with their type, so most
//! lines are recognised from their first bytes without parsing them.

#[path = "sources.rs"]
mod sources;
pub use sources::{holder_outside, in_session_scope, made_load, made_store, rename_offline, sweep_made, terminal_holder, tool_of, trash_session};

use base64::Engine;
use serde_json::{json, Value};
use std::hash::{Hash, Hasher};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct Entry {
    id: String,
    tool: &'static str,
    path: PathBuf,
    cwd: String,
    title: String,
    custom: bool, // the user named it; an ai-title no longer replaces it
    prompt: String,
    last_prompt: String,
    started: u64,
    updated: u64,
    bytes: u64,
    offset: u64, // read up to here
    entry: String, // Claude Code's entrypoint: cli, claude-desktop, sdk-cli (headless: agents, scripts, this host)
    parent: String, // a Codex sub-agent thread: the thread that spawned it ("review" for review agents)
    reply: String, // the agent's last words
    seen: Vec<String>, // the files it changed, the most recently changed last
    add: u64,
    del: u64,
    open: Vec<Value>, // edits read before their result: counted once it is read, unless it says they failed
    place: Option<Place>, // worked out by placed() when id or cwd change, and again after PLACE_AGE
}

// A folder removed, or made a repository, since its row was placed shows within this.
const PLACE_AGE: Duration = Duration::from_secs(60);

// What a row shows about where a session ran, for the id and cwd it was worked out for.
struct Place {
    id: String,
    cwd: String,
    shown: String, // local_cwd(cwd)
    local: bool,
    root: String,
    resume: String,
    at: Instant,
}

impl Entry {
    fn new(path: &Path, tool: &'static str) -> Self {
        let id = match tool {
            "claude" => path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default(),
            "opencode" => sources::opencode_id(path).unwrap_or_default(),
            _ => String::new(), // Codex: from session_meta; pi: from its header
        };
        Entry { id, tool, path: path.into(), cwd: String::new(), title: String::new(), custom: false,
                prompt: String::new(), last_prompt: String::new(), started: 0, updated: 0, bytes: 0, offset: 0,
                entry: String::new(), parent: String::new(), reply: String::new(), seen: Vec::new(), add: 0, del: 0, open: Vec::new(),
                place: None }
    }

    fn placed(&mut self) {
        if self.place.as_ref().is_some_and(|p| p.id == self.id && p.cwd == self.cwd && p.at.elapsed() < PLACE_AGE) {
            return;
        }
        let shown = local_cwd(&self.cwd);
        let local = !shown.is_empty() && Path::new(&shown).is_dir();
        let root = if local { repo_root(&shown) } else { String::new() };
        let resume = resume_command(self.tool, &self.id, if local { &shown } else { "" });
        self.place = Some(Place { id: self.id.clone(), cwd: self.cwd.clone(), shown, local, root, resume, at: Instant::now() });
    }

    // A protocol 4 row; `full` (protocol 5) adds files, lastReply, resume, repo, root and local.
    fn summary(&self, own: &HashSet<String>, full: bool) -> Value {
        // refresh places every entry it keeps
        let (shown, local, root, resume) = match &self.place {
            Some(p) => (p.shown.as_str(), p.local, p.root.as_str(), p.resume.as_str()),
            None => (self.cwd.as_str(), false, "", ""),
        };
        let mut row = json!({"id": self.id, "tool": self.tool, "path": self.path, "cwd": shown,
               "title": self.title, "prompt": self.prompt, "lastPrompt": self.last_prompt,
               "started": self.started, "updated": self.updated, "bytes": self.bytes,
               "entry": self.entry, "parent": self.parent, "own": own.contains(&self.id)});
        if full {
            let names: Vec<&str> = self.seen.iter().rev().take(3).map(|p| base_name(p)).collect();
            let o = row.as_object_mut().unwrap();
            o.insert("files".into(), json!({"n": self.seen.len(), "add": self.add, "del": self.del, "names": names}));
            o.insert("lastReply".into(), json!(self.reply));
            o.insert("resume".into(), json!(resume));
            o.insert("repo".into(), json!(!root.is_empty()));
            o.insert("root".into(), json!(root));
            o.insert("local".into(), json!(local));
        }
        row
    }
}

fn base_name(p: &str) -> &str {
    p.rsplit(['/', '\\']).next().unwrap_or(p)
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

// The command that opens the session again in a terminal.
fn resume_command(tool: &str, id: &str, cwd: &str) -> String {
    let id = if id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') { id.to_string() } else { sh_quote(id) };
    let run = match tool {
        "codex" => format!("codex resume {id}"),
        "pi" => format!("pi --session {id}"),
        "opencode" => format!("opencode --session {id}"),
        _ => format!("claude --resume {id}"),
    };
    if cwd.is_empty() { run } else { format!("cd {} && {run}", sh_quote(cwd)) }
}

// The nearest folder at or above `cwd` that holds a git repository, or "". An empty `.git` folder
// (agents leave them in /tmp and $HOME) is not one: a repository has a HEAD, a worktree a `.git` file.
fn repo_root(cwd: &str) -> String {
    Path::new(cwd).ancestors().find(|p| { let g = p.join(".git"); g.is_file() || g.join("HEAD").exists() }).map_or(String::new(), |p| p.to_string_lossy().into_owned())
}

// The words and edits a scan keeps from items, in the order the conversation had them. `prompts`:
// also what you asked (Claude Code and Codex rollouts have lines of their own for that).
fn absorb(e: &mut Entry, items: &[Value], prompts: bool) {
    for it in items {
        // a refused or failed edit changed nothing, as changes() counts
        if it["error"] == true {
            continue;
        }
        for f in it["files"].as_array().into_iter().flatten() {
            let p = f["path"].as_str().unwrap_or("").to_string();
            e.seen.retain(|x| *x != p);
            e.seen.push(p);
            e.add += f["add"].as_u64().unwrap_or(0);
            e.del += f["del"].as_u64().unwrap_or(0);
        }
        if it["kind"] != "text" {
            continue;
        }
        let t = it["text"].as_str().unwrap_or("");
        if it["role"] == "assistant" {
            e.reply = collapse(t, 300);
        } else if prompts && it["role"] == "user" {
            if e.prompt.is_empty() {
                e.prompt = clip(t, 200);
            }
            e.last_prompt = clip(t, 200);
        }
    }
}

// Whitespace collapsed to single spaces, at most `n` chars (the last one an ellipsis when cut).
fn collapse(t: &str, n: usize) -> String {
    let mut out = String::new();
    let mut chars = 0;
    for w in t.split_whitespace() {
        if chars > 0 {
            out.push(' ');
            chars += 1;
        }
        out.push_str(w);
        chars += w.chars().count();
        if chars > n {
            return clip(&out, n - 1);
        }
    }
    out
}

fn forget(e: &mut Entry) {
    e.reply.clear();
    e.seen.clear();
    e.add = 0;
    e.del = 0;
    e.open.clear();
}

// absorb() for a scan that reads on from where it stopped: an edit whose result is not in these items waits in
// `open` for settle(), as the result is a line of its own that may come in a later scan.
fn absorb_edits(e: &mut Entry, items: Vec<Value>) {
    for it in items {
        if it["files"].is_array() && it.get("result").is_none() {
            e.open.push(json!({"id": it["id"], "files": it["files"]}));
        } else {
            absorb(e, &[it], false);
        }
    }
}

// The result of edit `id`, read: it counts unless it failed.
fn settle(e: &mut Entry, id: &str, failed: bool) {
    if let Some(i) = e.open.iter().position(|o| o["id"] == id) {
        let it = e.open.remove(i);
        if !failed {
            absorb(e, &[it], false);
        }
    }
}

#[derive(Default)]
pub struct History {
    entries: HashMap<PathBuf, Entry>,
    own: HashSet<String>, // sessions this host started (state/agent-host/own.txt): headless, but the user's
    loaded: bool,
    saved: u64, // when the index was last written (ms)
    opencode: ((u64, u64), Vec<Value>), // opencode's sessions, and the database's write times when they were read
}
// What refresh learned, kept across restarts: built from nothing it reads every transcript on disk
// (GBs, ~8 s), which the app waited on for its session list after each host restart.
fn index_file() -> PathBuf {
    cache_dir().join("index.json")
}
// An index written with fewer fields than this host keeps is read again from the start.
// 3: the words for search are kept beside it (said_of); a version 2 index is read again so they are written
// 4: Codex edits of lines that start "--" or "++" counted (23aebbe, shipped in 0.1.7 without a new version), refused
// and failed edits left out and kept open until their result (open), a first prompt found past injected context
const INDEX_VERSION: u64 = 4;

// ~/.cache/agent-host, or AGENT_HOST_CACHE (tests)
pub(crate) fn cache_dir() -> PathBuf {
    std::env::var_os("AGENT_HOST_CACHE").map(PathBuf::from).unwrap_or_else(|| home().join(".cache/agent-host"))
}
impl Entry {
    fn saved(&self) -> Value {
        json!({"id": self.id, "tool": self.tool, "path": self.path, "cwd": self.cwd, "title": self.title, "custom": self.custom,
               "prompt": self.prompt, "lastPrompt": self.last_prompt, "started": self.started, "updated": self.updated,
               "bytes": self.bytes, "offset": self.offset, "entry": self.entry, "parent": self.parent,
               "reply": self.reply, "seen": self.seen, "add": self.add, "del": self.del, "open": self.open})
    }
    fn restored(v: &Value) -> Option<Self> {
        let tool = match v["tool"].as_str()? { "claude" => "claude", "codex" => "codex", "pi" => "pi", "opencode" => "opencode", _ => return None };
        let text = |k: &str| v[k].as_str().unwrap_or("").to_string();
        let num = |k: &str| v[k].as_u64().unwrap_or(0);
        Some(Entry { id: text("id"), tool, path: PathBuf::from(v["path"].as_str()?), cwd: text("cwd"), title: text("title"),
                     custom: v["custom"].as_bool().unwrap_or(false), prompt: text("prompt"), last_prompt: text("lastPrompt"),
                     started: num("started"), updated: num("updated"), bytes: num("bytes"), offset: num("offset"),
                     entry: text("entry"), parent: text("parent"), reply: text("reply"),
                     seen: v["seen"].as_array().into_iter().flatten().filter_map(|s| s.as_str().map(str::to_string)).collect(),
                     add: num("add"), del: num("del"), open: v["open"].as_array().cloned().unwrap_or_default(), place: None })
    }
}

pub fn own_file() -> PathBuf {
    home().join(".local/state/agent-host/own.txt")
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_default())
}

impl History {
    pub fn refresh(&mut self) {
        if !self.loaded {
            self.loaded = true;
            let saved: Value = fs::read(index_file()).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or(Value::Null);
            if saved["version"] == INDEX_VERSION {
                for e in saved["entries"].as_array().into_iter().flatten().filter_map(Entry::restored) {
                    self.entries.insert(e.path.clone(), e);
                }
            }
        }
        let mut scanned = false;
        let mut files: Vec<(PathBuf, &'static str)> = Vec::new();
        for dir in list_dir(&home().join(".claude/projects")) {
            files.extend(list_dir(&dir).into_iter().filter(|f| f.extension().is_some_and(|e| e == "jsonl")).map(|f| (f, "claude")));
        }
        walk(&home().join(".codex/sessions"), 4, &mut |f| {
            let name = f.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            if name.starts_with("rollout-") && name.ends_with(".jsonl") {
                files.push((f.to_path_buf(), "codex"));
            }
        });
        for dir in list_dir(&sources::pi_dir()) {
            files.extend(list_dir(&dir).into_iter().filter(|f| f.extension().is_some_and(|e| e == "jsonl")).map(|f| (f, "pi")));
        }
        // opencode's session list is read again only after the database was written
        let mark = sources::opencode_mark();
        if mark != self.opencode.0 {
            if let Some(rows) = sources::opencode_sessions() {
                self.opencode = (mark, rows);
            }
        }
        let oc: Vec<(PathBuf, Value)> = self.opencode.1.iter()
            .map(|r| (sources::opencode_key(r["id"].as_str().unwrap_or("")), r.clone())).collect();
        let keep: HashSet<&PathBuf> = files.iter().map(|(p, _)| p).chain(oc.iter().map(|(p, _)| p)).collect();
        let before = self.entries.len();
        self.entries.retain(|p, _| keep.contains(p));
        let dropped = self.entries.len() != before;
        let mut changed: Vec<PathBuf> = Vec::new();
        for (key, r) in &oc {
            let e = self.entries.entry(key.clone()).or_insert_with(|| Entry::new(key, "opencode"));
            e.cwd = r["directory"].as_str().unwrap_or("").into();
            e.title = r["title"].as_str().unwrap_or("").into();
            e.started = r["time_created"].as_u64().unwrap_or(0);
            e.updated = r["time_updated"].as_u64().unwrap_or(0);
            // offset: the time_updated its words and edits were read at
            if e.offset != e.updated {
                changed.push(key.clone());
            }
        }
        let ids: Vec<String> = changed.iter().map(|k| self.entries[k].id.clone()).collect();
        // a failed read leaves them to the next refresh
        if let Some(found) = if ids.is_empty() { None } else { sources::opencode_scan(&ids) } {
            for k in &changed {
                let e = self.entries.get_mut(k).unwrap();
                e.prompt.clear();
                e.last_prompt.clear();
                forget(e);
                absorb(e, found.get(&e.id).map_or(&[][..], |v| v), true);
                e.offset = e.updated;
            }
            scanned = true;
        }
        let names = codex_names();
        self.own = fs::read_to_string(own_file()).unwrap_or_default().lines().map(str::to_string).collect();
        for (path, tool) in &files {
            let Ok(meta) = fs::metadata(path) else { continue };
            let e = self.entries.entry(path.clone()).or_insert_with(|| Entry::new(path, tool));
            if meta.len() < e.offset {
                *e = Entry::new(path, tool); // rewritten, not appended
            }
            if meta.len() != e.bytes {
                scan(e);
                e.bytes = meta.len();
                scanned = true;
            }
            e.updated = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map_or(0, |d| d.as_millis() as u64);
            if let Some(n) = names.get(&e.id) {
                e.title = n.clone();
            }
        }
        for e in self.entries.values_mut() {
            e.placed();
        }
        if scanned || dropped {
            let keep: HashSet<String> = self.entries.keys().map(|p| said_name(p)).collect();
            for f in list_dir(&said_dir()) {
                if f.file_name().is_some_and(|n| !keep.contains(n.to_string_lossy().as_ref())) {
                    let _ = fs::remove_file(f);
                }
            }
        }
        // written at most every two minutes: a restart reads on from the saved offsets
        let t = now();
        if scanned && t.saturating_sub(self.saved) > 120_000 {
            self.saved = t;
            let all: Vec<Value> = self.entries.values().map(Entry::saved).collect();
            let file = index_file();
            let tmp = file.with_extension("tmp");
            let _ = fs::create_dir_all(file.parent().unwrap());
            if fs::write(&tmp, json!({"version": INDEX_VERSION, "entries": all}).to_string()).is_ok() {
                let _ = fs::rename(&tmp, &file);
            }
        }
    }

    pub fn list(&self) -> Vec<Value> {
        self.list_for(false)
    }

    // `new_agents`: a protocol 5 client, which also gets pi and opencode sessions (an older one
    // would resume them with claude) and the protocol 5 fields. Those fields add about half to the
    // reply, which the app parses on its GUI thread, so older clients do not get them.
    pub fn list_for(&self, new_agents: bool) -> Vec<Value> {
        let mut all: Vec<&Entry> = self.entries.values().filter(|e| !e.id.is_empty() && (new_agents || matches!(e.tool, "claude" | "codex"))).collect();
        all.sort_by(|a, b| b.updated.cmp(&a.updated));
        all.into_iter().map(|e| e.summary(&self.own, new_agents)).collect()
    }

    pub fn summaries(&self) -> HashMap<PathBuf, (u64, Value)> {
        self.summaries_for(false)
    }

    pub fn summaries_for(&self, new_agents: bool) -> HashMap<PathBuf, (u64, Value)> {
        self.entries.iter().filter(|(_, e)| new_agents || matches!(e.tool, "claude" | "codex"))
            .map(|(p, e)| (p.clone(), (e.updated, e.summary(&self.own, new_agents)))).collect()
    }

    pub fn locate(&self, id: &str) -> Option<(PathBuf, &'static str)> {
        self.entries.values().find(|e| e.id == id).map(|e| (e.path.clone(), e.tool))
    }
}

fn list_dir(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir).map(|r| r.flatten().map(|e| e.path()).collect()).unwrap_or_default()
}

fn walk(dir: &Path, depth: u32, f: &mut dyn FnMut(&Path)) {
    for p in list_dir(dir) {
        if p.is_dir() {
            if depth > 0 {
                walk(&p, depth - 1, f);
            }
        } else {
            f(&p);
        }
    }
}

fn codex_names() -> HashMap<String, String> {
    let mut names = HashMap::new();
    let Ok(text) = fs::read_to_string(home().join(".codex/session_index.jsonl")) else { return names };
    for line in text.lines() {
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            if let (Some(id), Some(n)) = (v["id"].as_str(), v["thread_name"].as_str()) {
                names.insert(id.to_string(), n.to_string()); // later lines win
            }
        }
    }
    names
}

// Codex sessions came from Windows: C:\dev\Wedding is ~/dev/Wedding here when that exists.
fn local_cwd(cwd: &str) -> String {
    if let Some(rest) = cwd.strip_prefix("C:\\").or_else(|| cwd.strip_prefix("c:\\")) {
        let p = home().join(rest.replace('\\', "/"));
        if p.is_dir() {
            return p.to_string_lossy().into_owned();
        }
    }
    cwd.to_string()
}

fn lines(path: &Path, from: u64, each: impl FnMut(&[u8])) -> u64 {
    lines_between(path, from, u64::MAX, each)
}

// lines(), stopping at the first line that starts at or after `to`
fn lines_between(path: &Path, from: u64, to: u64, mut each: impl FnMut(&[u8])) -> u64 {
    let Ok(mut f) = File::open(path) else { return from };
    if f.seek(SeekFrom::Start(from)).is_err() {
        return from;
    }
    let mut r = BufReader::with_capacity(1 << 20, f);
    let mut buf = Vec::new();
    let mut at = from;
    while at < to {
        buf.clear();
        match r.read_until(b'\n', &mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if buf.last() != Some(&b'\n') {
                    break; // a line still being written: read it next time
                }
                at += n as u64;
                each(&buf);
            }
        }
    }
    at
}

fn scan(e: &mut Entry) {
    let path = e.path.clone();
    let from = e.offset;
    match e.tool {
        "claude" => {
            let mut rewound = false;
            let mut words = Vec::new();
            let mut said = String::new();
            e.offset = lines(&path, from, |line| {
                rewound |= line.starts_with(br#"{"type":"last-prompt""#) && contains(line, br#""rewound":true"#);
                said_of(line, "claude", false, &mut said);
                claude_meta(e, line, &mut words)
            });
            said_write(&path, from == 0, &said);
            if let Ok(v) = serde_json::from_slice::<Value>(&words) {
                let mut items = Vec::new();
                claude_item(&v, &mut items, &mut HashMap::new(), false);
                absorb(e, &items, false);
            }
            // a rewind drops the turns it went back over: the words and edits are counted again
            // on the branch that ends the conversation, as changes() counts them
            if rewound {
                forget(e);
                absorb_edits(e, transcript_from(&path, "claude", 0, false).0);
                // the words of the turns it went back over go too
                let mut all = String::new();
                lines(&path, 0, |line| said_of(line, "claude", false, &mut all));
                said_write(&path, true, &all);
            }
        }
        "pi" => sources::pi_scan(e),
        _ => {
            let v2 = codex_v2(&path);
            let mut words = Vec::new();
            let mut said = String::new();
            e.offset = lines(&path, from, |line| {
                said_of(line, "codex", v2, &mut said);
                codex_meta(e, line, v2, &mut words)
            });
            said_write(&path, from == 0, &said);
            if let Ok(v) = serde_json::from_slice::<Value>(&words) {
                let mut items = Vec::new();
                codex_line(&v, v2, &mut items, &mut HashMap::new(), false);
                absorb(e, &items, false);
            }
        }
    }
}

fn str_field(line: &[u8], key: &str) -> Option<String> {
    serde_json::from_slice::<Value>(line).ok()?.get(key)?.as_str().map(str::to_string)
}

// `words`: the newest line with the agent's words, kept for scan() to read once at the end.
fn claude_meta(e: &mut Entry, line: &[u8], words: &mut Vec<u8>) {
    let head = &line[..line.len().min(48)];
    // this host's rename wrote the line with its keys sorted ({"customTitle":…,"type":"custom-title"})
    if head.starts_with(br#"{"type":"custom-title""#) || (head.starts_with(br#"{"customTitle""#) && contains(line, br#""type":"custom-title"}"#)) {
        if let Some(t) = str_field(line, "customTitle") {
            e.title = t;
            e.custom = true;
        }
    } else if head.starts_with(br#"{"type":"ai-title""#) {
        if !e.custom {
            if let Some(t) = str_field(line, "aiTitle") {
                e.title = t;
            }
        }
    } else if head.starts_with(br#"{"type":"last-prompt""#) {
        if let Some(t) = str_field(line, "lastPrompt") {
            e.last_prompt = clip(&t, 200);
        }
    } else if !head.starts_with(br#"{"type":""#) && (e.cwd.is_empty() || e.prompt.is_empty() || e.started == 0 || e.entry.is_empty()) {
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        if e.entry.is_empty() {
            e.entry = v["entrypoint"].as_str().unwrap_or("").into();
        }
        if e.cwd.is_empty() {
            e.cwd = v["cwd"].as_str().unwrap_or("").into();
        }
        if e.started == 0 {
            e.started = parse_ts(v["timestamp"].as_str().unwrap_or(""));
        }
        if e.prompt.is_empty() && v["type"] == "user" && v["isMeta"] != true && v["isSidechain"] != true {
            // the first block that is something typed: an IDE's open file or a system reminder may come before it
            let c = &v["message"]["content"];
            let blocks = c.as_str().into_iter().chain(c.as_array().into_iter().flatten().filter(|b| b["type"] == "text").filter_map(|b| b["text"].as_str()));
            if let Some(t) = blocks.map(said).find_map(|s| if let Said::Text(t) = s { Some(t) } else { None }) {
                e.prompt = clip(&t, 200);
            }
        }
    }
    // the agent's words and its edits, from assistant lines (the role sits in the first 600 bytes).
    // Claude Code writes one content block per line, and the block opens within the first 800
    // bytes: a text block or an editing tool is told from the bytes after it. Edits are parsed as
    // transcript_from parses them.
    if !head.starts_with(br#"{"type":""#) {
        let start = &line[..line.len().min(600)];
        if contains(start, br#""role":"assistant""#) && !contains(start, br#""isSidechain":true"#) {
            let (text, edit) = match find(&line[..line.len().min(2048)], br#""content":[{"type":""#) {
                Some(i) => {
                    let block = &line[i + 20..line.len().min(i + 320)];
                    (block.starts_with(br#"text""#), block.starts_with(br#"tool_use""#) && EDITS.iter().any(|t| contains(block, t)))
                }
                None => (contains(line, br#""type":"text""#), EDITS.iter().any(|t| contains(line, t))),
            };
            if text {
                words.clear();
                words.extend_from_slice(line);
            }
            if edit {
                if let Ok(v) = serde_json::from_slice::<Value>(line) {
                    let mut items = Vec::new();
                    claude_item(&v, &mut items, &mut HashMap::new(), false);
                    absorb_edits(e, items);
                }
            }
        } else if !e.open.is_empty() && contains(line, br#""type":"tool_result""#) {
            let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
            if v["type"] == "user" {
                for b in v["message"]["content"].as_array().into_iter().flatten().filter(|b| b["type"] == "tool_result") {
                    settle(e, b["tool_use_id"].as_str().unwrap_or(""), b["is_error"] == true);
                }
            }
        }
    }
}

const EDITS: [&[u8]; 4] = [br#""name":"Edit""#, br#""name":"Write""#, br#""name":"MultiEdit""#, br#""name":"NotebookEdit""#];

// Rollouts written by Codex since 0.15x number their lines ("ordinal") and record each finished
// item as an item_completed event, which is what they are read from.
fn codex_v2(path: &Path) -> bool {
    let mut head = [0u8; 160];
    let n = File::open(path).and_then(|mut f| std::io::Read::read(&mut f, &mut head)).unwrap_or(0);
    contains(&head[..n], br#""ordinal":"#)
}

// `last`: as claude_meta's `words`.
fn codex_meta(e: &mut Entry, line: &[u8], v2: bool, last: &mut Vec<u8>) {
    let head = &line[..line.len().min(120)];
    let start = &line[..line.len().min(400)];
    // the lines a transcript_from item with words or edits comes from
    let (words, wanted) = if v2 {
        let done = contains(start, br#""type":"item_completed""#);
        (done && contains(start, br#""item":{"type":"AgentMessage""#),
         done && (contains(start, br#""item":{"type":"UserMessage""#) || contains(start, br#""item":{"type":"FileChange""#)))
    } else {
        let item = contains(head, br#""type":"response_item""#);
        (item && contains(start, br#""type":"message""#) && contains(start, br#""role":"assistant""#),
         item && contains(start, br#""name":"apply_patch""#))
    };
    if words {
        last.clear();
        last.extend_from_slice(line);
        return;
    }
    if wanted {
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            let mut items = Vec::new();
            codex_line(&v, v2, &mut items, &mut HashMap::new(), false);
            // a v2 FileChange item is a finished edit; a patch's output is a line of its own
            if v2 { absorb(e, &items, v2) } else { absorb_edits(e, items) }
        }
        return;
    }
    if !v2 && !e.open.is_empty() && contains(head, br#""type":"response_item""#) && contains(start, br#"_call_output""#) {
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            let p = &v["payload"];
            settle(e, p["call_id"].as_str().unwrap_or(""), codex_exit_failed(&p["output"]));
        }
        return;
    }
    // a forked rollout repeats its parent's session_meta lines after its own first line
    if contains(head, br#""type":"session_meta""#) {
        if !e.id.is_empty() {
            return;
        }
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            let p = &v["payload"];
            e.id = p["id"].as_str().unwrap_or("").into();
            e.cwd = p["cwd"].as_str().unwrap_or("").into();
            let sub = &p["source"]["subagent"];
            if !sub.is_null() {
                e.parent = sub["thread_spawn"]["parent_thread_id"].as_str().or(sub.as_str()).unwrap_or("sub-agent").into();
            }
            e.started = parse_ts(p["timestamp"].as_str().unwrap_or(""));
        }
    } else if contains(head, br#""type":"event_msg","payload":{"type":"user_message""#) {
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            let t = v["payload"]["message"].as_str().unwrap_or("").trim().to_string();
            if e.prompt.is_empty() {
                e.prompt = clip(&t, 200);
            }
            e.last_prompt = clip(&t, 200);
        }
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    find(hay, needle).is_some()
}

// Horspool's search: the index scan looks through whole lines of GBs of transcripts.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    let n = needle.len();
    if n == 0 || hay.len() < n {
        return (n == 0).then_some(0);
    }
    let mut skip = [n.min(255) as u8; 256];
    for (i, &b) in needle[..n - 1].iter().enumerate() {
        skip[b as usize] = (n - 1 - i).min(255) as u8;
    }
    let mut i = 0;
    while i + n <= hay.len() {
        let last = hay[i + n - 1];
        if last == needle[n - 1] && &hay[i..i + n] == needle {
            return Some(i);
        }
        i += skip[last as usize] as usize;
    }
    None
}

fn clip(s: &str, n: usize) -> String {
    match s.char_indices().nth(n) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

// The text of an edit for the diff view, cut to whole lines: no "…" glued to a last line that looks like the
// file's own, and the number of lines left out (one at least, for a single long line), for the view to say.
fn clip_lines(s: &str, n: usize) -> (String, usize) {
    let Some((i, _)) = s.char_indices().nth(n) else { return (s.to_string(), 0) };
    let kept = match s[..i].rfind('\n') {
        Some(nl) if !s[i..].starts_with('\n') => &s[..nl],
        _ => &s[..i],
    };
    (kept.to_string(), (s.lines().count() - kept.lines().count()).max(1))
}

// A tool result is cut in the middle: a failing build or test says why at its end, so the head and the tail stay
// (the tail the larger part) and a line between says how much is left out.
fn clip_result(s: &str, n: usize) -> String {
    let total = s.chars().count();
    if total <= n {
        return s.to_string();
    }
    let head = n / 3;
    let tail = n - head;
    let h = s.char_indices().nth(head).map_or(s.len(), |(i, _)| i);
    let t = s.char_indices().nth(total - tail).map_or(s.len(), |(i, _)| i);
    format!("{}\n… {} characters left out …\n{}", &s[..h], total - head - tail, &s[t..])
}

fn first_text(content: &Value) -> Option<String> {
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    content.as_array()?.iter().find(|b| b["type"] == "text").and_then(|b| b["text"].as_str()).map(str::to_string)
}

// What a user line in a transcript is: something typed, a slash command, or harness noise.
enum Said {
    Text(String),
    Command(String),
    Note(&'static str),
    Nothing,
}

// The context Claude Code injects into the user's turn, by the tag each block opens with.
const INJECTED: [&str; 8] = ["system-reminder", "command-message", "local-command", "task-notification",
                             "bash-stdout", "bash-stderr", "artifact-content", "user-prompt-submit-hook"];

fn injected(t: &str) -> bool {
    let rest = &t[1..];
    let name = &rest[..rest.find(|c: char| c == '>' || c.is_whitespace()).unwrap_or(rest.len())];
    name.starts_with("ide_") || INJECTED.iter().any(|k| name.starts_with(k))
}

fn said(s: &str) -> Said {
    let t = s.trim();
    // A built-in command opens its block with <command-name>, a skill or custom one with
    // <command-message>, so the name is looked for in both. Anything else starting with '<' that
    // is not injected context is something a person typed - a question about a tag, or the CLI's
    // <pasted_content> block - and used to disappear from the conversation.
    if t.starts_with('<') {
        if t.starts_with("<command-name>") || t.starts_with("<command-message>") {
            if let Some((_, rest)) = t.split_once("<command-name>") {
                let name = rest.split("</command-name>").next().unwrap_or("").to_string();
                let args = t.split("<command-args>").nth(1).and_then(|a| a.split("</command-args>").next()).unwrap_or("").trim();
                return Said::Command(if args.is_empty() { name } else { format!("{name} {args}") });
            }
        }
        // a `!` shell line typed in the terminal: shown as the command it ran (its output stays out)
        if let Some(rest) = t.strip_prefix("<bash-input>") {
            return Said::Command(format!("! {}", rest.split("</bash-input>").next().unwrap_or("").trim()));
        }
        if injected(t) {
            return Said::Nothing;
        }
    }
    if t.starts_with("[Request interrupted") {
        return Said::Note("Interrupted");
    }
    if t.is_empty() || t.starts_with("Caveat: ") {
        return Said::Nothing;
    }
    Said::Text(t.to_string())
}

pub fn parse_ts(s: &str) -> u64 {
    let n = |a: usize, l: usize| s.get(a..a + l).and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
    if s.len() < 19 {
        return 0;
    }
    let (y, m, d) = (n(0, 4), n(5, 2), n(8, 2));
    let ms = if s.as_bytes().get(19) == Some(&b'.') { n(20, 3) } else { 0 };
    // days from the civil date (Howard Hinnant's algorithm)
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    ((days * 86400 + n(11, 2) * 3600 + n(14, 2) * 60 + n(17, 2)) * 1000 + ms).max(0) as u64
}

// ── search ─────────────────────────────────────────────────────────────────────

// ── the words, for search ─────────────────────────────────────────────────────

// Search read every transcript for each query (rg over ~15 GB here: 7.5 s, from disk every time, since that
// much does not stay cached). Refresh reads each transcript once, on from its offset: the lines with words on
// them (what a person typed or the agent said, as said_line() reads them) are also written to a file of
// their own, one text a line, and search reads those. A transcript read from its start is written anew.
fn said_dir() -> PathBuf {
    cache_dir().join("said")
}
fn said_name(path: &Path) -> String {
    format!("{}.txt", path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default())
}
// The words on one transcript line. A byte check first: most of a transcript is tool traffic, never words.
fn said_of(line: &[u8], tool: &str, v2: bool, out: &mut String) {
    let start = &line[..line.len().min(2000)];
    // Claude Code writes the line's own "type" after the message: the role, inside it, comes first
    let wanted = if tool == "claude" {
        (contains(start, br#""role":"user""#) && !contains(start, br#""tool_result""#))
            || (contains(start, br#""role":"assistant""#) && contains(line, br#""type":"text""#))
    } else {
        contains(start, br#"message""#) || contains(start, br#"Message""#)
    };
    if !wanted {
        return;
    }
    let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
    let mut items = Vec::new();
    let mut calls = HashMap::new();
    if tool == "claude" {
        claude_item(&v, &mut items, &mut calls, false);
    } else {
        codex_line(&v, v2, &mut items, &mut calls, false);
    }
    for t in items.iter().filter(|i| i["kind"] == "text").filter_map(|i| i["text"].as_str()) {
        let one = t.split_whitespace().collect::<Vec<_>>().join(" ");
        if !one.is_empty() {
            out.push_str(&one);
            out.push('\n');
        }
    }
}
fn said_write(path: &Path, fresh: bool, text: &str) {
    if !fresh && text.is_empty() {
        return;
    }
    let dir = said_dir();
    let _ = fs::create_dir_all(&dir);
    let file = dir.join(said_name(path));
    let opened = if fresh { File::create(&file) } else { fs::OpenOptions::new().create(true).append(true).open(&file) };
    if let Ok(mut f) = opened {
        let _ = f.write_all(text.as_bytes());
    }
}

pub fn search(q: &str, known: &HashMap<PathBuf, (u64, Value)>) -> Vec<Value> {
    let q = q.trim();
    if q.chars().count() < 2 {
        return Vec::new();
    }
    let needle = q.to_lowercase();
    let mut hits: HashMap<PathBuf, String> = HashMap::new();
    // Claude Code and Codex: the words refresh keeps (said_of), one file per transcript
    let by_name: HashMap<String, &PathBuf> = known.keys().map(|p| (said_name(p), p)).collect();
    let said = Command::new("rg")
        .args(["--no-heading", "--with-filename", "--text", "--fixed-strings", "--ignore-case", "--max-count", "1", "--", q])
        .arg(said_dir())
        .output();
    for line in said.as_ref().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default().lines() {
        let Some((file, text)) = line.split_once(".txt:") else { continue };
        let name = format!("{}.txt", file.rsplit('/').next().unwrap_or(""));
        if let (Some(path), Some(snippet)) = (by_name.get(&name), snippet_around(text, &needle)) {
            hits.insert((*path).clone(), snippet);
        }
    }
    // pi: its few transcripts read as before. Two passes: rg -l finds the files with the words anywhere, then
    // each matching line is read and kept only when the words are in what a person typed or the agent said
    // (every transcript also carries injected context). --text: one NUL byte makes rg skip a file as binary.
    if sources::pi_dir().is_dir() {
        let files = Command::new("rg")
            .args(["--files-with-matches", "--text", "--fixed-strings", "--ignore-case", "--glob", "*.jsonl", "--", q])
            .arg(sources::pi_dir())
            .output();
        let mut matched: Vec<(u64, PathBuf)> = files.as_ref().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default()
            .lines().map(PathBuf::from).filter_map(|p| known.get(&p).map(|(u, _)| (*u, p))).collect();
        matched.sort_by(|a, b| b.0.cmp(&a.0));
        matched.truncate(150);
        let out = if matched.is_empty() { None } else {
            Command::new("rg")
                .args(["--no-heading", "--with-filename", "--byte-offset", "--text", "--fixed-strings", "--ignore-case", "--max-count", "40",
                       "--max-columns", "1", "--max-columns-preview", "--"])
                .arg(q)
                .args(matched.iter().map(|(_, p)| p))
                .output().ok()
        };
        for line in out.as_ref().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default().lines() {
            let mut parts = line.splitn(3, ':');
            let (Some(path), Some(offset)) = (parts.next(), parts.next().and_then(|o| o.parse::<u64>().ok())) else { continue };
            let path = PathBuf::from(path);
            if hits.contains_key(&path) {
                continue;
            }
            if let Some(snippet) = said_line(&path, offset, "pi").and_then(|t| snippet_around(&t, &needle)) {
                hits.insert(path, snippet);
            }
        }
    }
    // opencode's words are rows in its database
    if known.keys().any(|p| sources::is_opencode_key(p)) {
        for (id, _, text) in sources::opencode_search(q) {
            let key = sources::opencode_key(&id);
            if known.contains_key(&key) {
                hits.insert(key, snippet_around(&text, &needle).unwrap_or_else(|| clip(&text, 140)));
            }
        }
    }
    let mut found: Vec<(u64, Value)> = hits.into_iter().map(|(p, snippet)| {
        let (updated, s) = &known[&p];
        let mut v = s.clone();
        v["snippet"] = Value::String(snippet);
        (*updated, v)
    }).collect();
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().take(100).map(|(_, v)| v).collect()
}

// The words a person or the agent wrote on one transcript line, or None for injected context,
// tool traffic and metadata.
fn said_line(path: &Path, offset: u64, tool: &str) -> Option<String> {
    let mut f = File::open(path).ok()?;
    f.seek(SeekFrom::Start(offset)).ok()?;
    let mut buf = Vec::new();
    BufReader::new(f).read_until(b'\n', &mut buf).ok()?;
    let v: Value = serde_json::from_slice(&buf).ok()?;
    let mut items = Vec::new();
    let mut calls = HashMap::new();
    match tool {
        "claude" => claude_item(&v, &mut items, &mut calls, false),
        "pi" => sources::pi_item(&v, "", &mut items, &mut calls, false),
        _ => codex_line(&v, codex_v2(path), &mut items, &mut calls, false),
    }
    let text: Vec<&str> = items.iter().filter(|i| i["kind"] == "text").filter_map(|i| i["text"].as_str()).collect();
    if text.is_empty() { None } else { Some(text.join(" ")) }
}

fn snippet_around(text: &str, needle: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let lower: Vec<char> = text.to_lowercase().chars().collect();
    let n: Vec<char> = needle.chars().collect();
    if lower.len() != chars.len() || n.is_empty() {
        return text.to_lowercase().contains(needle).then(|| clip(text, 140));
    }
    let at = (0..=lower.len().saturating_sub(n.len())).find(|&i| lower[i..].starts_with(&n))?;
    let from = at.saturating_sub(50);
    let to = (at + n.len() + 90).min(chars.len());
    let body: String = chars[from..to].iter().collect();
    let body = body.split_whitespace().collect::<Vec<_>>().join(" ");
    Some(format!("{}{}{}", if from > 0 { "…" } else { "" }, body, if to < chars.len() { "…" } else { "" }))
}

// ── a transcript as messages ───────────────────────────────────────────────────
//
// Items: {role: user|assistant|system, kind: text|tool|command|note|image, text?, tool?,
// summary?, stat?, result?, error?, ts}. A tool call carries its result, joined by id.

// Items from byte `from` on, and where reading stopped (the start of any line still being
// written), so a follower can ask again from there. A result whose call was read earlier comes
// back as {kind: "result", id, result, error, images} for the reader to attach - it is the only
// place those images are ever sent. `media` off skips extracting them, for a caller that wants
// the text only.
pub fn transcript_from(path: &Path, tool: &str, from: u64, media: bool) -> (Vec<Value>, u64) {
    let mut items: Vec<Value> = Vec::new();
    let mut calls: HashMap<String, usize> = HashMap::new();
    let end;
    if tool == "claude" {
        // A rewind leaves the turns it went back over in the file and goes on from an earlier
        // message. Such a file is read again for the branch that ends the conversation: each
        // item remembers its record (rec), and the records off that branch are dropped.
        let mut rewound = false;
        let mut marker_leaf: Option<String> = None;   // a rewind with nothing written after it yet
        end = lines(path, from, |line| {
            if line.starts_with(br#"{"type":"last-prompt""#) {
                if contains(line, br#""rewound":true"#) {
                    rewound = true;
                    marker_leaf = str_field(line, "leafUuid");
                }
                return;
            }
            // lines that open with their type are metadata, except Claude Code's queue (what you typed while it worked)
            if !line.starts_with(br#"{"type":""#) || line.starts_with(br#"{"type":"queue-operation""#) {
                if let Ok(v) = serde_json::from_slice::<Value>(line) {
                    if v["uuid"].is_string() && v["isSidechain"] != true {
                        marker_leaf = None;
                    }
                    let first = items.len();
                    claude_item(&v, &mut items, &mut calls, media);
                    // the record each item came from: a rewind names one of your messages by it
                    for it in items[first..].iter_mut() {
                        it["rec"] = v["uuid"].clone();
                    }
                }
            }
        });
        if rewound && from == 0 {
            let keep = active_branch(path, marker_leaf);
            items.retain(|it| it["rec"].as_str().map_or(true, |r| keep.contains(r)));
        }
    } else {
        (items, end) = read_tagged(path, tool, from, media);
        for it in items.iter_mut() {
            if let Some(o) = it.as_object_mut() {
                o.remove("_at");
            }
        }
    }
    (items, end)
}

// A Codex, pi or opencode session's items from `from` on, each tagged with where its record starts
// ("_at": a byte, or for opencode a message's number), and where reading stopped.
fn read_tagged(path: &Path, tool: &str, from: u64, media: bool) -> (Vec<Value>, u64) {
    match tool {
        "pi" => sources::pi_read(path, from, media),
        "opencode" => sources::opencode_read(path, from),
        _ => {
            let v2 = codex_v2(path);
            let mut items: Vec<Value> = Vec::new();
            let mut calls: HashMap<String, usize> = HashMap::new();
            let mut at = from;
            let end = lines(path, from, |line| {
                let start = at;
                at += line.len() as u64;
                let head = &line[..line.len().min(120)];
                if contains(head, br#""type":"response_item""#) || contains(head, br#""type":"user_message""#) || contains(head, br#""type":"compacted""#)
                    || (v2 && contains(&line[..line.len().min(160)], br#""type":"item_completed""#)) {
                    if let Ok(v) = serde_json::from_slice::<Value>(line) {
                        let first = items.len();
                        codex_line(&v, v2, &mut items, &mut calls, media);
                        for it in items[first..].iter_mut() {
                            it["_at"] = json!(start);
                        }
                    }
                }
            });
            (items, end)
        }
    }
}

// The newest whole turns of any agent's session, as transcript_tail gives them for Claude Code:
// the items, where that page starts (0: nothing before it) and where following goes on.
pub fn transcript_tail_of(path: &Path, tool: &str, limit: usize, until: Option<u64>) -> (Vec<Value>, u64, u64) {
    if tool == "claude" {
        return transcript_tail(path, limit, until);
    }
    let (mut items, end) = read_tagged(path, tool, 0, true);
    let stop = until.map_or(end, |u| u.min(end));
    let at = |it: &Value| it["_at"].as_u64().unwrap_or(0);
    items.retain(|it| at(it) < stop);
    let want = items.len().saturating_sub(limit);
    let mut cut = items.iter().enumerate().rev()
        .find(|(i, it)| *i <= want && it["role"] == "user" && matches!(it["kind"].as_str(), Some("text" | "command" | "image")))
        .map_or(0, |(i, _)| i);
    // the page starts with its record's first item: the page before keeps only what is before that
    // record, so images sent with the text would otherwise be on neither page
    while cut > 0 && at(&items[cut - 1]) == at(&items[cut]) {
        cut -= 1;
    }
    let page = if cut == 0 { 0 } else { at(&items[cut]) };
    items.drain(..cut);
    for it in items.iter_mut() {
        if let Some(o) = it.as_object_mut() {
            o.remove("_at");
        }
    }
    (items, page, stop)
}

// The records on the branch that ends the conversation: from its last message (or the message a
// rewind went back to, when nothing has been written since) up through each record's parent, and
// across a compaction by its logical parent.
// The newest whole turns of a Claude Code transcript, read from its end: a long session's file runs
// to hundreds of MB and the app shows a few hundred items at a time. Reads back from `until` (the
// end of the file when None) in a window that widens until it holds `limit` items beginning with one
// of your messages, so no tool call is cut from its result. Returns the items, the byte that page
// starts at (the `until` of the page before it; 0 when there is none), and the end of the last
// whole line, where following goes on. A rewind after the page (records it went back over) is
// applied as transcript_from applies it.
pub fn transcript_tail(path: &Path, limit: usize, until: Option<u64>) -> (Vec<Value>, u64, u64) {
    let size = fs::metadata(path).map_or(0, |m| m.len());
    let stop = until.map_or(size, |u| u.min(size));
    // records after the page matter only when something after it was rewound
    let reach = if until.is_some() && rewind_after(path, stop) { u64::MAX } else { stop };
    let mut window: u64 = 4 << 20;
    loop {
        let from = next_line(path, stop.saturating_sub(window));
        let mut items: Vec<Value> = Vec::new();
        let mut calls: HashMap<String, usize> = HashMap::new();
        let mut turns: Vec<(usize, u64)> = Vec::new(); // (first item, line start) of each of your messages
        let mut parent: HashMap<String, String> = HashMap::new();
        let mut last: Option<String> = None;
        let mut rewound = false;
        let mut marker_leaf: Option<String> = None;
        let mut at = from;
        let end = lines_between(path, from, reach, |line| {
            let start = at;
            at += line.len() as u64;
            if line.starts_with(br#"{"type":"last-prompt""#) {
                if contains(line, br#""rewound":true"#) {
                    rewound = true;
                    marker_leaf = str_field(line, "leafUuid");
                }
                return;
            }
            if line.starts_with(br#"{"type":""#) && !line.starts_with(br#"{"type":"queue-operation""#) {
                return;
            }
            let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
            if let Some(u) = v["uuid"].as_str().filter(|_| v["isSidechain"] != true) {
                if let Some(p) = v["parentUuid"].as_str().or(v["logicalParentUuid"].as_str()) {
                    parent.insert(u.to_string(), p.to_string());
                }
                last = Some(u.to_string());
                marker_leaf = None;
            }
            if start >= stop {
                return;
            }
            let first = items.len();
            claude_item(&v, &mut items, &mut calls, true);
            for it in items[first..].iter_mut() {
                it["rec"] = v["uuid"].clone();
            }
            if items[first..].iter().any(|it| it["role"] == "user" && matches!(it["kind"].as_str(), Some("text" | "command" | "image"))) {
                turns.push((first, start));
            }
        });
        let want = items.len().saturating_sub(limit);
        let cut = turns.iter().rev().find(|t| t.0 <= want).copied();
        let (skip, page) = match cut {
            Some(c) => c,
            None if from == 0 => (0, 0),
            // a turn longer than the window: take it from its start once the window is wide
            None if window >= 64 << 20 && !turns.is_empty() => turns[0],
            // each pass reads the whole window again: too few items widens it by what they took so far (at
            // most eight times), not four times blind (an 18 MB page read 4, 16 and 64 MB: 115 ms)
            None if !items.is_empty() && items.len() < limit => {
                window = (window as f64 * limit as f64 / items.len() as f64 * 1.3).clamp(2.0 * window as f64, 8.0 * window as f64) as u64;
                continue;
            }
            None => {
                window *= 4;
                continue;
            }
        };
        items.drain(..skip);
        // nothing before the cut: this is the first page
        let page = if from == 0 && skip == 0 { 0 } else { page };
        if rewound {
            let mut keep = HashSet::new();
            let mut node = marker_leaf.or(last);
            while let Some(u) = node {
                if !keep.insert(u.clone()) {
                    break;
                }
                node = parent.get(&u).cloned();
            }
            items.retain(|it| it["rec"].as_str().map_or(true, |r| keep.contains(r)));
        }
        return (items, page, end.min(stop));
    }
}

// Whether a rewind was recorded at or after `from`.
fn rewind_after(path: &Path, from: u64) -> bool {
    let mut found = false;
    lines(path, from, |line| {
        if !found && line.starts_with(br#"{"type":"last-prompt""#) && contains(line, br#""rewound":true"#) {
            found = true;
        }
    });
    found
}

// The start of the first whole line at or after `pos`.
fn next_line(path: &Path, pos: u64) -> u64 {
    if pos == 0 {
        return 0;
    }
    let Ok(mut f) = File::open(path) else { return pos };
    if f.seek(SeekFrom::Start(pos - 1)).is_err() {
        return pos;
    }
    let mut skip = Vec::new();
    match BufReader::new(f).read_until(b'\n', &mut skip) {
        Ok(n) => pos - 1 + n as u64,
        Err(_) => pos,
    }
}

fn active_branch(path: &Path, marker_leaf: Option<String>) -> HashSet<String> {
    let mut parent: HashMap<String, String> = HashMap::new();
    let mut last: Option<String> = None;
    lines(path, 0, |line| {
        if line.starts_with(br#"{"type":""#) || contains(&line[..line.len().min(400)], br#""isSidechain":true"#) {
            return;
        }
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        let Some(u) = v["uuid"].as_str().map(str::to_string) else { return };
        if let Some(p) = v["parentUuid"].as_str().or(v["logicalParentUuid"].as_str()) {
            parent.insert(u.clone(), p.to_string());
        }
        last = Some(u);
    });
    let mut keep = HashSet::new();
    let mut at = marker_leaf.or(last);
    while let Some(u) = at {
        if !keep.insert(u.clone()) {
            break;
        }
        at = parent.get(&u).cloned();
    }
    keep
}

fn push_said(items: &mut Vec<Value>, s: &str, ts: u64) {
    match said(s) {
        Said::Text(t) => items.push(json!({"role": "user", "kind": "text", "text": clip(&t, 20000), "ts": ts})),
        Said::Command(c) => items.push(json!({"role": "user", "kind": "command", "text": c, "ts": ts})),
        Said::Note(n) => items.push(json!({"role": "system", "kind": "note", "text": n, "ts": ts})),
        Said::Nothing => {}
    }
}

// A user line's text. A background task's report and a slash command's output are the harness speaking:
// each is a one-line note, or the reply after it reads as unprompted. The rest goes to said().
fn push_line(items: &mut Vec<Value>, s: &str, ts: u64) {
    let t = s.trim();
    let inner = |tag: &str| t.split(&format!("<{tag}>")).nth(1).and_then(|r| r.split(&format!("</{tag}>")).next())
        .map(|x| x.split_whitespace().collect::<Vec<_>>().join(" "));
    if t.starts_with("<task-notification>") {
        let text = inner("summary").filter(|x| !x.is_empty()).map_or("A background task finished".to_string(), |x| clip(&x, 160));
        items.push(json!({"role": "system", "kind": "note", "text": text, "ts": ts}));
    } else if t.starts_with("<local-command-stdout>") || t.starts_with("<local-command-stderr>") {
        if let Some(out) = inner("local-command-stdout").or_else(|| inner("local-command-stderr")).filter(|x| !x.is_empty()) {
            items.push(json!({"role": "system", "kind": "note", "text": clip(&out, 200), "ts": ts}));
        }
    } else {
        push_said(items, s, ts);
    }
}

fn image_item(items: &mut Vec<Value>, b: &Value, media: bool, ts: u64) {
    let pics = if media { images(&Value::Array(vec![b.clone()])) } else { Vec::new() };
    items.push(json!({"role": "user", "kind": "image", "images": pics, "ts": ts}));
}

// a compaction writes a boundary and a summary line: one note says it
fn compacted(items: &mut Vec<Value>, ts: u64) {
    if !items.iter().rev().take(6).any(|i| i["kind"] == "note" && i["text"] == "Conversation compacted") {
        items.push(json!({"role": "system", "kind": "note", "text": "Conversation compacted", "ts": ts}));
    }
}

static NEXT_TMP: AtomicU64 = AtomicU64::new(0);

// An image's base64 bytes as a file the app can show:
// ~/.cache/agent-host/media/<hash>.<ext>, written once per distinct image. None if it cannot be
// decoded or written.
fn media_file(data: &str, mime: &str) -> Option<PathBuf> {
    let ext = mime.rsplit('/').next().unwrap_or("png").replace("jpeg", "jpg");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    let dir = cache_dir().join("media");
    let path = dir.join(format!("{:016x}.{ext}", hasher.finish()));
    if !path.exists() {
        let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data) else { return None };
        let _ = fs::create_dir_all(&dir);
        // written under a name of its own and renamed: a write cut short would otherwise
        // stay in the cache forever, since the only guard is that the file exists
        let tmp = dir.join(format!("{:016x}.{}.part", hasher.finish(), NEXT_TMP.fetch_add(1, Ordering::Relaxed)));
        if fs::write(&tmp, bytes).is_err() || fs::rename(&tmp, &path).is_err() {
            let _ = fs::remove_file(&tmp);
            return None;
        }
    }
    Some(path)
}

// Images inside a message or a tool result (base64 blocks) as files the app can show.
fn images(c: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    for b in c.as_array().into_iter().flatten() {
        if b["type"] != "image" || b["source"]["type"] != "base64" {
            continue;
        }
        let Some(data) = b["source"]["data"].as_str() else { continue };
        let mime = b["source"]["media_type"].as_str().unwrap_or("image/png");
        if let Some(path) = media_file(data, mime) {
            out.push(Value::String(path.to_string_lossy().into_owned()));
        }
    }
    out
}

// opencode keeps an image the agent looked at as a tool attachment, and one you sent as a file
// part; both carry a data: URL (a file: URL is already a path). As a file the app can show: the
// path, or None.
fn media_from_url(url: &str) -> Option<Value> {
    if let Some(rest) = url.strip_prefix("data:") {
        let (meta, data) = rest.split_once(',')?;
        let mime = meta.strip_suffix(";base64")?;
        return media_file(data, mime).map(|p| Value::String(p.to_string_lossy().into_owned()));
    }
    url.strip_prefix("file://").map(|p| Value::String(p.to_string()))
}

// Nothing else removes what images() writes, and it is written again for every transcript ever
// read: 600 MB had built up before this was noticed. Anything still wanted is extracted again
// the next time its transcript is read.
pub fn prune_media(keep_days: u64) {
    let dir = cache_dir().join("media");
    let Some(cutoff) = SystemTime::now().checked_sub(Duration::from_secs(keep_days * 86400)) else { return };
    let (mut files, mut bytes) = (0u64, 0u64);
    for p in list_dir(&dir) {
        let Ok(m) = fs::metadata(&p) else { continue };
        if m.modified().is_ok_and(|t| t < cutoff) && fs::remove_file(&p).is_ok() {
            files += 1;
            bytes += m.len();
        }
    }
    if files > 0 {
        eprintln!("agent-host: media cache: dropped {files} files over {keep_days} days old ({} MB)", bytes / (1 << 20));
    }
}

fn result_text(c: &Value) -> String {
    if let Some(s) = c.as_str() {
        return s.to_string();
    }
    c.as_array().map(|a| a.iter().filter_map(|b| b["text"].as_str()).collect::<Vec<_>>().join("\n")).unwrap_or_default()
}

// Claude Code's result for a Write over a file that was there ("The file <path> has been updated successfully."); a new
// file's is "File created successfully at: <path>".
fn overwrote(result: &str) -> bool {
    result.starts_with("The file ") && result.contains(" has been updated")
}

fn attach(items: &mut Vec<Value>, calls: &HashMap<String, usize>, id: &str, content: &Value, error: bool, media: bool) {
    let text = result_text(content);
    let result = clip_result(text.trim(), 400);
    let pics = if media { images(content) } else { Vec::new() };
    match calls.get(id) {
        Some(&i) => {
            // a Write over a file that was there is an edit of it, not a new file
            if !error && items[i]["tool"] == "Write" && overwrote(&text) {
                for f in items[i]["files"].as_array_mut().into_iter().flatten() {
                    f["kind"] = json!("edit");
                }
            }
            items[i]["result"] = Value::String(result);
            items[i]["error"] = Value::Bool(error);
            if !pics.is_empty() {
                items[i]["images"] = Value::Array(pics);
            }
        }
        None => items.push(json!({"role": "assistant", "kind": "result", "id": id, "result": result, "error": error, "images": pics})),
    }
}

pub fn claude_item(v: &Value, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>, media: bool) {
    claude_item_of(v, items, calls, media, false);
}

// `side`: a sub-agent's own transcript, every line of which is a sidechain line
fn claude_item_of(v: &Value, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>, media: bool, side: bool) {
    if v["isSidechain"] == true && !side {
        return;
    }
    let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
    // the live stream marks what the harness injected `isSynthetic`; the transcript marks it `isMeta`
    let meta = v["isMeta"] == true || v["isSynthetic"] == true;
    match v["type"].as_str().unwrap_or("") {
        "user" if v["isCompactSummary"] == true => compacted(items, ts),
        // a Stop hook that sent the turn back (a goal not met yet): hidden, the reply after it read as unprompted.
        // Its text is "Stop hook feedback:\n[<hook or goal>]: <why>"; a note is one line, so it names the hook.
        "user" if meta && v["message"]["content"].as_str().is_some_and(|s| s.starts_with("Stop hook feedback:")) => {
            let s = v["message"]["content"].as_str().unwrap_or("");
            let name = s.split_once('[').and_then(|(_, r)| r.split_once("]:")).map_or("", |(n, _)| n.trim());
            let text = if name.is_empty() { "Stop hook sent it back".to_string() } else { format!("Stop hook · {}", clip(name, 80)) };
            items.push(json!({"role": "system", "kind": "note", "text": text, "ts": ts}));
        }
        // another agent's report handed to this session: the reply after it is to that, not unprompted
        "user" if meta && v["message"]["content"].as_str().is_some_and(|s| s.starts_with("Another Claude session sent a message")) => {
            items.push(json!({"role": "system", "kind": "note", "text": "An agent reported back", "ts": ts}));
        }
        "user" if !meta => {
            let c = &v["message"]["content"];
            if let Some(s) = c.as_str() {
                push_line(items, s, ts);
            }
            for b in c.as_array().into_iter().flatten() {
                match b["type"].as_str().unwrap_or("") {
                    "tool_result" => attach(items, calls, b["tool_use_id"].as_str().unwrap_or(""), &b["content"], b["is_error"] == true, media),
                    "text" => push_line(items, b["text"].as_str().unwrap_or(""), ts),
                    "image" => image_item(items, b, media, ts),
                    _ => {}
                }
            }
        }
        // what Claude Code writes in Claude's place (an API error, out of credits, "No response requested."):
        // a note, not Claude speaking with a "<synthetic>" model
        "assistant" if v["message"]["model"] == "<synthetic>" || v["isApiErrorMessage"] == true => {
            for b in v["message"]["content"].as_array().into_iter().flatten() {
                let t = b["text"].as_str().unwrap_or("").split_whitespace().collect::<Vec<_>>().join(" ");
                if !t.is_empty() && t != "No response requested." {
                    items.push(json!({"role": "system", "kind": "note", "text": clip(&t, 200), "ts": ts}));
                }
            }
        }
        "assistant" => {
            // how full the context was for this call: everything the model read
            let u = &v["message"]["usage"];
            let ctx = u["input_tokens"].as_u64().unwrap_or(0) + u["cache_read_input_tokens"].as_u64().unwrap_or(0)
                + u["cache_creation_input_tokens"].as_u64().unwrap_or(0);
            let first = items.len();
            for b in v["message"]["content"].as_array().into_iter().flatten() {
                match b["type"].as_str().unwrap_or("") {
                    "text" => {
                        let t = b["text"].as_str().unwrap_or("").trim();
                        if !t.is_empty() {
                            items.push(json!({"role": "assistant", "kind": "text", "text": clip(t, 40000), "ts": ts,
                                              "model": v["message"]["model"]}));
                        }
                    }
                    "tool_use" => {
                        let name = b["name"].as_str().unwrap_or("");
                        let id = b["id"].as_str().unwrap_or("");
                        calls.insert(id.into(), items.len());
                        let mut t = tool_item(name, &b["input"], ts);
                        t["id"] = Value::String(id.into());
                        items.push(t);
                    }
                    "thinking" => {
                        let t = b["thinking"].as_str().unwrap_or("").trim();
                        // Narration is prose to the user in a thinking block (the terminal draws it as text);
                        // the rest is reasoning. Claude Code sometimes writes a block again under a second message id.
                        let kind = if is_narration(b["signature"].as_str().unwrap_or("")) { "text" } else { "thinking" };
                        let text = clip(t, if kind == "text" { 40000 } else { 20000 });
                        let repeat = items.iter().rev().take(8).any(|i| i["kind"] == kind && i["text"] == text.as_str());
                        if !t.is_empty() && !repeat {
                            let mut it = json!({"role": "assistant", "kind": kind, "text": text, "ts": ts, "model": v["message"]["model"]});
                            if kind == "text" {
                                it["narration"] = json!(true);
                            }
                            items.push(it);
                        }
                    }
                    _ => {}
                }
            }
            if ctx > 0 {
                for it in items[first..].iter_mut() {
                    it["ctx"] = json!(ctx);
                }
            }
        }
        "system" if v["subtype"] == "compact_boundary" => compacted(items, ts),
        // a slash command run in the terminal (/model, /artifacts): the command, then its output as a note
        "system" if v["subtype"] == "local_command" => push_line(items, v["content"].as_str().unwrap_or(""), ts),
        // Something queued while Claude worked reaches it beside a tool result. Yours is said there (its text and
        // pasted images); a background task's report, another agent's or a goal's continuation is a note.
        "attachment" if v["attachment"]["type"] == "queued_command" => {
            let a = &v["attachment"];
            match a["origin"]["kind"].as_str() {
                Some("human") | None => {
                    if let Some(s) = a["prompt"].as_str() {
                        push_line(items, s, ts);
                    }
                    for b in a["prompt"].as_array().into_iter().flatten() {
                        match b["type"].as_str().unwrap_or("") {
                            "text" => push_line(items, b["text"].as_str().unwrap_or(""), ts),
                            "image" => image_item(items, b, media, ts),
                            _ => {}
                        }
                    }
                }
                Some("peer") => items.push(json!({"role": "system", "kind": "note", "text": "An agent reported back", "ts": ts})),
                Some("auto-continuation") => items.push(json!({"role": "system", "kind": "note", "text": "Continued toward the goal", "ts": ts})),
                _ => {}
            }
        }
        // Claude Code's queue of messages waiting for the turn to end (written as they are typed): passed on so
        // the app can show them waiting. Task notifications go through it too; the app keeps only what you wrote.
        "queue-operation" => {
            // clipped: a task notification can be 24 KB, and every page carries it
            let text = v["content"].as_str().map(|t| clip(t, 2000));
            items.push(json!({"role": "system", "kind": "queue", "op": v["operation"], "text": text, "ts": ts}));
        }
        _ => {}
    }
}

// A thinking block the server tagged as narration: its signature is base64 protobuf whose field 2 > 1 > 8
// holds the block's kind (read the way Claude Code 2.1.295 reads it; an unreadable signature is reasoning).
fn is_narration(sig: &str) -> bool {
    fn b64(s: &str) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(s.len() * 3 / 4);
        let (mut acc, mut bits) = (0u32, 0u32);
        for c in s.bytes() {
            let v = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' | b'-' => 62,
                b'/' | b'_' => 63,
                b'=' => break,
                _ => return None,
            };
            acc = (acc << 6) | v as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
            }
        }
        Some(out)
    }
    fn varint(b: &[u8], i: &mut usize) -> Option<u64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let c = *b.get(*i)?;
            *i += 1;
            v |= ((c & 0x7f) as u64) << shift;
            if c < 0x80 { return Some(v); }
            shift += 7;
            if shift > 63 { return None; }
        }
    }
    // the last length-delimited field `n`
    fn field(b: &[u8], n: u64) -> Option<&[u8]> {
        let (mut i, mut out) = (0usize, None);
        while i < b.len() {
            let key = varint(b, &mut i)?;
            match key & 7 {
                0 => { varint(b, &mut i)?; }
                1 => i += 8,
                5 => i += 4,
                2 => {
                    let len = varint(b, &mut i)? as usize;
                    let end = i.checked_add(len).filter(|e| *e <= b.len())?;
                    if key >> 3 == n { out = Some(&b[i..end]); }
                    i = end;
                }
                _ => return None,
            }
        }
        out
    }
    if sig.is_empty() { return false; }
    let Some(raw) = b64(sig) else { return false };
    field(&raw, 2).and_then(|f| field(f, 1)).and_then(|f| field(f, 8)) == Some(b"narration".as_slice())
}

// A command's exit status in a rollout without item events, from the lines Codex puts before the output: "Exit
// code: N" (shell), "Process exited with code N" after "Chunk ID:" and "Wall time:" (exec_command), or JSON whose
// metadata holds exit_code. A patch Codex could not apply, a command that could not start and one the person
// stopped say so in those lines instead. Anything else (no command, no status) did not fail.
fn codex_exit_failed(output: &Value) -> bool {
    const HEAD: [&str; 3] = ["Chunk ID:", "Wall time", "Original token count:"];
    const FAILED: [&str; 3] = ["apply_patch verification failed", "execution error:", "aborted by user"];
    let Some(text) = output.as_str() else { return false };
    let text = text.trim_start();
    if text.starts_with('{') {
        return serde_json::from_str::<Value>(text).is_ok_and(|j| j["metadata"]["exit_code"].as_i64().is_some_and(|n| n != 0));
    }
    for l in text.lines() {
        if let Some(rest) = l.strip_prefix("Exit code:").or_else(|| l.strip_prefix("Process exited with code")) {
            return rest.trim_start().split(|c: char| !(c.is_ascii_digit() || c == '-')).next().and_then(|n| n.parse::<i64>().ok()).is_some_and(|n| n != 0);
        }
        if FAILED.iter().any(|f| l.starts_with(f)) {
            return true;
        }
        if !HEAD.iter().any(|h| l.starts_with(h)) {
            break;
        }
    }
    false
}

fn codex_item(v: &Value, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>, media: bool) {
    let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
    let p = &v["payload"];
    if v["type"] == "compacted" {
        items.push(json!({"role": "system", "kind": "note", "text": "Conversation compacted", "ts": ts}));
        return;
    }
    match p["type"].as_str().unwrap_or("") {
        "user_message" => push_said(items, p["message"].as_str().unwrap_or(""), ts),
        "message" if p["role"] == "assistant" => {
            for b in p["content"].as_array().into_iter().flatten() {
                let t = b["text"].as_str().unwrap_or("").trim();
                if !t.is_empty() {
                    items.push(json!({"role": "assistant", "kind": "text", "text": clip(t, 40000), "ts": ts}));
                }
            }
        }
        "function_call" | "custom_tool_call" => {
            let name = p["name"].as_str().unwrap_or("");
            let input = match p["arguments"].as_str().or(p["input"].as_str()) {
                Some(raw) => serde_json::from_str::<Value>(raw).unwrap_or_else(|_| json!({"input": raw})),
                None => Value::Null,
            };
            let id = p["call_id"].as_str().unwrap_or("");
            calls.insert(id.into(), items.len());
            let mut t = tool_item(name, &input, ts);
            t["id"] = Value::String(id.into());
            items.push(t);
        }
        "reasoning" => {
            let t: Vec<&str> = p["summary"].as_array().into_iter().flatten().filter_map(|b| b["text"].as_str()).collect();
            if !t.is_empty() {
                items.push(json!({"role": "assistant", "kind": "thinking", "text": clip(&t.join("\n\n"), 20000), "ts": ts}));
            }
        }
        "function_call_output" | "custom_tool_call_output" => {
            attach(items, calls, p["call_id"].as_str().unwrap_or(""), &p["output"], codex_exit_failed(&p["output"]), media);
        }
        _ => {}
    }
}

// One rollout line. A v2 rollout's items come from its item_completed events; its response items
// repeat the agent's words and reasoning, and in code mode carry each command as JavaScript (a
// custom_tool_call named exec) that the CommandExecution item already shows as the command run.
fn codex_line(v: &Value, v2: bool, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>, media: bool) {
    if v2 {
        let p = &v["payload"];
        if v["type"] == "event_msg" {
            if p["type"] == "item_completed" {
                codex_v2_item(v, items, calls);
            }
            return;
        }
        match p["type"].as_str().unwrap_or("") {
            "message" | "reasoning" | "agent_message" => return,
            "custom_tool_call" | "function_call" if matches!(p["name"].as_str(), Some("exec" | "apply_patch")) => return,
            "custom_tool_call_output" if !calls.contains_key(p["call_id"].as_str().unwrap_or("")) => return,
            _ => {}
        }
    }
    codex_item(v, items, calls, media);
}

// A command as it was typed: the script given to the shell (bash -lc, pwsh -Command), else the argv.
fn command_text(c: &Value) -> String {
    if let Some(s) = c.as_str() {
        return s.to_string();
    }
    let argv: Vec<&str> = c.as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    match argv.as_slice() {
        [_, flag, script] if matches!(*flag, "-lc" | "-c" | "-Command") => script.to_string(),
        _ => argv.join(" "),
    }
}

fn codex_v2_item(v: &Value, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>) {
    let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
    let it = &v["payload"]["item"];
    let id = it["id"].as_str().unwrap_or("").to_string();
    let mut tool = |items: &mut Vec<Value>, mut t: Value| {
        t["id"] = Value::String(id.clone());
        calls.insert(id.clone(), items.len());
        items.push(t);
    };
    match it["type"].as_str().unwrap_or("") {
        "UserMessage" => {
            for c in it["content"].as_array().into_iter().flatten() {
                match c["type"].as_str().unwrap_or("") {
                    "text" => push_said(items, c["text"].as_str().unwrap_or(""), ts),
                    "local_image" => {
                        let p = c["path"].as_str().unwrap_or("");
                        let pics = if Path::new(p).exists() { json!([p]) } else { json!([]) };
                        items.push(json!({"role": "user", "kind": "image", "images": pics, "ts": ts}));
                    }
                    _ => {}
                }
            }
        }
        "AgentMessage" => {
            let t: Vec<&str> = it["content"].as_array().into_iter().flatten().filter_map(|c| c["text"].as_str()).collect();
            let t = t.join("\n\n");
            if !t.trim().is_empty() {
                items.push(json!({"role": "assistant", "kind": "text", "text": clip(t.trim(), 40000), "ts": ts}));
            }
        }
        "Reasoning" => {
            let t: Vec<&str> = it["summary_text"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            if !t.is_empty() {
                items.push(json!({"role": "assistant", "kind": "thinking", "text": clip(&t.join("\n\n"), 20000), "ts": ts}));
            }
        }
        "CommandExecution" => {
            let mut t = tool_item("Bash", &json!({"command": command_text(&it["command"])}), ts);
            t["result"] = Value::String(clip_result(it["aggregated_output"].as_str().unwrap_or("").trim(), 400));
            t["error"] = Value::Bool(it["exit_code"].as_i64().is_some_and(|c| c != 0) || it["status"] == "failed");
            tool(items, t);
        }
        // changes: {path: {type: add|update|delete, content | unified_diff}}, written out as the
        // patch apply_patch would have taken
        "FileChange" => {
            let mut patch = String::new();
            let mut adds = true;
            for (path, c) in it["changes"].as_object().into_iter().flatten() {
                let body = match c["type"].as_str().unwrap_or("") {
                    "add" => {
                        patch += &format!("*** Add File: {path}\n");
                        c["content"].as_str().unwrap_or("").lines().map(|l| format!("+{l}\n")).collect::<String>()
                    }
                    "delete" => {
                        adds = false;
                        patch += &format!("*** Delete File: {path}\n");
                        String::new()
                    }
                    _ => {
                        adds = false;
                        patch += &format!("*** Update File: {path}\n");
                        // the "---"/"+++" file header comes before the first hunk; after it, a line that starts so is content
                        let mut hunks = false;
                        c["unified_diff"].as_str().unwrap_or("").lines().filter(|l| {
                            hunks |= l.starts_with("@@");
                            hunks || !(l.starts_with("+++") || l.starts_with("---"))
                        }).map(|l| format!("{l}\n")).collect::<String>()
                    }
                };
                patch += &body;
            }
            let mut t = tool_item("apply_patch", &json!({"input": patch}), ts);
            t["tool"] = json!(if adds { "Write" } else { "Edit" });
            t["result"] = Value::String(clip_result(it["stdout"].as_str().unwrap_or("").trim(), 400));
            t["error"] = Value::Bool(it["status"] == "failed");
            tool(items, t);
        }
        "McpToolCall" => {
            let name = format!("mcp__{}__{}", it["server"].as_str().unwrap_or(""), it["tool"].as_str().unwrap_or(""));
            let mut t = tool_item(&name, &it["arguments"], ts);
            t["result"] = Value::String(clip_result(result_text(&it["result"]["content"]).trim(), 400));
            t["error"] = Value::Bool(it["status"] == "failed");
            tool(items, t);
        }
        "Extension" if it["kind"] == "web.search" => tool(items, tool_item("WebSearch", &json!({"query": it["query"]}), ts)),
        "ImageView" => {
            let p = it["path"].as_str().unwrap_or("");
            tool(items, tool_item("view_image", &json!({"path": p.strip_prefix("file://").unwrap_or(p)}), ts));
        }
        _ => {}
    }
}

// A workflow's row: the saved workflow's name, else what its script's `meta` says it does (its description, else its
// name), so the row reads "Review changed files…" instead of the script's first line.
fn workflow_label(input: &Value) -> String {
    if let Some(n) = input["name"].as_str().filter(|n| !n.trim().is_empty()) {
        return n.trim().to_string();
    }
    let script = input["script"].as_str().unwrap_or("");
    let meta = script.find("meta").map_or("", |i| &script[i..]);
    for key in ["description", "name"] {
        if let Some(v) = quoted_after(meta, key) {
            return v;
        }
    }
    "A workflow".to_string()
}

// The quoted string after `key:` in a JS object literal ('…', "…" or `…`), when there is one.
fn quoted_after(text: &str, key: &str) -> Option<String> {
    let mut from = 0;
    while let Some(i) = text[from..].find(key) {
        let at = from + i;
        from = at + key.len();
        let before = text[..at].chars().last();
        if before.map_or(false, |c| c.is_alphanumeric() || c == '_') {
            continue;
        }
        let rest = text[from..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else { continue };
        let rest = rest.trim_start();
        let q = rest.chars().next()?;
        if !matches!(q, '\'' | '"' | '`') {
            continue;
        }
        let body = &rest[1..];
        let end = body.find(q)?;
        let v = body[..end].trim();
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    None
}

fn tool_item(name: &str, input: &Value, ts: u64) -> Value {
    let s = |k: &str| input[k].as_str().unwrap_or("").to_string();
    let home = home().to_string_lossy().into_owned();
    let short = |p: String| if !home.is_empty() && p.starts_with(&home) { format!("~{}", &p[home.len()..]) } else { p };
    let summary = match name {
        "Bash" => s("command"),
        "Read" | "Edit" | "Write" | "MultiEdit" => short(s("file_path")),
        "NotebookEdit" => short(s("notebook_path")),
        "Grep" | "Glob" => s("pattern"),
        "WebFetch" => s("url"),
        "WebSearch" => s("query"),
        "Agent" | "Task" => s("description"),
        "Skill" => s("skill"),
        "AskUserQuestion" => input["questions"][0]["question"].as_str().unwrap_or("").to_string(),
        // shell_command (Codex's own shell tool) has its command as a string, which may run over several lines
        "exec_command" | "shell" | "shell_command" => input["cmd"].as_str().or(input["command"].as_str()).map(str::to_string)
            .or_else(|| input["command"].as_array().map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" ")))
            .unwrap_or_default(),
        "apply_patch" => patch_files(&s("input")).iter().map(|f| short(f.0.clone())).collect::<Vec<_>>().join(", "),
        "Workflow" => workflow_label(input),
        // an unknown tool: only a field that names what it does. The first string of an object is an id or a flag as
        // often as words, and a script or other multi-line value is code ("export const meta = {" says nothing)
        _ => ["command", "cmd", "path", "file_path", "url", "query", "pattern", "description", "title", "name", "prompt"].iter()
                .find_map(|k| input[*k].as_str().filter(|v| !v.trim().is_empty() && !v.contains('\n')))
                .unwrap_or("").to_string(),
    };
    let first = summary.lines().next().unwrap_or("").to_string();
    let mut item = json!({"role": "assistant", "kind": "tool", "tool": name, "summary": clip(&first, 200), "stat": "", "ts": ts});
    // what the agent says it is doing ("Installing host and checking…"): the terminal's headline
    if let Some(d) = input["description"].as_str().filter(|d| !d.trim().is_empty()) {
        if !matches!(name, "Agent" | "Task") {
            item["desc"] = Value::String(clip(d.trim(), 160));
        }
    }
    // what the conversation's pinned plan is built from
    match name {
        "TaskCreate" => item["task"] = json!({"subject": input["subject"], "active": input["activeForm"]}),
        "TaskUpdate" => item["task"] = json!({"id": input["taskId"], "status": input["status"], "subject": input["subject"]}),
        "TodoWrite" => item["task"] = json!({"todos": input["todos"]}),
        _ => {}
    }
    // What an edit changed: the text on both sides (clipped) for the diff view, and real line
    // counts (common leading and trailing lines left out) for the stat and the changes panel.
    let mut set = |files: Vec<(String, usize, usize, &str)>| {
        let (a, d) = files.iter().fold((0, 0), |t, f| (t.0 + f.1, t.1 + f.2));
        item["stat"] = Value::String(if d == 0 { format!("+{a}") } else { format!("+{a} −{d}") });
        item["files"] = Value::Array(files.into_iter().map(|(p, a, d, k)| json!({"path": p, "add": a, "del": d, "kind": k})).collect());
    };
    match name {
        "Edit" => {
            let (a, d) = changed(&s("old_string"), &s("new_string"));
            set(vec![(s("file_path"), a, d, "edit")]);
            diff_text(&mut item, Some(&s("old_string")), &s("new_string"));
        }
        "MultiEdit" => {
            let edits = input["edits"].as_array().cloned().unwrap_or_default();
            let (a, d) = edits.iter().fold((0, 0), |t, e| {
                let c = changed(e["old_string"].as_str().unwrap_or(""), e["new_string"].as_str().unwrap_or(""));
                (t.0 + c.0, t.1 + c.1)
            });
            set(vec![(s("file_path"), a, d, "edit")]);
            let join = |k: &str| edits.iter().filter_map(|e| e[k].as_str()).collect::<Vec<_>>().join("\n⋯\n");
            diff_text(&mut item, Some(&join("old_string")), &join("new_string"));
        }
        "Write" => {
            set(vec![(s("file_path"), s("content").lines().count(), 0, "write")]);
            diff_text(&mut item, None, &s("content"));
        }
        "NotebookEdit" => set(vec![(s("notebook_path"), s("new_source").lines().count(), 0, "edit")]),
        "AskUserQuestion" => {
            item["questions"] = input["questions"].clone();
        }
        "view_image" => {
            if Path::new(&s("path")).exists() {
                item["images"] = json!([s("path")]);
            }
        }
        "apply_patch" => {
            let patch = s("input");
            set(patch_files(&patch).into_iter().map(|(p, a, d, k)| (p, a, d, k)).collect());
            let (text, cut) = clip_lines(&patch, 6000);
            item["patch"] = Value::String(text);
            if cut > 0 {
                item["cut"] = json!(cut);
            }
        }
        _ => {}
    }
    item
}

// The text on both sides of an edit, cut to 3000 characters; `cut` (only when something is left out) is how
// many lines the longer side lost.
fn diff_text(item: &mut Value, old: Option<&str>, new: &str) {
    let (old, a) = old.map_or((None, 0), |o| { let (t, n) = clip_lines(o, 3000); (Some(t), n) });
    let (new, b) = clip_lines(new, 3000);
    if let Some(old) = old {
        item["old"] = Value::String(old);
    }
    item["new"] = Value::String(new);
    if a.max(b) > 0 {
        item["cut"] = json!(a.max(b));
    }
}

// Lines added and removed going from `old` to `new`, not counting what both share at the
// start and the end.
fn changed(old: &str, new: &str) -> (usize, usize) {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    let mut p = 0;
    while p < a.len() && p < b.len() && a[p] == b[p] {
        p += 1;
    }
    let mut q = 0;
    while q < a.len() - p && q < b.len() - p && a[a.len() - 1 - q] == b[b.len() - 1 - q] {
        q += 1;
    }
    (b.len() - p - q, a.len() - p - q)
}

// A Codex apply_patch: each file it touches, with its + and - line counts.
fn patch_files(patch: &str) -> Vec<(String, usize, usize, &'static str)> {
    let mut out: Vec<(String, usize, usize, &'static str)> = Vec::new();
    for l in patch.lines() {
        if let Some(p) = l.strip_prefix("*** Update File: ") {
            out.push((p.trim().to_string(), 0, 0, "edit"));
        } else if let Some(p) = l.strip_prefix("*** Add File: ") {
            out.push((p.trim().to_string(), 0, 0, "write"));
        } else if let Some(p) = l.strip_prefix("*** Delete File: ") {
            out.push((p.trim().to_string(), 0, 0, "delete"));
        } else if let Some(f) = out.last_mut() {
            // an apply_patch has no "---"/"+++" file header, so these start a line of content too
            if l.starts_with('+') {
                f.1 += 1;
            } else if l.starts_with('-') {
                f.2 += 1;
            }
        }
    }
    out
}

// Every file a session changed, from its whole transcript: path, lines added and removed,
// how many edits, and whether it was new. `before`: a message the app has just rewound to, which
// the transcript only records with the next message; nothing from it on counts.
pub fn changes(path: &Path, tool: &str, before: Option<&str>) -> Value {
    // read on from where the last call stopped (the app asks on every open and after every turn);
    // a rewind since then, or a cut the app asks for, reads the whole file again
    // pi and opencode are read whole: the branch that counts in a pi file can change anywhere, and
    // an opencode session is not a file
    let cache = CHANGES.get_or_init(Default::default);
    let cacheable = before.is_none() && matches!(tool, "claude" | "codex");
    let size = fs::metadata(path).map_or(0, |m| m.len());
    let (mut at, mut files, mut made, mut open) = match cache.lock().unwrap().get(path) {
        Some((at, files, made, open)) if cacheable && *at <= size => (*at, files.clone(), made.clone(), open.clone()),
        _ => (0, Vec::new(), Vec::new(), Vec::new()),
    };
    if at > 0 && tool == "claude" && rewind_after(path, at) {
        at = 0;
        files.clear();
        made.clear();
        open.clear();
    }
    let from = at;
    let (items, end) = transcript_from(path, tool, at, false);
    for item in items {
        if before.is_some() && item["rec"].as_str() == before {
            break;
        }
        // An edit the agent was refused or that failed changed nothing. One counted before its result was
        // written (`open`) is taken back when the result, read later, says it failed; a Write's that it updated a
        // file that was there is an edit, not a file made.
        if item["kind"] == "result" {
            let id = item["id"].as_str().unwrap_or("");
            if let Some(i) = open.iter().position(|(o, _, _)| o == id) {
                let (_, edited, ts) = open.remove(i);
                if item["error"] == true {
                    for f in edited.as_array().into_iter().flatten() {
                        let p = f["path"].as_str().unwrap_or("");
                        if let Some(i) = files.iter().position(|x| x["path"] == p) {
                            let x = &mut files[i];
                            for k in ["add", "del"] {
                                x[k] = json!(x[k].as_u64().unwrap_or(0).saturating_sub(f[k].as_u64().unwrap_or(0)));
                            }
                            x["edits"] = json!(x["edits"].as_u64().unwrap_or(0).saturating_sub(1));
                            if x["edits"] == 0 {
                                files.remove(i);
                            }
                        }
                        if f["kind"] == "write" {
                            made.retain(|m| !(m["path"] == p && m["at"] == ts));
                        }
                    }
                } else if overwrote(item["result"].as_str().unwrap_or("")) {
                    for f in edited.as_array().into_iter().flatten().filter(|f| f["kind"] == "write") {
                        let p = f["path"].as_str().unwrap_or("");
                        made.retain(|m| !(m["path"] == p && m["at"] == ts));
                        if f["new"] == true {
                            if let Some(x) = files.iter_mut().find(|x| x["path"] == p) {
                                x["created"] = json!(false);
                            }
                        }
                    }
                }
            }
            continue;
        }
        if item["error"] == true {
            continue;
        }
        let ts = item["ts"].as_u64().unwrap_or(0);
        // as counted, each file this call was the first to touch marked new (whose `created` it set)
        let mut edited = item["files"].clone();
        for (j, f) in item["files"].as_array().into_iter().flatten().enumerate() {
            let p = f["path"].as_str().unwrap_or("");
            if f["kind"] == "write" {
                made_at(&mut made, p, ts, "tool");
            }
            let at = match files.iter().position(|x| x["path"] == p) {
                Some(i) => i,
                None => {
                    files.push(json!({"path": p, "add": 0, "del": 0, "edits": 0, "created": f["kind"] == "write", "deleted": false}));
                    edited[j]["new"] = json!(true);
                    files.len() - 1
                }
            };
            let x = &mut files[at];
            x["add"] = json!(x["add"].as_u64().unwrap_or(0) + f["add"].as_u64().unwrap_or(0));
            x["del"] = json!(x["del"].as_u64().unwrap_or(0) + f["del"].as_u64().unwrap_or(0));
            x["edits"] = json!(x["edits"].as_u64().unwrap_or(0) + 1);
            x["deleted"] = json!(f["kind"] == "delete");
        }
        if item["files"].is_array() && item.get("result").is_none() {
            open.push((item["id"].as_str().unwrap_or("").to_string(), edited, ts));
        }
    }
    // images Codex generated: saved_path on an image_generation_end event, savedPath on a v2
    // image_gen.generation item
    if tool == "codex" {
        lines_between(path, from, end, |line| {
            let head = &line[..line.len().min(400)];
            if contains(head, br#""type":"image_generation_end""#) || contains(head, br#""kind":"image_gen.generation""#) {
                if let Ok(v) = serde_json::from_slice::<Value>(line) {
                    let p = &v["payload"];
                    if let Some(s) = p["saved_path"].as_str().or(p["item"]["savedPath"].as_str()) {
                        made_at(&mut made, s, parse_ts(v["timestamp"].as_str().unwrap_or("")), "image");
                    }
                }
            }
        });
    }
    if cacheable {
        cache.lock().unwrap().insert(path.to_path_buf(), (end, files.clone(), made.clone(), open));
    }
    let add: u64 = files.iter().map(|f| f["add"].as_u64().unwrap_or(0)).sum();
    let del: u64 = files.iter().map(|f| f["del"].as_u64().unwrap_or(0)).sum();
    let cwd = local_cwd(&session_cwd(path, tool));
    let repo = Path::new(&cwd).is_dir() && !repo_root(&cwd).is_empty();
    json!({"files": files, "add": add, "del": del, "repo": repo, "made": made_now(&made, &cwd)})
}
// per file: where the last read ended, the files changed, the files made, and the edits counted before their result
// was written (call id, files, time)
static CHANGES: OnceLock<Mutex<HashMap<PathBuf, (u64, Vec<Value>, Vec<Value>, Vec<(String, Value, u64)>)>>> = OnceLock::new();

fn made_at(made: &mut Vec<Value>, path: &str, at: u64, how: &str) {
    made.retain(|m| m["path"] != path);
    made.push(json!({"path": path, "at": at, "how": how}));
}

// What a session made that is still there, newest first: {path, kind, size, at, how}. A relative
// path is the session's folder's; a Windows path is looked for under the home folder.
fn made_now(made: &[Value], cwd: &str) -> Vec<Value> {
    let mut out: Vec<Value> = made.iter().filter_map(|m| {
        let p = local_file(m["path"].as_str().unwrap_or(""), cwd)?;
        let meta = fs::metadata(&p).ok().filter(|m| m.is_file())?;
        let path = p.to_string_lossy().into_owned();
        Some(json!({"path": path, "kind": sources::kind_of(&path), "size": meta.len(), "at": m["at"], "how": m["how"]}))
    }).collect();
    out.sort_by(|a, b| b["at"].as_u64().cmp(&a["at"].as_u64()));
    out
}

fn local_file(p: &str, cwd: &str) -> Option<PathBuf> {
    if p.is_empty() {
        return None;
    }
    if let Some(rest) = p.strip_prefix("C:\\").or_else(|| p.strip_prefix("c:\\")) {
        let rest = rest.replace('\\', "/");
        let mine = rest.strip_prefix("Users/").and_then(|r| r.split_once('/')).map(|(_, r)| r.to_string());
        return mine.into_iter().chain([rest]).map(|r| home().join(r)).find(|f| f.exists());
    }
    let f = if p.starts_with('/') { PathBuf::from(p) } else if !cwd.is_empty() { Path::new(cwd).join(p) } else { return None };
    f.exists().then_some(f)
}

// The folder a session ran in, from the first record that names it.
pub(crate) fn session_cwd(path: &Path, tool: &str) -> String {
    if tool == "opencode" {
        return sources::opencode_cwd(path);
    }
    let Ok(f) = File::open(path) else { return String::new() };
    let mut r = BufReader::new(f);
    let mut buf = Vec::new();
    for _ in 0..500 {
        buf.clear();
        if r.read_until(b'\n', &mut buf).map_or(true, |n| n == 0) {
            break;
        }
        let Ok(v) = serde_json::from_slice::<Value>(&buf) else { continue };
        let cwd = v["cwd"].as_str().or(v["payload"]["cwd"].as_str()).unwrap_or("");
        if !cwd.is_empty() {
            return cwd.to_string();
        }
    }
    String::new()
}

// ── What a session started besides its own turns: sub-agents, workflow runs and background
// commands. Claude Code writes each beside the transcript: <session>/subagents/agent-<id>.jsonl
// with a .meta.json (agentType, description, toolUseId, parentAgentId, spawnDepth, model);
// <session>/subagents/workflows/wf_*/ with a journal.jsonl (started / result per agent) and the
// run's agents; <session>/workflows/wf_*.json once a run has ended. The transcript itself holds
// the call that started each, the task id it was given, and a task-notification when it stops.

#[derive(Default, Clone)]
struct Calls {
    offset: u64,
    agents: HashMap<String, Value>,            // Agent/Task call id -> {description, background, result?}
    flows: HashMap<String, Value>,             // Workflow call id -> {name, script, task, run}
    shells: Vec<Value>,                        // background Bash: {call, command, desc, started, task}
    recent: Vec<Value>,                        // the last foreground Bash calls, until one is moved to the background
    tasks: HashMap<String, (String, String)>,  // task id -> (status, summary), from task-notifications
}

static SCANS: OnceLock<Mutex<HashMap<PathBuf, Calls>>> = OnceLock::new();
static SUMMARIES: OnceLock<Mutex<HashMap<PathBuf, (u64, Value)>>> = OnceLock::new();

fn between(s: &str, a: &str, b: &str) -> String {
    s.split_once(a).and_then(|(_, r)| r.split_once(b)).map(|(m, _)| m.trim().to_string()).unwrap_or_default()
}
fn after<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    s.find(key).map(|i| &s[i + key.len()..])
}
fn word(s: &str) -> String {
    s.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-').collect()
}
pub(crate) fn mtime_ms(p: &Path) -> u64 {
    fs::metadata(p).and_then(|m| m.modified()).ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map_or(0, |d| d.as_millis() as u64)
}

/// What changes whenever a session's transcript does: its file's length, or for an opencode session (a key into
/// opencode's database, no file of its own) the write times of the database and its log.
pub(crate) fn transcript_mark(path: &Path) -> (u64, u64) {
    if sources::is_opencode_key(path) {
        return sources::opencode_mark();
    }
    (fs::metadata(path).map_or(0, |m| m.len()), 0)
}
fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

// The transcript, read on from where the last call stopped: a long session's file is tens of MB
// and the app asks every two seconds while something runs. Only lines that can matter are parsed.
fn scan_calls(path: &Path) -> Calls {
    let cache = SCANS.get_or_init(Default::default);
    let mut c = cache.lock().unwrap().get(path).cloned().unwrap_or_default();
    if fs::metadata(path).map(|m| m.len()).unwrap_or(0) < c.offset {
        c = Calls::default(); // rewritten
    }
    let start = c.offset;
    let end = lines(path, start, |line| {
        let Ok(text) = std::str::from_utf8(line) else { return };
        let call = text.contains(r#""type":"tool_use""#)
            && (text.contains(r#""name":"Agent""#) || text.contains(r#""name":"Task""#)
                || text.contains(r#""name":"Workflow""#) || text.contains(r#""name":"Bash""#));
        let result = text.contains(r#""type":"tool_result""#)
            && (text.contains("Async agent launched") || text.contains("Workflow launched")
                || text.contains("running in background with ID") || text.contains("moved to the background (ID: ")
                || after(text, r#""tool_use_id":""#).is_some_and(|t| c.agents.contains_key(&word(t))));
        let note = text.contains("<task-notification>");
        if !(call || result || note) {
            return;
        }
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        if v["isSidechain"] == true {
            return;
        }
        let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
        let content = &v["message"]["content"];
        // a notification comes as the user's turn, or (queued while the session worked: a stop, a finish)
        // as an attachment carrying it as a queued command; both say how the task ended
        if note && matches!(v["type"].as_str(), Some("user" | "attachment")) {
            let text = first_text(content).or_else(|| v["attachment"]["prompt"].as_str().map(str::to_string)).unwrap_or_default();
            if text.trim_start().starts_with("<task-notification>") {
                for chunk in text.split("<task-notification>").skip(1) {
                    let id = between(chunk, "<task-id>", "</task-id>");
                    if !id.is_empty() {
                        c.tasks.insert(id, (between(chunk, "<status>", "</status>"), between(chunk, "<summary>", "</summary>")));
                    }
                }
            }
        }
        for b in content.as_array().into_iter().flatten() {
            match b["type"].as_str().unwrap_or("") {
                "tool_use" => {
                    let id = b["id"].as_str().unwrap_or("").to_string();
                    let inp = &b["input"];
                    match b["name"].as_str().unwrap_or("") {
                        "Agent" | "Task" => {
                            c.agents.insert(id, json!({"description": inp["description"], "type": inp["subagent_type"],
                                                       "background": inp["run_in_background"] == true, "ts": ts}));
                        }
                        "Workflow" => {
                            c.flows.insert(id, json!({"name": inp["name"], "script": inp["script"], "scriptPath": inp["scriptPath"], "ts": ts}));
                        }
                        "Bash" => {
                            let s = json!({"call": id, "command": clip(inp["command"].as_str().unwrap_or("").trim(), 300),
                                           "desc": inp["description"], "started": ts});
                            if inp["run_in_background"] == true {
                                c.shells.push(s);
                            } else {
                                // a command past its timeout is moved to the background (Claude Code 2.1.x): its
                                // result names the task, and only then is it one; a handful covers parallel calls
                                c.recent.push(s);
                                if c.recent.len() > 16 {
                                    c.recent.remove(0);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                "tool_result" => {
                    let call = b["tool_use_id"].as_str().unwrap_or("").to_string();
                    let text = result_text(&b["content"]);
                    if let Some(a) = c.agents.get_mut(&call) {
                        match after(&text, "agentId: ") {
                            Some(t) if text.starts_with("Async agent launched") => a["task"] = json!(word(t)),
                            _ => {
                                a["result"] = json!(true);
                                a["error"] = json!(b["is_error"] == true);
                                a["ended"] = json!(ts);
                            }
                        }
                    }
                    if text.starts_with("Workflow launched") {
                        if let Some(f) = c.flows.get_mut(&call) {
                            if let Some(t) = after(&text, "Task ID: ") {
                                f["task"] = json!(word(t));
                            }
                            if let Some(t) = after(&text, "Transcript dir: ") {
                                f["run"] = json!(t.lines().next().unwrap_or("").trim().rsplit('/').next().unwrap_or(""));
                            }
                        }
                    }
                    if let Some(t) = after(&text, "moved to the background (ID: ") {
                        if let Some(i) = c.recent.iter().position(|s| s["call"] == call.as_str()) {
                            let mut s = c.recent.remove(i);
                            s["task"] = json!(word(t));
                            s["output"] = json!(after(&text, "Output is being written to: ")
                                .map(|o| o.split_whitespace().next().unwrap_or("").trim_end_matches('.').to_string()));
                            c.shells.push(s);
                        }
                    }
                    if let Some(t) = after(&text, "running in background with ID: ") {
                        let task = word(t);
                        let output = after(&text, "Output is being written to: ")
                            .map(|o| o.split_whitespace().next().unwrap_or("").trim_end_matches('.').to_string());
                        if let Some(s) = c.shells.iter_mut().find(|s| s["call"] == call.as_str()) {
                            s["task"] = json!(task);
                            s["output"] = json!(output);
                        }
                    }
                }
                _ => {}
            }
        }
    });
    c.offset = end;
    cache.lock().unwrap().insert(path.to_path_buf(), c.clone());
    c
}

// One agent's own transcript, summed up (cached; the report clipped to 8000 characters).
fn agent_summary(path: &Path) -> Value {
    let len = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let cache = SUMMARIES.get_or_init(Default::default);
    if let Some((l, v)) = cache.lock().unwrap().get(path) {
        if *l == len {
            return v.clone();
        }
    }
    let v = summarize(path, 8000);
    cache.lock().unwrap().insert(path.to_path_buf(), (len, v.clone()));
    v
}

// What it was asked, how many steps, its newest step, when it started and last wrote, its model,
// its last words (the report, up to `cap` characters; reportCut when longer), whether it ended on them.
fn summarize(path: &Path, cap: usize) -> Value {
    let (mut asked, mut model, mut report) = (String::new(), String::new(), String::new());
    let (mut steps, mut started, mut ended) = (0u64, 0u64, 0u64);
    let mut last = Value::Null;
    let mut ends_on_words = false;
    // ended on words that closed its turn: a text block written before the rest of its message
    // (a tool call still being generated) has no stop_reason yet
    let mut finished = false;
    // a sub-agent may end by handing its report to its caller (SubagentHandback): that report is
    // its last word, and the "delivered" result after it does not mean it went on
    let mut handed_back = false;
    // a workflow agent ends on StructuredOutput: its input is the report, and words after it
    // (a recap) do not replace it unless the call was refused
    let mut structured = String::new();
    lines(path, 0, |line| {
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
        if ts > 0 {
            if started == 0 {
                started = ts;
            }
            ended = ts;
        }
        match v["type"].as_str().unwrap_or("") {
            "user" => {
                // a workflow agent's first message is the harness relaying the user's request; what it
                // was asked to do is the computed task after it (computed_task)
                if asked.is_empty() || asked.starts_with("[Workflow harness") {
                    if let Some(t) = first_text(&v["message"]["content"]).filter(|t| !t.trim().is_empty()) {
                        if let Some(task) = computed_task(&t) {
                            asked = clip(&task, 12000);
                        } else if asked.is_empty() || !t.trim().starts_with("[Workflow harness") {
                            asked = clip(t.trim(), 12000);
                        }
                    }
                }
                ends_on_words = false;
                finished = false;
                let refused = |b: &Value| b["tool_use_id"] == structured.as_str() && b["is_error"] == true;
                if !structured.is_empty() && v["message"]["content"].as_array().is_some_and(|c| c.iter().any(refused)) {
                    structured.clear();
                    handed_back = false;
                }
            }
            "assistant" => {
                if model.is_empty() {
                    model = v["message"]["model"].as_str().unwrap_or("").into();
                }
                for b in v["message"]["content"].as_array().into_iter().flatten() {
                    match b["type"].as_str().unwrap_or("") {
                        "tool_use" => {
                            steps += 1;
                            let name = b["name"].as_str().unwrap_or("");
                            let t = tool_item(name, &b["input"], ts);
                            last = json!({"tool": t["tool"], "summary": t["summary"], "desc": t["desc"]});
                            ends_on_words = false;
                            finished = false;
                            handed_back = matches!(name, "SubagentHandback" | "StructuredOutput");
                            if name == "SubagentHandback" {
                                if let Some(m) = b["input"]["message"].as_str() {
                                    report = m.trim().to_string();
                                    structured.clear();
                                }
                            } else if name == "StructuredOutput" {
                                report = serde_json::to_string_pretty(&b["input"]).unwrap_or_default();
                                structured = b["id"].as_str().unwrap_or("").to_string();
                            }
                        }
                        "text" => {
                            let t = b["text"].as_str().unwrap_or("").trim();
                            if !t.is_empty() {
                                if structured.is_empty() {
                                    report = t.to_string();
                                }
                                ends_on_words = true;
                                finished = v["message"]["stop_reason"].as_str().is_some_and(|r| r != "tool_use");
                                handed_back = false;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    });
    let cut = report.char_indices().nth(cap).is_some();
    json!({"asked": asked, "steps": steps, "last": last, "started": started, "ended": ended,
           "model": model, "report": clip(&report, cap), "reportKind": if structured.is_empty() { "text" } else { "json" },
           "reportCut": cut, "endsOnWords": ends_on_words || handed_back, "finished": finished || handed_back})
}

// The task in a workflow harness's "computed task" message: the text after its frame, with the
// harness's indent taken off and its notice about relayed messages left out.
fn computed_task(t: &str) -> Option<String> {
    let t = t.trim_start();
    if !t.starts_with("[Workflow harness — computed task]") {
        return None;
    }
    let body = t.split_once("The computed task text follows:").map_or(t, |(_, b)| b);
    let text = body.lines().map(|l| l.strip_prefix("  ").unwrap_or(l)).collect::<Vec<_>>().join("\n");
    let mut text = text.trim().to_string();
    if text.starts_with("Only the task below is yours.") {
        if let Some(i) = text.find("do not answer or act on them.") {
            text = text[i + "do not answer or act on them.".len()..].trim_start().to_string();
        }
    }
    (!text.is_empty()).then_some(text)
}

// running / done / failed / stopped: what the notification says, else what the call's result says,
// else whether the session's process reports its task as running (`listed`). While a process holds
// the session (`held`: since when, 0 if not known), an agent whose file it wrote and whose turn has
// not finished is still working: one step or one model call can take minutes. With no process
// nothing is working, however recently the file was written (a crash or a reboot leaves fresh files
// behind). `s`: its agent_summary.
fn state_of(note: Option<&(String, String)>, result: Option<bool>, listed: bool, held: Option<u64>, path: &Path, s: &Value) -> &'static str {
    let ends_on_words = s["endsOnWords"] == true;
    if let Some((status, _)) = note {
        return match status.as_str() {
            "completed" => "done",
            "failed" => "failed",
            _ => "stopped",
        };
    }
    if let Some(error) = result {
        return if error { "failed" } else { "done" };
    }
    let working = match held {
        Some(since) => mtime_ms(path) >= since && s["finished"] != true,
        None => false,
    };
    if listed || working {
        return "running";
    }
    if ends_on_words { "done" } else { "stopped" }
}

// What the host knows of a session it runs: the task ids (and the tool_use ids of the calls that
// started them) its process reported started and not yet ended, and whether that process is alive.
#[derive(Default)]
pub struct LiveTasks {
    pub running: HashSet<String>,
    pub alive: bool,
}

// Since when a process has held the session open: a Claude Code process's ~/.claude/sessions
// record (the host's own sessions have one too), else 0 when the host says its process is alive.
fn held_since(id: &str, live: &LiveTasks) -> Option<u64> {
    if let Some(pid) = terminal_holder(id) {
        let rec = fs::read(home().join(format!(".claude/sessions/{pid}.json"))).ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
        return Some(rec.and_then(|v| v["startedAt"].as_u64()).unwrap_or(0));
    }
    live.alive.then_some(0)
}

// One agents() read per transcript at a time: a request that comes while one runs waits for its
// answer instead of reading the same files beside it. Both come from the one host, so the live
// tasks they were asked with are the same.
struct Flight {
    answer: Mutex<Option<Value>>,
    landed: Condvar,
}
static FLIGHTS: OnceLock<Mutex<HashMap<PathBuf, Arc<Flight>>>> = OnceLock::new();

// Lands the flight when the read ends, also by a panic (then with Null, and a waiter reads itself).
struct Landing<'a>(&'a Path, Arc<Flight>, Value);
impl Drop for Landing<'_> {
    fn drop(&mut self) {
        if let Some(f) = FLIGHTS.get() {
            f.lock().unwrap().remove(self.0);
        }
        *self.1.answer.lock().unwrap() = Some(self.2.take());
        self.1.landed.notify_all();
    }
}

pub fn agents(path: &Path) -> Value {
    agents_live(path, &LiveTasks::default())
}

pub fn agents_live(path: &Path, live: &LiveTasks) -> Value {
    let flights = FLIGHTS.get_or_init(Default::default);
    let (flight, first) = {
        let mut f = flights.lock().unwrap();
        match f.get(path) {
            Some(x) => (x.clone(), false),
            None => {
                let x = Arc::new(Flight { answer: Mutex::new(None), landed: Condvar::new() });
                f.insert(path.to_path_buf(), x.clone());
                (x, true)
            }
        }
    };
    if !first {
        let mut a = flight.answer.lock().unwrap();
        while a.is_none() {
            a = flight.landed.wait(a).unwrap();
        }
        match a.as_ref() {
            Some(v) if !v.is_null() => return v.clone(),
            _ => {
                drop(a);
                return read_agents(path, live);
            }
        }
    }
    let mut landing = Landing(path, flight, Value::Null);
    let v = read_agents(path, live);
    landing.2 = v.clone();
    v
}

// How a call ended in its parent agent's transcript: Some(error) once its result is there. A
// background agent's result only says it was launched; its end is the task notification that names
// it (by call or task id), given as (status, summary).
fn end_in(file: &Path, call: &str, task: &str) -> (Option<bool>, Option<(String, String)>) {
    let (mut result, mut note) = (None, None);
    if call.is_empty() {
        return (result, note);
    }
    let key = format!(r#""tool_use_id":"{call}""#);
    let (by_call, by_task) = (format!("<tool-use-id>{call}</tool-use-id>"), format!("<task-id>{task}</task-id>"));
    lines(file, 0, |line| {
        let Ok(text) = std::str::from_utf8(line) else { return };
        let answer = result.is_none() && text.contains(&key) && text.contains(r#""type":"tool_result""#);
        let told = text.contains("<task-notification>") && (text.contains(&by_call) || text.contains(&by_task));
        if !(answer || told) {
            return;
        }
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        for b in v["message"]["content"].as_array().into_iter().flatten() {
            if answer && b["type"] == "tool_result" && b["tool_use_id"] == call && !result_text(&b["content"]).starts_with("Async agent launched") {
                result = Some(b["is_error"] == true);
            }
        }
        if told && matches!(v["type"].as_str(), Some("user" | "attachment")) {
            let body = first_text(&v["message"]["content"]).or_else(|| v["attachment"]["prompt"].as_str().map(str::to_string)).unwrap_or_default();
            for chunk in body.split("<task-notification>").skip(1) {
                let status = between(chunk, "<status>", "</status>");
                if !status.is_empty() && (chunk.contains(&by_call) || chunk.contains(&by_task)) {
                    note = Some((status, between(chunk, "<summary>", "</summary>")));
                }
            }
        }
    });
    (result, note)
}

// The phases a workflow script declares: meta = { ... phases: [{ title: 'Review' }, ...] }
fn phases_of(script: &str) -> Vec<String> {
    let Some(list) = after(script, "phases") else { return Vec::new() };
    let list = list.split(']').next().unwrap_or("");
    list.split("title").skip(1).filter_map(|t| {
        let t = t.trim_start_matches([' ', ':', '\t']);
        let q = t.chars().next()?;
        if !matches!(q, '\'' | '"' | '`') {
            return None;
        }
        t[1..].split(q).next().map(str::to_string)
    }).collect()
}

// Every task output file (…/tasks/<id>.output) some process holds open right now.
fn open_outputs() -> HashSet<String> {
    let mut open = HashSet::new();
    for pid in list_dir(Path::new("/proc")) {
        if !pid.file_name().is_some_and(|n| n.to_string_lossy().bytes().all(|b| b.is_ascii_digit())) {
            continue;
        }
        for fd in list_dir(&pid.join("fd")) {
            if let Ok(target) = fs::read_link(&fd) {
                let t = target.to_string_lossy();
                if t.ends_with(".output") && t.contains("/tasks/") {
                    open.insert(t.into_owned());
                }
            }
        }
    }
    open
}

fn read_agents(path: &Path, live: &LiveTasks) -> Value {
    let dir = path.with_extension("");
    let calls = scan_calls(path);
    let note = |task: &str| calls.tasks.get(task);
    let listed = |task: &str, call: &str| live.running.contains(task) || (!call.is_empty() && live.running.contains(call));
    let held = held_since(&path.file_stem().unwrap_or_default().to_string_lossy(), live);

    // sub-agents started by the session or by its agents
    let mut agents = Vec::new();
    let mut by_call: HashMap<String, String> = HashMap::new();
    for meta in list_dir(&dir.join("subagents")).into_iter().filter(|p| p.to_string_lossy().ends_with(".meta.json")) {
        let Ok(m) = fs::read(&meta).map_err(|_| ()).and_then(|b| serde_json::from_slice::<Value>(&b).map_err(|_| ())) else { continue };
        let name = meta.file_name().unwrap_or_default().to_string_lossy().replace(".meta.json", "");
        let id = name.trim_start_matches("agent-").to_string();
        let file = meta.with_file_name(format!("{name}.jsonl"));
        let s = agent_summary(&file);
        let call = m["toolUseId"].as_str().unwrap_or("").to_string();
        let c = calls.agents.get(&call);
        let task = c.and_then(|c| c["task"].as_str()).unwrap_or(&id).to_string();
        let (result, told) = match c.and_then(|c| c["result"].as_bool().map(|_| c["error"] == true)) {
            Some(r) => (Some(r), None),
            None => m["parentAgentId"].as_str()
                .map_or((None, None), |p| end_in(&meta.with_file_name(format!("agent-{p}.jsonl")), &call, &task)),
        };
        let state = if m["stoppedByUser"] == true { "stopped" } else {
            state_of(note(&task).or(told.as_ref()), result, listed(&task, &call), held, &file, &s)
        };
        by_call.insert(call.clone(), id.clone());
        agents.push(json!({"agent": id, "call": call, "parent": m["parentAgentId"], "depth": m["spawnDepth"],
            "type": m["agentType"], "description": m["description"], "name": m["name"],
            "model": if s["model"] != "" { s["model"].clone() } else { m["model"].clone() },
            "background": c.is_some_and(|c| c["background"] == true) || c.is_some_and(|c| c["task"].is_string()),
            "task": task, "state": state, "steps": s["steps"],
            "last": if state == "running" { s["last"].clone() } else { Value::Null },
            "started": s["started"], "ended": s["ended"]}));
    }
    agents.sort_by_key(|a| a["started"].as_u64().unwrap_or(0));

    // workflow runs: those this session launched, and any run directory beside it
    let mut workflows = Vec::new();
    let runs_dir = dir.join("subagents").join("workflows");
    let mut runs: Vec<PathBuf> = list_dir(&runs_dir).into_iter().filter(|p| p.is_dir()).collect();
    runs.sort_by_key(|p| mtime_ms(p));
    for run in runs {
        let run_name = run.file_name().unwrap_or_default().to_string_lossy().to_string();
        let (call, launched) = calls.flows.iter().find(|(_, f)| f["run"] == run_name.as_str())
            .map(|(k, f)| (k.clone(), f.clone())).unwrap_or_default();
        let record: Value = fs::read(dir.join("workflows").join(format!("{run_name}.json"))).ok()
            .and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or(Value::Null);
        let script = record["script"].as_str().or(launched["script"].as_str()).unwrap_or("").to_string();
        // a running inline script has no record yet: its meta's name, which the record will carry as workflowName
        let name = record["workflowName"].as_str().or(launched["name"].as_str()).map(str::to_string)
            .or_else(|| quoted_after(script.find("meta").map_or("", |i| &script[i..]), "name"))
            .unwrap_or_else(|| run_name.clone());
        // its agents, as the journal tells them: started with a label and phase, then a result
        let mut members: Vec<Value> = Vec::new();
        let journal = run.join("journal.jsonl");
        lines(&journal, 0, |line| {
            let Ok(j) = serde_json::from_slice::<Value>(line) else { return };
            let id = j["agentId"].as_str().unwrap_or("").to_string();
            match j["type"].as_str().unwrap_or("") {
                "started" => members.push(json!({"agent": id, "label": j["label"], "phase": j["phase"], "state": "running"})),
                "result" => {
                    if let Some(m) = members.iter_mut().find(|m| m["agent"] == id.as_str()) {
                        m["state"] = json!(if j["result"]["ok"] == false { "failed" } else { "done" });
                    }
                }
                _ => {}
            }
        });
        let latest = list_dir(&run).iter().map(|p| mtime_ms(p)).max().unwrap_or(0);
        let task = launched["task"].as_str().unwrap_or("").to_string();
        let state = match record["status"].as_str() {
            Some("completed") => "done",
            Some("failed") => "failed",
            Some(_) => "stopped",
            None => match note(&task) {
                Some(n) => if n.0 == "completed" { "done" } else if n.0 == "failed" { "failed" } else { "stopped" },
                None if listed(&task, &call) => "running",
                // held: running if that process wrote it; with no process, stopped however fresh its files
                None if held.is_some_and(|since| latest >= since) => "running",
                None => "stopped",
            },
        };
        if state != "running" {
            for m in members.iter_mut().filter(|m| m["state"] == "running") {
                m["state"] = json!("stopped");
            }
        }
        for m in members.iter_mut() {
            // older journals do not carry the phase; the agent's own record does
            if m["phase"].is_null() {
                let meta = run.join(format!("agent-{}.meta.json", m["agent"].as_str().unwrap_or("")));
                if let Some(ph) = fs::read(&meta).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()).and_then(|v| v["workflowPhase"].as_str().map(str::to_string)) {
                    m["phase"] = json!(ph);
                }
            }
            let f = run.join(format!("agent-{}.jsonl", m["agent"].as_str().unwrap_or("")));
            let s = agent_summary(&f);
            m["asked"] = s["asked"].clone();
            m["steps"] = s["steps"].clone();
            m["last"] = if m["state"] == "running" { s["last"].clone() } else { Value::Null };
            m["started"] = s["started"].clone();
            m["ended"] = s["ended"].clone();
            m["model"] = s["model"].clone();
        }
        // older runs give their agents no label: each is named by the first line of what it was
        // asked that the run's other agents were not also asked (they share a preamble)
        let asked: Vec<Vec<String>> = members.iter().map(|m| m["asked"].as_str().unwrap_or("").lines()
            .map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect()).collect();
        for (i, m) in members.iter_mut().enumerate() {
            if m["label"].as_str().is_some_and(|l| !l.is_empty()) {
                continue;
            }
            // its least shared line, the first of them if several tie
            let shared = |l: &String| asked.iter().filter(|o| o.contains(l)).count();
            let fewest = asked[i].iter().map(shared).min().unwrap_or(0);
            let own = asked[i].iter().find(|l| shared(l) == fewest).cloned().unwrap_or_default();
            m["label"] = json!(clip(&own, 80));
        }
        for m in members.iter_mut() {
            if let Some(o) = m.as_object_mut() {
                o.remove("asked");
            }
        }
        let logs: Vec<Value> = record["logs"].as_array().map(|l| l.iter().rev().take(40).rev().cloned().collect()).unwrap_or_default();
        workflows.push(json!({"run": run_name, "call": call, "task": task, "name": name,
            "summary": record["summary"].as_str().unwrap_or(""), "phases": phases_of(&script), "state": state,
            "started": record["startTime"].as_u64().or(launched["ts"].as_u64()).unwrap_or(0),
            "ended": if state == "running" { 0 } else { latest }, "durationMs": record["durationMs"],
            "error": record["error"], "logs": logs, "agents": members}));
    }

    // background commands: what their task-notification says; without one, running while some
    // process still holds the command's output file open (the shell writing it), else ended
    let open = if calls.shells.iter().any(|s| !note(s["task"].as_str().unwrap_or("")).is_some()) { open_outputs() } else { HashSet::new() };
    let shells: Vec<Value> = calls.shells.iter().map(|s| {
        let task = s["task"].as_str().unwrap_or("");
        let state = match note(task) {
            Some(n) => if n.0 == "completed" { "done" } else if n.0 == "failed" { "failed" } else { "stopped" },
            None if s["output"].as_str().is_some_and(|o| open.contains(o)) => "running",
            None => "ended",
        };
        let mut s = s.clone();
        s["state"] = json!(state);
        s
    }).collect();

    // the list shows running commands; the ones that ended are only counted
    let ended = shells.iter().filter(|x| x["state"] != "running").count();
    let shells: Vec<Value> = shells.into_iter().filter(|x| x["state"] == "running").collect();
    json!({"agents": agents, "workflows": workflows, "shells": shells, "shellsEnded": ended, "byCall": by_call})
}

// One agent's conversation as transcript items, found beside the session or inside a workflow run,
// with what it was asked and its report in full.
// A sub-agent's transcript: subagents/agent-ID.jsonl, or a workflow member's under subagents/workflows/RUN/.
fn agent_file(path: &Path, agent: &str) -> PathBuf {
    let dir = path.with_extension("").join("subagents");
    let name = format!("agent-{agent}.jsonl");
    let file = dir.join(&name);
    if file.exists() {
        return file;
    }
    list_dir(&dir.join("workflows")).into_iter().map(|r| r.join(&name)).find(|f| f.exists()).unwrap_or(file)
}

pub fn agent_items(path: &Path, agent: &str) -> Value {
    let file = agent_file(path, agent);
    let mut items = Vec::new();
    let mut calls = HashMap::new();
    lines(&file, 0, |line| {
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            claude_item_of(&v, &mut items, &mut calls, true, true);
        }
    });
    let mut s = agent_summary(&file);
    if s["reportCut"] == true {
        s = summarize(&file, 200_000);
    }
    json!({"items": items, "asked": s["asked"], "report": s["report"], "reportKind": s["reportKind"], "reportCut": s["reportCut"]})
}

static DIGESTS: OnceLock<Mutex<HashMap<PathBuf, (u64, Value)>>> = OnceLock::new();

// What results.rs needs of one sub-agent, read again only when its transcript grows: what it was
// asked, its report, the files its steps changed and the pictures its steps read. `step` counts
// its tool steps from 0, as the app's agent page numbers them.
pub fn agent_digest(path: &Path, agent: &str) -> Value {
    let file = agent_file(path, agent);
    let len = fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
    let cache = DIGESTS.get_or_init(Default::default);
    if let Some((l, v)) = cache.lock().unwrap().get(&file) {
        if *l == len {
            return v.clone();
        }
    }
    let mut items = Vec::new();
    let mut calls = HashMap::new();
    lines(&file, 0, |line| {
        if let Ok(v) = serde_json::from_slice::<Value>(line) {
            claude_item_of(&v, &mut items, &mut calls, true, true);
        }
    });
    let (mut files, mut pictures) = (Vec::new(), Vec::new());
    for (step, it) in items.iter().filter(|i| i["kind"] == "tool").enumerate() {
        for f in it["files"].as_array().into_iter().flatten() {
            files.push(json!({"path": f["path"], "add": f["add"], "del": f["del"], "step": step}));
        }
        for p in it["images"].as_array().into_iter().flatten() {
            pictures.push(json!({"path": p, "step": step, "what": it["summary"]}));
        }
    }
    // the whole report: a structured one is read field by field, and a cut one does not parse
    let mut s = agent_summary(&file);
    if s["reportCut"] == true {
        s = summarize(&file, 200_000);
    }
    let v = json!({"asked": s["asked"], "report": s["report"], "reportKind": s["reportKind"], "ended": s["ended"],
                   "files": files, "pictures": pictures});
    cache.lock().unwrap().insert(file, (len, v.clone()));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    // A Workflow step reads as what the workflow does, not as its script's first line (issue tacit#136).
    #[test]
    fn workflow_step_says_what_it_does() {
        let script = "export const meta = {\n  name: 'review-changes',\n  description: 'Review changed files across dimensions',\n}\nphase('Review')";
        let it = tool_item("Workflow", &json!({"script": script}), 0);
        assert_eq!(it["summary"], "Review changed files across dimensions");
        let it = tool_item("Workflow", &json!({"script": "export const meta = { name: \"only-a-name\" }"}), 0);
        assert_eq!(it["summary"], "only-a-name");
        let it = tool_item("Workflow", &json!({"name": "nightly-audit", "args": {"x": 1}}), 0);
        assert_eq!(it["summary"], "nightly-audit");
        let it = tool_item("Workflow", &json!({"script": "phase('x')"}), 0);
        assert_eq!(it["summary"], "A workflow");
        // a key named like the one wanted but longer is not it
        let it = tool_item("Workflow", &json!({"script": "const meta = { filename: 'x.js', name: 'real' }"}), 0);
        assert_eq!(it["summary"], "real");
    }

    // Codex's shell_command with a command of several lines shows its first, as Bash does: it fell to the unknown
    // tool's rule, which takes no value of several lines.
    #[test]
    fn a_codex_shell_command_of_several_lines_shows_its_first() {
        let it = tool_item("shell_command", &json!({"command": "python - <<'EOF'\nprint(1)\nEOF", "workdir": "/w"}), 0);
        assert_eq!(it["summary"], "python - <<'EOF'");
        let it = tool_item("shell", &json!({"command": ["bash", "-lc", "ls"]}), 0);
        assert_eq!(it["summary"], "bash -lc ls");
    }

    // An unknown tool never shows a line of code as its summary: a naming field first, else a one-line value.
    #[test]
    fn unknown_tool_skips_code() {
        let it = tool_item("SomeTool", &json!({"script": "line one\nline two", "title": "Make the thing"}), 0);
        assert_eq!(it["summary"], "Make the thing");
        let it = tool_item("SomeTool", &json!({"script": "line one\nline two", "target": "staging"}), 0);
        assert_eq!(it["summary"], "", "a field that is not listed is not guessed at");
        let it = tool_item("SomeTool", &json!({"script": "line one\nline two"}), 0);
        assert_eq!(it["summary"], "");
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("agent-host-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    // Pages read back from the end `limit` items at a time join up into the whole transcript.
    fn pages_join(path: &Path, tool: &str, limit: usize) {
        let (all, _) = transcript_from(path, tool, 0, true);
        let mut got: Vec<Value> = Vec::new();
        let mut until = None;
        loop {
            let (items, page, _) = transcript_tail_of(path, tool, limit, until);
            got.splice(0..0, items);
            if page == 0 {
                break;
            }
            assert!(until.map_or(true, |u| page < u), "{tool} limit {limit}: page {page} does not move back");
            until = Some(page);
        }
        assert_eq!(got.len(), all.len(), "{tool} limit {limit}: {} items in pages, {} in the transcript", got.len(), all.len());
        assert!(got == all, "{tool} limit {limit}: the pages differ from the transcript");
    }

    #[test]
    fn a_computed_task_is_what_the_agent_was_asked() {
        let t = "[Workflow harness — computed task] The task text below was computed. The computed task text follows:\n  Only the task below is yours. While you work, messages may be relayed; do not answer or act on them. Review phase 4\n  of the browser.";
        assert_eq!(computed_task(t).as_deref(), Some("Review phase 4\nof the browser."));
        assert_eq!(computed_task("[Workflow harness — user request] continue"), None);
        assert_eq!(computed_task("Just a task"), None);
    }

    // opencode hands an image as a data: URL (an attachment the agent looked at, or a file you
    // sent): it becomes a file the app can show. A file: URL is the path already.
    #[test]
    fn media_from_url_writes_a_data_url_and_keeps_a_path() {
        let cache = scratch("cache-media");
        std::env::set_var("AGENT_HOST_CACHE", &cache);
        let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
        let path = media_from_url(&format!("data:image/png;base64,{png}")).expect("decoded").as_str().map(str::to_string).unwrap();
        assert!(path.starts_with(&*cache.to_string_lossy()) && path.ends_with(".png"), "cached: {path}");
        assert_eq!(fs::read(&path).unwrap(), base64::engine::general_purpose::STANDARD.decode(png).unwrap());
        assert_eq!(media_from_url("file:///tmp/x.png").unwrap(), json!("/tmp/x.png"));
        assert!(media_from_url("data:text/plain,hello").is_none());
    }

    #[test]
    fn pages_keep_images_sent_with_text() {
        let cache = scratch("cache");
        std::env::set_var("AGENT_HOST_CACHE", &cache);
        // a pi message of an image and its text: two items from one record
        let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
        let ts = "2026-10-03T00:00:00.000Z";
        let msg = |id: &str, parent: Value, role: &str, content: Value| {
            json!({"type": "message", "id": id, "parentId": parent, "timestamp": ts, "message": {"role": role, "content": content}}).to_string()
        };
        let lines = [
            json!({"type": "session", "id": "pi-test", "cwd": "/tmp", "timestamp": ts}).to_string(),
            msg("a", Value::Null, "user", json!([{"type": "text", "text": "first"}])),
            msg("b", json!("a"), "assistant", json!([{"type": "text", "text": "one"}])),
            msg("c", json!("b"), "user", json!([{"type": "image", "data": png, "mimeType": "image/png"}, {"type": "text", "text": "second"}])),
            msg("d", json!("c"), "assistant", json!([{"type": "text", "text": "two"}])),
        ];
        let dir = scratch("pi");
        let file = dir.join("s.jsonl");
        fs::write(&file, lines.join("\n") + "\n").unwrap();
        for limit in 1..=3 {
            pages_join(&file, "pi", limit);
        }
        let _ = fs::remove_dir_all(&dir);
        // a Codex v2 rollout whose message is [local_image, local_image, text], when it is on this machine
        let codex = home().join(".codex/sessions/2026/09/27/rollout-2026-09-27T05-07-32-01a0e0d5-0842-7e31-a699-672332836415.jsonl");
        if codex.exists() {
            for limit in 1..=3 {
                pages_join(&codex, "codex", limit);
            }
        } else {
            eprintln!("skipped: {} is not here", codex.display());
        }
        let _ = fs::remove_dir_all(&cache);
    }

    // A timestamp of whole seconds with no zone, 19 characters, read past its end: the panic, inside refresh, poisoned
    // the history lock for every client.
    #[test]
    fn a_timestamp_of_whole_seconds_is_read() {
        assert_eq!(parse_ts("2026-11-12T10:00:00"), parse_ts("2026-11-12T10:00:00.000Z"));
        assert_eq!(parse_ts("2026-11-12T10:00:00.250Z") - parse_ts("2026-11-12T10:00:00Z"), 250);
        assert_eq!(parse_ts("2026-11-12"), 0);
    }

    // A first message whose first block is injected context (an IDE's open file, a system reminder) still gives the
    // row its prompt: only the first text block was looked at.
    #[test]
    fn the_prompt_is_found_past_injected_context() {
        let mut e = Entry::new(Path::new("/x/eeee-5.jsonl"), "claude");
        let line = json!({"parentUuid": null, "type": "user", "cwd": "/w", "timestamp": "2026-11-12T10:00:00.000Z",
                          "message": {"role": "user", "content": [
                              {"type": "text", "text": "<ide_opened_file>The user opened /w/login.rs</ide_opened_file>"},
                              {"type": "text", "text": "<system-reminder>\nmind the tests\n</system-reminder>"},
                              {"type": "text", "text": "fix the login bug"}]}});
        claude_meta(&mut e, line.to_string().as_bytes(), &mut Vec::new());
        assert_eq!(e.prompt, "fix the login bug");
    }

    // A folder removed, or made a repository, while the host runs shows on its row within a minute: the row's place
    // was worked out once, and repo_root kept every answer for good.
    #[test]
    fn a_rows_folder_is_looked_at_again() {
        let dir = scratch("placed");
        let (wt, plain) = (dir.join("wt"), dir.join("plain"));
        let git = |d: &Path| {
            fs::create_dir_all(d.join(".git")).unwrap();
            fs::write(d.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        };
        git(&wt);
        fs::create_dir_all(&plain).unwrap();
        let placed = |cwd: &Path| {
            let mut e = Entry::new(&dir.join("dddd-4.jsonl"), "claude");
            e.id = "dddd-4".into();
            e.cwd = cwd.to_string_lossy().into_owned();
            e.placed();
            e
        };
        let (mut gone, mut made) = (placed(&wt), placed(&plain));
        assert!(gone.place.as_ref().is_some_and(|p| p.local && !p.root.is_empty()));
        assert!(made.place.as_ref().is_some_and(|p| p.root.is_empty()));
        fs::remove_dir_all(&wt).unwrap();
        git(&plain);
        for e in [&mut gone, &mut made] {
            e.placed();
            e.place.as_mut().unwrap().at -= PLACE_AGE;
            e.placed();
        }
        let p = gone.place.as_ref().unwrap();
        assert_eq!((p.local, p.root.as_str(), p.resume.as_str()), (false, "", "claude --resume dddd-4"));
        assert_eq!(made.place.as_ref().unwrap().root, plain.to_string_lossy());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rows_by_protocol() {
        let dir = scratch("rows");
        let repo = dir.join("it's a repo");
        fs::create_dir_all(repo.join(".git")).unwrap();
        fs::write(repo.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::create_dir_all(repo.join("sub")).unwrap();
        // an empty .git above the scratch folder (Codex leaves them in /tmp) is not a repository
        fs::create_dir_all(dir.join(".git")).unwrap();
        let mut h = History::default();
        for (name, tool, cwd) in [("aaaa-1", "claude", repo.join("sub")), ("bbbb-2", "codex", dir.join("gone")), ("cccc-3", "pi", dir.clone())] {
            let mut e = Entry::new(&dir.join(format!("{name}.jsonl")), tool);
            e.id = name.into();
            e.cwd = cwd.to_string_lossy().into_owned();
            e.reply = "done".into();
            e.seen = vec!["/x/a.rs".into(), "/x/b.rs".into(), "/x/c.rs".into(), "/x/d.rs".into()];
            e.updated = 1;
            e.placed();
            h.entries.insert(e.path.clone(), e);
        }
        let keys = |r: &Value| { let mut k: Vec<String> = r.as_object().unwrap().keys().cloned().collect(); k.sort(); k };
        let old = ["bytes", "cwd", "entry", "id", "lastPrompt", "own", "parent", "path", "prompt", "started", "title", "tool", "updated"];
        let p4 = h.list();
        assert_eq!(p4.len(), 2, "protocol 4 has no pi rows");
        for r in &p4 {
            assert_eq!(keys(r), old, "protocol 4 row {r}");
        }
        let p5 = h.list_for(true);
        assert_eq!(p5.len(), 3);
        let row = |id: &str| p5.iter().find(|r| r["id"] == id).unwrap().clone();
        let a = row("aaaa-1");
        let sub = repo.join("sub").to_string_lossy().into_owned();
        assert_eq!(a["resume"], format!("cd '{}' && claude --resume aaaa-1", sub.replace('\'', r"'\''")));
        assert_eq!((a["repo"].clone(), a["root"].clone(), a["local"].clone()), (json!(true), json!(repo.to_string_lossy()), json!(true)));
        assert_eq!(a["files"], json!({"n": 4, "add": 0, "del": 0, "names": ["d.rs", "c.rs", "b.rs"]}));
        assert_eq!(a["lastReply"], "done");
        let b = row("bbbb-2");
        assert_eq!((b["resume"].clone(), b["repo"].clone(), b["root"].clone(), b["local"].clone()), (json!("codex resume bbbb-2"), json!(false), json!(""), json!(false)));
        let c = row("cccc-3");
        assert_eq!((c["resume"].clone(), c["repo"].clone(), c["local"].clone()), (json!(format!("cd '{}' && pi --session cccc-3", dir.display())), json!(false), json!(true)));
        let _ = fs::remove_dir_all(&dir);
    }

    // Two background agents in the middle of a step their files last wrote an hour ago: stopped
    // with nothing known, running when the host lists the task (by task id or by call id), and
    // both running while the host's process for the session is alive. While it is alive, words
    // written ahead of the rest of their message (no stop_reason) are running; words that closed
    // the turn are done.
    // A background agent stopped while the session worked: its notification comes as an attachment
    // (a queued command), and it reads as stopped even while the session's process is alive.
    #[test]
    fn stop_notice_as_attachment() {
        let dir = scratch("stopnote");
        let main = dir.join("stop-note-test.jsonl");
        let sub = dir.join("stop-note-test/subagents");
        fs::create_dir_all(&sub).unwrap();
        let ts = "2026-10-25T00:00:00.000Z";
        let notice = "<task-notification>\n<task-id>eee</task-id>\n<tool-use-id>toolu_e</tool-use-id>\n<status>killed</status>\n<summary>Agent \"eee\" was stopped</summary>\n</task-notification>";
        let lines = [
            json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": "toolu_e", "name": "Agent",
                "input": {"description": "eee", "subagent_type": "general-purpose", "run_in_background": true}}]}}).to_string(),
            json!({"type": "user", "timestamp": ts, "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_e",
                "content": "Async agent launched successfully.\nagentId: eee (internal ID)"}]}}).to_string(),
            json!({"type": "attachment", "timestamp": ts, "isSidechain": false,
                "attachment": {"type": "queued_command", "prompt": notice}}).to_string(),
        ];
        fs::write(&main, lines.join("\n") + "\n").unwrap();
        fs::write(sub.join("agent-eee.meta.json"), json!({"agentType": "general-purpose", "toolUseId": "toolu_e"}).to_string()).unwrap();
        let step = json!([{"type": "tool_use", "id": "s1", "name": "Bash", "input": {"command": "sleep 3000"}}]);
        fs::write(sub.join("agent-eee.jsonl"), json!({"type": "assistant", "timestamp": ts, "message": {"model": "m", "content": step}}).to_string() + "\n").unwrap();
        let v = agents_live(&main, &LiveTasks { running: HashSet::new(), alive: true });
        let a = v["agents"].as_array().unwrap();
        assert_eq!(a.iter().find(|x| x["agent"] == "eee").unwrap()["state"], json!("stopped"));
    }

    #[test]
    fn agent_states_from_live_tasks() {
        let dir = scratch("live");
        let main = dir.join("live-tasks-test.jsonl");
        let sub = dir.join("live-tasks-test/subagents");
        fs::create_dir_all(&sub).unwrap();
        let ts = "2026-10-03T00:00:00.000Z";
        let mut lines = Vec::new();
        let step = json!([{"type": "tool_use", "id": "s1", "name": "Bash", "input": {"command": "sleep 3000", "description": "Waiting"}}]);
        let words = json!([{"type": "text", "text": "Writing it now."}]);
        for (aid, call, tail, stop) in [("aaa", "toolu_a", &step, Value::Null), ("bbb", "toolu_b", &step, Value::Null),
                                        ("ccc", "toolu_c", &words, Value::Null), ("ddd", "toolu_d", &words, json!("end_turn"))] {
            lines.push(json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": call, "name": "Agent",
                "input": {"description": aid, "subagent_type": "general-purpose", "run_in_background": true}}]}}).to_string());
            lines.push(json!({"type": "user", "timestamp": ts, "message": {"content": [{"type": "tool_result", "tool_use_id": call,
                "content": format!("Async agent launched successfully.\nagentId: {aid} (internal ID)")}]}}).to_string());
            fs::write(sub.join(format!("agent-{aid}.meta.json")), json!({"agentType": "general-purpose", "toolUseId": call}).to_string()).unwrap();
            let own = [
                json!({"type": "user", "timestamp": ts, "message": {"content": "wait"}}).to_string(),
                json!({"type": "assistant", "timestamp": ts, "message": {"model": "m", "content": tail, "stop_reason": stop}}).to_string(),
            ];
            let f = sub.join(format!("agent-{aid}.jsonl"));
            fs::write(&f, own.join("\n") + "\n").unwrap();
            File::options().write(true).open(&f).unwrap().set_modified(SystemTime::now() - Duration::from_secs(3600)).unwrap();
        }
        fs::write(&main, lines.join("\n") + "\n").unwrap();
        let states = |live: LiveTasks| {
            let v = agents_live(&main, &live);
            let a = v["agents"].as_array().unwrap().clone();
            let of = |id: &str| a.iter().find(|x| x["agent"] == id).unwrap().clone();
            (of("aaa")["state"].clone(), of("bbb")["state"].clone(), of("aaa")["last"].clone(), (of("ccc")["state"].clone(), of("ddd")["state"].clone()))
        };
        let set = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<HashSet<String>>();
        let (a, b, _, words) = states(LiveTasks::default());
        assert_eq!((a, b, words), (json!("stopped"), json!("stopped"), (json!("done"), json!("done"))), "nothing known");
        let (a, b, _, _) = states(LiveTasks { running: set(&["aaa"]), alive: false });
        assert_eq!((a, b), (json!("running"), json!("stopped")), "listed by task id");
        let (a, b, _, _) = states(LiveTasks { running: set(&["toolu_b"]), alive: false });
        assert_eq!((a, b), (json!("stopped"), json!("running")), "listed by call id");
        let (a, b, last, words) = states(LiveTasks { running: HashSet::new(), alive: true });
        assert_eq!((a, b), (json!("running"), json!("running")), "process alive");
        assert_eq!(last, json!({"tool": "Bash", "summary": "sleep 3000", "desc": "Waiting"}));
        assert_eq!(words, (json!("running"), json!("done")), "process alive: words mid-message, words that closed the turn");
        let _ = fs::remove_dir_all(&dir);
    }

    // An agent's own background agent, in the middle of a step written just now in a live session: its parent's
    // "launched" result is not its end, so it is running; the parent's task notification ends it.
    // An agent's foreground agent ends on its result.
    #[test]
    fn nested_background_agent() {
        let dir = scratch("nested");
        let main = dir.join("nested-test.jsonl");
        let sub = dir.join("nested-test/subagents");
        fs::create_dir_all(&sub).unwrap();
        let ts = "2026-10-03T00:00:00.000Z";
        let call = |id: &str, bg: bool| json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": id,
            "name": "Agent", "input": {"description": id, "subagent_type": "general-purpose", "run_in_background": bg}}]}}).to_string();
        let res = |id: &str, text: &str| json!({"type": "user", "timestamp": ts, "isSidechain": true, "message": {"content": [{"type": "tool_result",
            "tool_use_id": id, "content": [{"type": "text", "text": text}]}]}}).to_string();
        fs::write(&main, call("toolu_p", false) + "\n").unwrap();
        let parent = sub.join("agent-ppp.jsonl");
        fs::write(&parent, [call("toolu_n", true), res("toolu_n", "Async agent launched successfully.\nagentId: nnn (internal ID)"),
                            call("toolu_f", false), res("toolu_f", "The answer.")].join("\n") + "\n").unwrap();
        fs::write(sub.join("agent-ppp.meta.json"), json!({"toolUseId": "toolu_p"}).to_string()).unwrap();
        let step = json!({"type": "assistant", "timestamp": ts, "message": {"model": "m", "content": [{"type": "tool_use", "id": "s1",
            "name": "Bash", "input": {"command": "make", "description": "Building"}}]}}).to_string();
        for (aid, c) in [("nnn", "toolu_n"), ("fff", "toolu_f")] {
            fs::write(sub.join(format!("agent-{aid}.meta.json")), json!({"toolUseId": c, "parentAgentId": "ppp", "spawnDepth": 2}).to_string()).unwrap();
            fs::write(sub.join(format!("agent-{aid}.jsonl")), step.clone() + "\n").unwrap();
        }
        let of = |id: &str| agents_live(&main, &LiveTasks { running: HashSet::new(), alive: true })["agents"].as_array().unwrap().iter().find(|x| x["agent"] == id).unwrap().clone();
        let n = of("nnn");
        assert_eq!((n["state"].clone(), n["last"]["desc"].clone()), (json!("running"), json!("Building")), "launched, not ended");
        assert_eq!(of("fff")["state"], json!("done"), "foreground: its result");
        let told = |status: &str| json!({"type": "user", "timestamp": ts, "isSidechain": true, "message": {"content": format!(
            "[SYSTEM NOTIFICATION]\n\n<task-notification>\n<task-id>nnn</task-id>\n<tool-use-id>toolu_n</tool-use-id>\n<status>{status}</status>\n<summary>s</summary>\n</task-notification>")}}).to_string();
        fs::write(&parent, fs::read_to_string(&parent).unwrap() + &told("killed") + "\n").unwrap();
        assert_eq!(of("nnn")["state"], json!("stopped"), "notified killed");
        fs::write(&parent, fs::read_to_string(&parent).unwrap() + &told("completed") + "\n").unwrap();
        assert_eq!(of("nnn")["state"], json!("done"), "notified again, completed");
        let _ = fs::remove_dir_all(&dir);
    }

    // A foreground command past its timeout, moved to the background: listed as a background command,
    // running while its output file is held open, done once its task notification says so. A command
    // that finished in the foreground is not listed.
    #[test]
    fn command_moved_to_the_background() {
        let dir = scratch("moved");
        let main = dir.join("moved-test.jsonl");
        let out = dir.join("tasks/bmoved1.output");
        fs::create_dir_all(out.parent().unwrap()).unwrap();
        let ts = "2026-10-03T00:00:00.000Z";
        let call = |id: &str, cmd: &str| json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": id,
            "name": "Bash", "input": {"command": cmd, "description": cmd}}]}}).to_string();
        let res = |id: &str, text: &str| json!({"type": "user", "timestamp": ts, "message": {"content": [{"type": "tool_result",
            "tool_use_id": id, "content": text}]}}).to_string();
        let moved = format!("Command did not complete within its 240s timeout and was moved to the background (ID: bmoved1). Output is being written to: {}. You will be notified when it completes.", out.display());
        fs::write(&main, [call("toolu_q", "ls"), res("toolu_q", "a b"), call("toolu_m", "gh run watch"), res("toolu_m", &moved)].join("\n") + "\n").unwrap();
        let held = File::create(&out).unwrap();
        let v = agents(&main);
        let shells = v["shells"].as_array().unwrap();
        assert_eq!(shells.len(), 1, "only the moved command");
        assert_eq!((shells[0]["task"].clone(), shells[0]["command"].clone(), shells[0]["state"].clone()), (json!("bmoved1"), json!("gh run watch"), json!("running")));
        drop(held);
        let told = json!({"type": "user", "timestamp": ts, "message": {"content":
            "<task-notification>\n<task-id>bmoved1</task-id>\n<status>completed</status>\n<summary>s</summary>\n</task-notification>"}}).to_string();
        fs::write(&main, fs::read_to_string(&main).unwrap() + &told + "\n").unwrap();
        let v = agents(&main);
        assert_eq!((v["shells"].as_array().unwrap().len(), v["shellsEnded"].clone()), (0, json!(1)), "done once notified");
        let _ = fs::remove_dir_all(&dir);
    }

    // A StructuredOutput is the report and words after it do not replace it; a refused one does
    // not count. A report longer than the cap is cut and says so.
    #[test]
    fn structured_output_is_the_report() {
        let dir = scratch("so");
        let ts = "2026-10-03T00:00:00.000Z";
        let so = |id: &str| json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": id, "name": "StructuredOutput",
            "input": {"status": "done", "files": ["a.rs"]}}]}}).to_string();
        let res = |id: &str, err: bool| json!({"type": "user", "timestamp": ts, "message": {"content": [{"type": "tool_result", "tool_use_id": id,
            "content": "x", "is_error": err}]}}).to_string();
        let text = |t: &str| json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "text", "text": t}]}}).to_string();
        let asked = json!({"type": "user", "timestamp": ts, "message": {"content": "go"}}).to_string();
        let run = |name: &str, lines: Vec<String>, cap: usize| {
            let f = dir.join(name);
            fs::write(&f, lines.join("\n") + "\n").unwrap();
            summarize(&f, cap)
        };
        let pretty = serde_json::to_string_pretty(&json!({"status": "done", "files": ["a.rs"]})).unwrap();
        let s = run("a.jsonl", vec![asked.clone(), text("working on it"), so("t1"), res("t1", false), text("Done: recap")], 8000);
        assert_eq!((s["report"].as_str().unwrap(), s["reportKind"].as_str().unwrap(), s["endsOnWords"].clone()), (pretty.as_str(), "json", json!(true)));
        let s = run("b.jsonl", vec![asked.clone(), so("t1"), res("t1", false)], 8000);
        assert_eq!((s["reportKind"].as_str().unwrap(), s["endsOnWords"].clone()), ("json", json!(true)), "ends on the call");
        let s = run("c.jsonl", vec![asked.clone(), so("t1"), res("t1", true), text("could not")], 8000);
        assert_eq!((s["report"].as_str().unwrap(), s["reportKind"].as_str().unwrap()), ("could not", "text"), "refused");
        let s = run("d.jsonl", vec![asked, text(&"x".repeat(50))], 20);
        assert_eq!((s["report"].as_str().unwrap().chars().count(), s["reportCut"].clone()), (21, json!(true)));
        let _ = fs::remove_dir_all(&dir);
    }

    // a signature's block kind sits at field 2 > 1 > 8, beside other fields of every wire type
    #[test]
    fn narration_is_read_from_the_signature() {
        fn ld(field: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![(field << 3) | 2, body.len() as u8];
            v.extend_from_slice(body);
            v
        }
        fn b64(b: &[u8]) -> String {
            const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let mut out = String::new();
            for c in b.chunks(3) {
                let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
                for i in 0..4 {
                    out.push(if i <= c.len() { A[(n >> (18 - 6 * i) & 63) as usize] as char } else { '=' });
                }
            }
            out
        }
        let sig = |kind: &[u8]| {
            let inner = [vec![0x08, 0x96, 0x01], ld(8, kind), vec![0x15, 1, 2, 3, 4]].concat();
            b64(&[ld(1, b"x"), ld(2, &ld(1, &inner))].concat())
        };
        assert!(is_narration(&sig(b"narration")));
        assert!(!is_narration(&sig(b"summary")));
        assert!(!is_narration(""));
        assert!(!is_narration("not base64 at all!"));
        assert!(!is_narration(&b64(&[0x12, 0x40, 0x01])));   // a length past the end
    }

    // A long result keeps its end, where a failing build says why, and a line says what is left out.
    #[test]
    fn long_result_keeps_head_and_tail() {
        let long = format!("{}{}", "w".repeat(3000), "error: build failed");
        let mut items = Vec::new();
        attach(&mut items, &HashMap::new(), "t1", &json!(long), true, false);
        let r = items[0]["result"].as_str().unwrap();
        assert!(r.starts_with("www") && r.ends_with("error: build failed"), "{r}");
        assert!(r.contains("… 2619 characters left out …"), "{r}");
        assert!(r.chars().count() < 460);
        assert_eq!(clip_result("short ünï", 400), "short ünï");
        let multi = "é".repeat(1000);
        assert!(clip_result(&multi, 400).ends_with('é'));
    }

    // A Codex command that exits non-zero is an error, in both output shapes a rollout holds.
    #[test]
    fn codex_exit_status_is_an_error() {
        let rows = |out: Value| {
            let mut items = Vec::new();
            let mut calls = HashMap::new();
            let call = json!({"type": "response_item", "payload": {"type": "function_call", "name": "shell", "call_id": "c1",
                "arguments": json!({"command": ["bash", "-lc", "cargo test"]}).to_string()}});
            let done = json!({"type": "response_item", "payload": {"type": "function_call_output", "call_id": "c1", "output": out}});
            codex_item(&call, &mut items, &mut calls, false);
            codex_item(&done, &mut items, &mut calls, false);
            assert_eq!(items.len(), 1, "{items:?}");
            items[0]["error"].clone()
        };
        assert_eq!(rows(json!("Exit code: 1\nWall time: 0.2 seconds\nOutput:\nboom")), json!(true));
        assert_eq!(rows(json!("Exit code: 0\nWall time: 0.2 seconds\nOutput:\nok")), json!(false));
        assert_eq!(rows(json!(r#"{"output":"boom","metadata":{"exit_code":2,"duration_seconds":0.1}}"#)), json!(true));
        assert_eq!(rows(json!(r#"{"output":"ok","metadata":{"exit_code":0,"duration_seconds":0.1}}"#)), json!(false));
        assert_eq!(rows(json!("plain text, no status")), json!(false));
        // exec_command's, a patch Codex could not apply, one that could not start, one the person stopped (codex-cli's
        // rollouts on this machine)
        assert_eq!(rows(json!("Chunk ID: 4f2a1c\nWall time: 1.0021 seconds\nProcess exited with code 1\nOriginal token count: 12\nOutput:\nboom")), json!(true));
        assert_eq!(rows(json!("Chunk ID: 4f2a1c\nWall time: 1.0021 seconds\nProcess exited with code 0\nOutput:\nok")), json!(false));
        assert_eq!(rows(json!("apply_patch verification failed: Failed to find expected lines in /w/a.rs:\nfn a() {}")), json!(true));
        assert_eq!(rows(json!("execution error: Io(Error { kind: InvalidInput, message: \"nul byte found in provided data\" })")), json!(true));
        assert_eq!(rows(json!("aborted by user after 10.8s")), json!(true));
        assert_eq!(rows(json!("Wall time: 12.2 seconds\naborted by user")), json!(true));
        // only Codex's own lines are read: the output's words are not a status
        assert_eq!(rows(json!("Exit code: 0\nWall time: 0.2 seconds\nOutput:\nExit code: 3\naborted by user")), json!(false));
        assert_eq!(rows(json!("Plan updated\naborted by user")), json!(false));
    }

    // A tool with no listed key has no detail: the first string of its input is an id as often as words.
    #[test]
    fn unknown_tool_has_no_guessed_summary() {
        let t = tool_item("mcp__omnicord__send_message", &json!({"channel_id": "1234567890", "content": "hi"}), 0);
        assert_eq!(t["summary"], json!(""));
        let t = tool_item("TaskStop", &json!({"task_id": "b7x2k9"}), 0);
        assert_eq!(t["summary"], json!(""));
        let t = tool_item("mcp__x__fetch", &json!({"id": "7", "url": "https://example.com"}), 0);
        assert_eq!(t["summary"], json!("https://example.com"));
    }

    // A large write or edit is cut to whole lines, with no "…" on the last, and says how many lines it lost.
    #[test]
    fn large_edit_says_what_was_cut() {
        let body: String = (0..400).map(|i| format!("line number {i} of the file\n")).collect();
        let t = tool_item("Write", &json!({"file_path": "/tmp/f.rs", "content": body}), 0);
        let new = t["new"].as_str().unwrap();
        assert!(!new.contains('…') && new.lines().all(|l| l.starts_with("line number ") && l.ends_with(" of the file")), "{new}");
        assert_eq!(t["cut"].as_u64().unwrap() as usize + new.lines().count(), 400);
        assert_eq!(t["stat"], json!("+400"));
        assert!(t.get("old").is_none());
        let t = tool_item("Edit", &json!({"file_path": "/tmp/f.rs", "old_string": "a\nb", "new_string": "a\nc"}), 0);
        assert_eq!((t["old"].clone(), t["new"].clone(), t.get("cut").is_none()), (json!("a\nb"), json!("a\nc"), true));
        let one = tool_item("Write", &json!({"file_path": "/tmp/m.js", "content": "x".repeat(5000)}), 0);
        assert_eq!((one["new"].as_str().unwrap().len(), one["cut"].clone()), (3000, json!(1)));
    }

    // Lines that start with "--" or "++" in an edit are lines of the edit, not the diff's file header.
    #[test]
    fn dashed_lines_are_counted() {
        let patch = "*** Begin Patch\n*** Update File: q.sql\n@@\n-- old comment\n+++i;\n context\n-x\n*** End Patch";
        let f = patch_files(patch);
        assert_eq!((f[0].1, f[0].2), (1, 2));
        let mut items = Vec::new();
        let mut calls = HashMap::new();
        let v2 = json!({"payload": {"item": {"type": "FileChange", "status": "completed",
            "changes": {"q.sql": {"type": "update", "unified_diff": "--- a/q.sql\n+++ b/q.sql\n@@ -1,2 +1,2 @@\n--- old comment\n+++i;\n context\n"}}}}});
        codex_v2_item(&v2, &mut items, &mut calls);
        let t = &items[0];
        assert_eq!(t["patch"].as_str().unwrap(), "*** Update File: q.sql\n@@ -1,2 +1,2 @@\n--- old comment\n+++i;\n context\n");
        assert_eq!(t["stat"], json!("+1 −1"));
    }

    // The changes record leaves out edits that were refused or failed, also when the refusal is read
    // on a later call than the edit.
    #[test]
    fn refused_edits_are_not_changes() {
        let dir = scratch("refused");
        let file = dir.join("refused-test.jsonl");
        let ts = "2026-10-25T00:00:00.000Z";
        let call = |id: &str, name: &str, input: Value| json!({"type": "assistant", "timestamp": ts,
            "message": {"content": [{"type": "tool_use", "id": id, "name": name, "input": input}]}}).to_string();
        let result = |id: &str, bad: bool| json!({"type": "user", "timestamp": ts,
            "message": {"content": [{"type": "tool_result", "tool_use_id": id, "content": "x", "is_error": bad}]}}).to_string();
        let edit = |f: &str| json!({"file_path": f, "old_string": "a", "new_string": "b\nc"});
        let mut lines = vec![
            call("e1", "Edit", edit("/x/kept.toml")), result("e1", false),
            call("e2", "Edit", edit("/x/denied.toml")), result("e2", true),
            call("e3", "Edit", edit("/x/kept.toml")), result("e3", true),
            call("w1", "Write", json!({"file_path": "/x/new.txt", "content": "one\ntwo"})),
        ];
        fs::write(&file, lines.join("\n") + "\n").unwrap();
        let got = changes(&file, "claude", None);
        let names: Vec<&str> = got["files"].as_array().unwrap().iter().map(|f| f["path"].as_str().unwrap()).collect();
        assert!(names.contains(&"/x/kept.toml") && names.contains(&"/x/new.txt") && !names.contains(&"/x/denied.toml"), "{names:?}");
        let kept = got["files"].as_array().unwrap().iter().find(|f| f["path"] == "/x/kept.toml").unwrap().clone();
        assert_eq!((kept["add"].clone(), kept["del"].clone(), kept["edits"].clone()), (json!(2), json!(1), json!(1)));
        // the Write was counted when it was pending; its denial arrives on the next read
        lines.clear();
        lines.push(result("w1", true));
        let mut f = fs::OpenOptions::new().append(true).open(&file).unwrap();
        std::io::Write::write_all(&mut f, (lines.join("\n") + "\n").as_bytes()).unwrap();
        let got = changes(&file, "claude", None);
        let names: Vec<&str> = got["files"].as_array().unwrap().iter().map(|f| f["path"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["/x/kept.toml"]);
        let _ = fs::remove_dir_all(&dir);
    }

    // A Write over a file that was there (Claude Code's result: "has been updated successfully") is an edit of it, in
    // its item (the activity line's kind) and in the changes record, not a file made; also when its result is read on a
    // later call than the Write. One that made its file is a new file as before.
    #[test]
    fn a_write_over_a_file_that_was_there_is_an_edit() {
        let dir = scratch("overwrite");
        let file = dir.join("overwrite-test.jsonl");
        let ts = "2026-10-25T00:00:00.000Z";
        let call = |id: &str, p: &str| json!({"type": "assistant", "timestamp": ts,
            "message": {"content": [{"type": "tool_use", "id": id, "name": "Write", "input": {"file_path": p, "content": "one\ntwo"}}]}}).to_string();
        let result = |id: &str, text: String| json!({"type": "user", "timestamp": ts,
            "message": {"content": [{"type": "tool_result", "tool_use_id": id, "content": text}]}}).to_string();
        let made = |p: &str| format!("File created successfully at: {p} (file state is current in your context — no need to Read it back)");
        let updated = |p: &str| format!("The file {p} has been updated successfully. (file state is current in your context — no need to Read it back)");
        let paths: Vec<String> = ["new.txt", "config.json", "README.md"].iter().map(|n| dir.join(n).to_string_lossy().into_owned()).collect();
        for p in &paths {
            fs::write(p, "one\ntwo").unwrap(); // a made file is listed while it is there
        }
        let (new, old, later) = (paths[0].as_str(), paths[1].as_str(), paths[2].as_str());
        let lines = [call("w1", new), result("w1", made(new)), call("w2", old), result("w2", updated(old)), call("w3", later)];
        fs::write(&file, lines.join("\n") + "\n").unwrap();
        let (items, _) = transcript_from(&file, "claude", 0, false);
        let kinds: Vec<&str> = items.iter().filter_map(|i| i["files"][0]["kind"].as_str()).collect();
        assert_eq!(kinds, ["write", "edit", "write"]);
        let created = |got: &Value| -> Vec<(String, bool)> {
            got["files"].as_array().unwrap().iter().map(|f| (f["path"].as_str().unwrap().to_string(), f["created"] == true)).collect()
        };
        let got = changes(&file, "claude", None);
        assert_eq!(created(&got), [(new.into(), true), (old.into(), false), (later.into(), true)]);
        // the third Write's result, on the next read: it updated a file that was there
        let mut f = fs::OpenOptions::new().append(true).open(&file).unwrap();
        std::io::Write::write_all(&mut f, (result("w3", updated(later)) + "\n").as_bytes()).unwrap();
        let got = changes(&file, "claude", None);
        assert_eq!(created(&got), [(new.into(), true), (old.into(), false), (later.into(), false)]);
        let made: Vec<&str> = got["made"].as_array().unwrap().iter().filter_map(|m| m["path"].as_str()).collect();
        assert_eq!(made, [new]);
        let _ = fs::remove_dir_all(&dir);
    }

    // A session's row counts an edit once its result is read, and not when it was refused or failed, as the Review
    // panel does: the index counted each edit at its call, so a refused one stayed on the row as a changed file.
    #[test]
    fn a_rows_changes_leave_out_refused_edits() {
        let dir = scratch("rowrefused");
        let append = |f: &Path, line: String| {
            let mut f = fs::OpenOptions::new().append(true).create(true).open(f).unwrap();
            std::io::Write::write_all(&mut f, (line + "\n").as_bytes()).unwrap();
        };
        // Claude Code's lines, in its own key order
        let file = dir.join("row-refused.jsonl");
        let line = |kind: &str, block: Value| format!(r#"{{"parentUuid":null,"isSidechain":false,"type":"{kind}","message":{{"role":"{kind}","content":[{block}]}},"timestamp":"2026-10-25T00:00:00.000Z"}}"#);
        let call = |id: &str, name: &str, input: Value| line("assistant", json!({"type": "tool_use", "id": id, "name": name, "input": input}));
        let result = |id: &str, bad: bool| line("user", json!({"tool_use_id": id, "type": "tool_result", "content": "x", "is_error": bad}));
        let edit = |f: &str| json!({"file_path": f, "old_string": "a", "new_string": "b\nc"});
        for l in [call("e1", "Edit", edit("/x/kept.toml")), result("e1", false), call("e2", "Edit", edit("/x/denied.toml")), result("e2", true),
                  call("w1", "Write", json!({"file_path": "/x/new.txt", "content": "one\ntwo"}))] {
            append(&file, l);
        }
        let mut e = Entry::new(&file, "claude");
        scan(&mut e);
        assert_eq!((e.seen.clone(), e.add, e.del), (vec!["/x/kept.toml".to_string()], 2, 1));
        // the Write's refusal comes with a later read, after the index was saved and read back
        let mut e = Entry::restored(&e.saved()).unwrap();
        append(&file, result("w1", true));
        scan(&mut e);
        assert_eq!((e.seen.clone(), e.add, e.del, e.open.len()), (vec!["/x/kept.toml".to_string()], 2, 1, 0));
        // a Codex rollout without item events: a patch Codex could not apply, then one it applied
        let file = dir.join("rollout-2026-10-25T00-00-00-019a0000-0000-7000-8000-00000000d001.jsonl");
        let item = |payload: Value| format!(r#"{{"timestamp":"2026-10-25T00:00:00.000Z","type":"response_item","payload":{payload}}}"#);
        let patch = |id: &str, body: &str| item(json!({"type": "custom_tool_call", "name": "apply_patch", "call_id": id,
                                                        "input": format!("*** Begin Patch\n{body}\n*** End Patch")}));
        let out = |id: &str, text: &str| item(json!({"type": "custom_tool_call_output", "call_id": id, "output": text}));
        for l in [patch("p1", "*** Update File: /w/a.rs\n@@\n-old\n+new"), out("p1", "apply_patch verification failed: Failed to find expected lines in /w/a.rs:\nold"),
                  patch("p2", "*** Add File: /w/b.rs\n+fn b() {}"),
                  out("p2", r#"{"output":"Success. Updated the following files:\nA /w/b.rs\n","metadata":{"exit_code":0,"duration_seconds":0.0}}"#)] {
            append(&file, l);
        }
        let mut e = Entry::new(&file, "codex");
        scan(&mut e);
        assert_eq!((e.seen.clone(), e.add, e.del), (vec!["/w/b.rs".to_string()], 1, 0));
        let _ = fs::remove_dir_all(&dir);
    }

    // With no process holding the session (a crash, a reboot, the host stopped), a workflow or an agent whose
    // files were written a moment ago is stopped, not running.
    #[test]
    fn nothing_runs_without_a_process() {
        let dir = scratch("noproc");
        let main = dir.join("no-process-test.jsonl");
        let sub = dir.join("no-process-test/subagents");
        let run = sub.join("workflows/run1");
        fs::create_dir_all(&run).unwrap();
        let ts = "2026-10-25T00:00:00.000Z";
        let step = json!([{"type": "tool_use", "id": "s1", "name": "Bash", "input": {"command": "sleep 3000"}}]);
        let line = json!({"type": "assistant", "timestamp": ts, "message": {"model": "m", "content": step}}).to_string() + "\n";
        fs::write(&main, "").unwrap();
        fs::write(sub.join("agent-fff.jsonl"), &line).unwrap();
        fs::write(sub.join("agent-fff.meta.json"), json!({"agentType": "general-purpose", "toolUseId": "toolu_f"}).to_string()).unwrap();
        fs::write(run.join("journal.jsonl"), json!({"type": "started", "agentId": "ggg", "label": "g"}).to_string() + "\n").unwrap();
        fs::write(run.join("agent-ggg.jsonl"), &line).unwrap();
        let v = agents_live(&main, &LiveTasks::default());
        assert_eq!(v["agents"][0]["state"], json!("stopped"));
        assert_eq!((v["workflows"][0]["state"].clone(), v["workflows"][0]["agents"][0]["state"].clone()), (json!("stopped"), json!("stopped")));
        let v = agents_live(&main, &LiveTasks { running: HashSet::new(), alive: true });
        assert_eq!(v["agents"][0]["state"], json!("running"), "the host's process is alive");
        let _ = fs::remove_dir_all(&dir);
    }

    // A running workflow launched with an inline script (no record yet, no saved name) is named by its meta's
    // `name`, as its tool row reads it: not by a longer key that ends in name or a comment, and a script ending in
    // `name:` is no panic.
    #[test]
    fn a_running_inline_workflow_is_named_by_its_meta() {
        let dir = scratch("flowname");
        let main = dir.join("flow-name-test.jsonl");
        let runs = dir.join("flow-name-test/subagents/workflows");
        let ts = "2026-11-13T00:00:00.000Z";
        let mut lines = Vec::new();
        for (call, run, script) in [("toolu_w1", "wf_1", "const meta = { filename: 'x.js', name: 'real' }\nphase('a')"),
                                    ("toolu_w2", "wf_2", "// its name: below\nconst meta = { name: `second` }"),
                                    ("toolu_w3", "wf_3", "phase('a') // name:")] {
            fs::create_dir_all(runs.join(run)).unwrap();
            lines.push(json!({"type": "assistant", "timestamp": ts, "message": {"content": [{"type": "tool_use", "id": call,
                "name": "Workflow", "input": {"script": script}}]}}).to_string());
            lines.push(json!({"type": "user", "timestamp": ts, "message": {"content": [{"type": "tool_result", "tool_use_id": call,
                "content": format!("Workflow launched in the background.\nTask ID: t{run}\nTranscript dir: {}", runs.join(run).display())}]}}).to_string());
        }
        fs::write(&main, lines.join("\n") + "\n").unwrap();
        let v = agents_live(&main, &LiveTasks::default());
        let name = |run: &str| v["workflows"].as_array().unwrap().iter().find(|w| w["run"] == run).map_or(Value::Null, |w| w["name"].clone());
        assert_eq!((name("wf_1"), name("wf_2"), name("wf_3")), (json!("real"), json!("second"), json!("wf_3")));
        let _ = fs::remove_dir_all(&dir);
    }
}
