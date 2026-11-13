//! The other agents' sessions, and what the host does to session files on disk.
//!
//! pi keeps one JSONL file per session (~/.pi/agent/sessions/--<cwd>--/<ts>_<id>.jsonl): a header
//! line {type: session, id, cwd, timestamp}, then entries that form a tree by id/parentId. The
//! conversation is the branch from the last entry back to the root.
//!
//! opencode keeps its sessions in SQLite (~/.local/share/opencode/opencode.db): 1.x in rows of `session`,
//! `message` and `part`; 2 in `session_v2` and `session_message` (one row per message, its content as JSON), and a
//! database 2 has opened can hold both. It is read only through `sqlite3 -readonly`; nothing here writes to it.

use super::{attach, clip, home, images, lines, list_dir, media_from_url, parse_ts, push_said, tool_item, Entry};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

// ── pi ──────────────────────────────────────────────────────────────────────────

pub(super) fn pi_dir() -> PathBuf {
    home().join(".pi/agent/sessions")
}

fn first_line(path: &Path) -> Value {
    let mut buf = Vec::new();
    File::open(path).ok().and_then(|f| BufReader::new(f).read_until(b'\n', &mut buf).ok());
    serde_json::from_slice(&buf).unwrap_or(Value::Null)
}

// The index's fields for a pi session, read whole: the files are small, and the branch that counts
// can change anywhere in them.
pub(super) fn pi_scan(e: &mut Entry) {
    let head = first_line(&e.path);
    e.id = head["id"].as_str().unwrap_or("").into();
    e.cwd = head["cwd"].as_str().unwrap_or("").into();
    e.started = parse_ts(head["timestamp"].as_str().unwrap_or(""));
    e.title.clear();
    lines(&e.path, 0, |line| {
        if super::contains(&line[..line.len().min(200)], br#""type":"session_info""#) {
            if let Some(n) = serde_json::from_slice::<Value>(line).ok().filter(|v| v["type"] == "session_info").and_then(|v| v["name"].as_str().map(str::to_string)) {
                e.title = n; // the newest name wins
            }
        }
    });
    let (items, end) = pi_read(&e.path, 0, false);
    e.prompt.clear();
    e.last_prompt.clear();
    super::forget(e);
    super::absorb(e, &items, true);
    e.offset = end;
}

// Items from byte `from` on, each tagged with the byte its entry starts at ("_at"). Read from the
// start, only the entries on the active branch are kept.
pub(super) fn pi_read(path: &Path, from: u64, media: bool) -> (Vec<Value>, u64) {
    let cwd = first_line(path)["cwd"].as_str().unwrap_or("").to_string();
    let mut items: Vec<Value> = Vec::new();
    let mut calls: HashMap<String, usize> = HashMap::new();
    let mut parent: HashMap<String, String> = HashMap::new();
    let mut last: Option<String> = None;
    let mut at = from;
    let end = lines(path, from, |line| {
        let start = at;
        at += line.len() as u64;
        let Ok(v) = serde_json::from_slice::<Value>(line) else { return };
        if v["type"] == "session" {
            return;
        }
        if let Some(id) = v["id"].as_str() {
            if let Some(p) = v["parentId"].as_str() {
                parent.insert(id.to_string(), p.to_string());
            }
            last = Some(id.to_string());
        }
        let first = items.len();
        pi_item(&v, &cwd, &mut items, &mut calls, media);
        for it in items[first..].iter_mut() {
            it["rec"] = v["id"].clone();
            it["_at"] = json!(start);
        }
    });
    if from == 0 {
        let mut keep = HashSet::new();
        let mut node = last;
        while let Some(id) = node {
            if !keep.insert(id.clone()) {
                break;
            }
            node = parent.get(&id).cloned();
        }
        items.retain(|it| it["rec"].as_str().map_or(true, |r| keep.contains(r)));
    }
    (items, end)
}

// pi's image block {type: image, data, mimeType} in the shape images() reads.
fn claude_blocks(c: &Value) -> Value {
    match c.as_array() {
        Some(a) => Value::Array(a.iter().map(|b| if b["type"] == "image" {
            json!({"type": "image", "source": {"type": "base64", "data": b["data"], "media_type": b["mimeType"]}})
        } else {
            b.clone()
        }).collect()),
        None => c.clone(),
    }
}

// pi's tools under Claude Code's names and input keys; grep, find and ls keep their own.
fn pi_tool(name: &str, a: &Value, cwd: &str) -> (String, Value) {
    let path = |k: &str| {
        let p = a[k].as_str().unwrap_or("");
        if p.is_empty() || p.starts_with('/') || p.starts_with('~') || cwd.is_empty() { p.to_string() } else { format!("{cwd}/{p}") }
    };
    match name {
        "bash" => ("Bash".into(), json!({"command": a["command"]})),
        "read" => ("Read".into(), json!({"file_path": path("path"), "offset": a["offset"], "limit": a["limit"]})),
        "edit" => {
            let edits: Vec<Value> = a["edits"].as_array().into_iter().flatten()
                .map(|x| json!({"old_string": x["oldText"], "new_string": x["newText"]})).collect();
            ("MultiEdit".into(), json!({"file_path": path("path"), "edits": edits}))
        }
        "write" => ("Write".into(), json!({"file_path": path("path"), "content": a["content"]})),
        _ => (name.to_string(), a.clone()),
    }
}

pub(super) fn pi_item(v: &Value, cwd: &str, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>, media: bool) {
    let ts = parse_ts(v["timestamp"].as_str().unwrap_or(""));
    match v["type"].as_str().unwrap_or("") {
        "compaction" => items.push(json!({"role": "system", "kind": "note", "text": "Conversation compacted", "ts": ts})),
        "message" => {
            let m = &v["message"];
            let c = &m["content"];
            match m["role"].as_str().unwrap_or("") {
                "user" => {
                    if let Some(s) = c.as_str() {
                        push_said(items, s, ts);
                    }
                    for b in c.as_array().into_iter().flatten() {
                        match b["type"].as_str().unwrap_or("") {
                            "text" => push_said(items, b["text"].as_str().unwrap_or(""), ts),
                            "image" => {
                                let pics = if media { images(&claude_blocks(&json!([b]))) } else { Vec::new() };
                                items.push(json!({"role": "user", "kind": "image", "images": pics, "ts": ts}));
                            }
                            _ => {}
                        }
                    }
                }
                "assistant" => {
                    for b in c.as_array().into_iter().flatten() {
                        match b["type"].as_str().unwrap_or("") {
                            "text" => {
                                let t = b["text"].as_str().unwrap_or("").trim();
                                if !t.is_empty() {
                                    items.push(json!({"role": "assistant", "kind": "text", "text": clip(t, 40000), "ts": ts, "model": m["model"]}));
                                }
                            }
                            "thinking" => {
                                let t = b["thinking"].as_str().unwrap_or("").trim();
                                if !t.is_empty() {
                                    items.push(json!({"role": "assistant", "kind": "thinking", "text": clip(t, 20000), "ts": ts}));
                                }
                            }
                            "toolCall" => {
                                let (name, input) = pi_tool(b["name"].as_str().unwrap_or(""), &b["arguments"], cwd);
                                let id = b["id"].as_str().unwrap_or("");
                                calls.insert(id.into(), items.len());
                                let mut t = tool_item(&name, &input, ts);
                                t["id"] = Value::String(id.into());
                                items.push(t);
                            }
                            _ => {}
                        }
                    }
                    // a turn the provider refused (an expired key, no credits) ends on this
                    if m["stopReason"] == "error" {
                        if let Some(err) = m["errorMessage"].as_str() {
                            items.push(json!({"role": "system", "kind": "note", "text": clip(err, 300), "ts": ts}));
                        }
                    }
                }
                "toolResult" => attach(items, calls, m["toolCallId"].as_str().unwrap_or(""), &claude_blocks(c), m["isError"] == true, media),
                // a command typed with ! in pi's own terminal
                "bashExecution" => {
                    let mut t = tool_item("Bash", &json!({"command": m["command"]}), ts);
                    t["result"] = Value::String(clip(m["output"].as_str().unwrap_or("").trim(), 400));
                    t["error"] = Value::Bool(m["exitCode"].as_i64().is_some_and(|x| x != 0));
                    items.push(t);
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ── opencode ────────────────────────────────────────────────────────────────────

pub(super) fn opencode_db() -> PathBuf {
    home().join(".local/share/opencode/opencode.db")
}

// An opencode session's place in the index: "<db>#<id>".
pub(super) fn opencode_key(id: &str) -> PathBuf {
    PathBuf::from(format!("{}#{id}", opencode_db().display()))
}

pub(super) fn is_opencode_key(p: &Path) -> bool {
    p.to_string_lossy().starts_with(&format!("{}#", opencode_db().display()))
}

pub(super) fn opencode_id(key: &Path) -> Option<String> {
    key.to_string_lossy().rsplit_once('#').map(|(_, id)| id.to_string())
}

// None when sqlite3 failed (not installed, or the database stayed busy): the index then keeps what
// it read before instead of dropping opencode's sessions.
fn try_query(sql: &str) -> Option<Vec<Value>> {
    let out = Command::new("sqlite3").args(["-readonly", "-json", "-cmd", ".timeout 2000"]).arg(opencode_db()).arg(sql).output().ok()?;
    if !out.status.success() {
        return None;
    }
    if out.stdout.iter().all(u8::is_ascii_whitespace) {
        return Some(Vec::new()); // no rows
    }
    serde_json::from_slice::<Value>(&out.stdout).ok()?.as_array().cloned()
}

fn query(sql: &str) -> Vec<Value> {
    try_query(sql).unwrap_or_default()
}

fn safe_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

// Which of 1.x's and 2's tables the database holds.
fn tables() -> (bool, bool) {
    let names: Vec<String> = query("select name from sqlite_master where type = 'table' and name in ('session', 'session_v2')")
        .iter().filter_map(|r| r["name"].as_str().map(str::to_string)).collect();
    (names.iter().any(|n| n == "session"), names.iter().any(|n| n == "session_v2"))
}

// 2's title for a session: a rename made here (opencode.rs) when it is this session's (a fork copies the metadata),
// else opencode's own.
const TITLE_V2: &str = "case when json_extract(metadata,'$.renamed.id') = id then json_extract(metadata,'$.renamed.title') else title end";

// Top-level sessions (sub-agent sessions have a parent_id), not archived: id, directory, title, times. A session
// in both 1.x's and 2's tables is 2's.
pub(super) fn opencode_sessions() -> Option<Vec<Value>> {
    if !opencode_db().exists() {
        return Some(Vec::new());
    }
    let v1 = "select id, directory, title, time_created, time_updated from session where parent_id is null and time_archived is null";
    let v2 = format!("select id, directory, {TITLE_V2} title, time_created, time_updated from session_v2 where parent_id is null and time_archived is null");
    match tables() {
        (true, true) => try_query(&format!("{v2} union all {v1} and id not in (select id from session_v2)")),
        (false, true) => try_query(&v2),
        _ => try_query(v1),
    }
}

// The sessions among `ids` that are 2's.
fn v2_ids(ids: &[String]) -> HashSet<String> {
    let list: Vec<String> = ids.iter().filter(|i| safe_id(i)).map(|i| format!("'{i}'")).collect();
    if list.is_empty() {
        return HashSet::new();
    }
    // with no session_v2 table the query fails, which is none of them: no tables() first, a process less per poll
    query(&format!("select id from session_v2 where id in ({})", list.join(","))).iter().filter_map(|r| r["id"].as_str().map(str::to_string)).collect()
}

// 2's messages of these sessions, in order: session, message id, kind (user, assistant, compaction, …), time, and
// the message as JSON. `from`: one session's, from its `from`th message on.
fn messages_v2(ids: &[String], from: u64) -> Option<Vec<Value>> {
    let list: Vec<String> = ids.iter().filter(|i| safe_id(i)).map(|i| format!("'{i}'")).collect();
    if list.is_empty() {
        return Some(Vec::new());
    }
    try_query(&format!("select session_id s, id mid, type t, time_created c, data d from session_message \
        where session_id in ({}) and type in ('user', 'assistant', 'compaction') order by session_id, seq limit -1 offset {from}", list.join(",")))
}

// A message of 2's as items. `only_scan`: what the index needs, your words, the agent's words and the edits.
fn oc2_items(r: &Value, items: &mut Vec<Value>, only_scan: bool) {
    let ts = r["c"].as_u64().unwrap_or(0);
    let d: Value = r["d"].as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or(Value::Null);
    match r["t"].as_str().unwrap_or("") {
        "user" => {
            push_said(items, d["text"].as_str().unwrap_or(""), ts);
            for f in d["files"].as_array().into_iter().flatten().filter(|_| !only_scan) {
                if let Some(p) = f["uri"].as_str().filter(|u| crate::image_type_of(u.trim_start_matches("file://")).is_some()).and_then(media_from_url) {
                    items.push(json!({"role": "user", "kind": "image", "images": [p], "ts": ts}));
                }
            }
        }
        "compaction" if !only_scan => items.push(json!({"role": "system", "kind": "note", "text": "Conversation compacted", "ts": ts})),
        "assistant" => {
            for c in d["content"].as_array().into_iter().flatten() {
                let t = c["text"].as_str().unwrap_or("").trim();
                match c["type"].as_str().unwrap_or("") {
                    "text" if !t.is_empty() => items.push(json!({"role": "assistant", "kind": "text", "text": t, "ts": ts})),
                    "reasoning" if !t.is_empty() && !only_scan => items.push(json!({"role": "assistant", "kind": "thinking", "text": clip(t, 20000), "ts": ts})),
                    "tool" => {
                        let name = c["name"].as_str().unwrap_or("");
                        if only_scan && !matches!(name, "edit" | "write") {
                            continue;
                        }
                        let st = &c["state"];
                        let (name, input) = crate::opencode_v2::tool_of(name, &st["input"]);
                        let mut it = tool_item(&name, &input, ts);
                        it["id"] = c["id"].clone();
                        match st["status"].as_str().unwrap_or("") {
                            "completed" => {
                                let out: Vec<&str> = st["content"].as_array().into_iter().flatten().filter_map(|p| p["text"].as_str()).collect();
                                it["result"] = Value::String(clip(out.join("\n").trim(), 400));
                                it["error"] = Value::Bool(false);
                            }
                            "error" => {
                                it["result"] = Value::String(clip(st["error"]["message"].as_str().unwrap_or("").trim(), 400));
                                it["error"] = Value::Bool(true);
                            }
                            _ => {}
                        }
                        items.push(it);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

// A 2 message is finished once it was completed or failed; your words always are.
fn done_v2(r: &Value) -> bool {
    if r["t"] != "assistant" {
        return true;
    }
    let d: Value = r["d"].as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or(Value::Null);
    !d["time"]["completed"].is_null() || !d["error"].is_null()
}

// The database and its write-ahead log change on every write.
pub(super) fn opencode_mark() -> (u64, u64) {
    let db = opencode_db();
    (super::mtime_ms(&db), super::mtime_ms(&db.with_extension("db-wal")))
}

// The parts of a session as rows: message id, role, whether the message is finished, and the
// part's fields that items are built from.
const PARTS: &str = "select p.session_id s, m.id mid, json_extract(m.data,'$.role') r, \
    (json_extract(m.data,'$.role') = 'user' or json_extract(m.data,'$.time.completed') is not null or json_extract(m.data,'$.error') is not null) done, \
    p.time_created c, json_extract(p.data,'$.type') t, substr(json_extract(p.data,'$.text'),1,40000) x, \
    json_extract(p.data,'$.synthetic') y, json_extract(p.data,'$.tool') n, json_extract(p.data,'$.callID') k, \
    json_extract(p.data,'$.state.status') st, json_extract(p.data,'$.state.input') i, \
    substr(json_extract(p.data,'$.state.output'),1,2000) o, substr(json_extract(p.data,'$.state.error'),1,2000) e, \
    json_extract(p.data,'$.state.attachments') a, \
    case when json_extract(p.data,'$.type') = 'file' then json_extract(p.data,'$.mime') end fm, \
    case when json_extract(p.data,'$.type') = 'file' then json_extract(p.data,'$.url') end fu \
    from part p join message m on m.id = p.message_id";

fn parts(ids: &[String], only_scan: bool) -> Option<Vec<Value>> {
    let list: Vec<String> = ids.iter().filter(|i| safe_id(i)).map(|i| format!("'{i}'")).collect();
    if list.is_empty() {
        return Some(Vec::new());
    }
    // the index needs your words, the agent's words and the edits
    let filter = if only_scan { " and (json_extract(p.data,'$.type') = 'text' or json_extract(p.data,'$.tool') in ('edit','write'))" } else { "" };
    try_query(&format!("{PARTS} where p.session_id in ({}){filter} order by p.session_id, m.time_created, m.id, p.id", list.join(",")))
}

// One session's parts from its `from`th message on, its messages counted as opencode_read counts them (those with
// parts, in order): following a session reads what is new, not every part of it on every poll.
fn parts_from(id: &str, from: u64) -> Option<Vec<Value>> {
    if !safe_id(id) {
        return Some(Vec::new());
    }
    try_query(&format!("{PARTS} where p.session_id = '{id}' and m.id in (select m2.id from message m2 join part p2 on p2.message_id = m2.id \
        where p2.session_id = '{id}' group by m2.id order by m2.time_created, m2.id limit -1 offset {from}) \
        order by p.session_id, m.time_created, m.id, p.id"))
}

// opencode's tools under Claude Code's names and input keys.
fn oc_tool(name: &str, i: &Value) -> (String, Value) {
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

fn oc_item(r: &Value, items: &mut Vec<Value>, calls: &mut HashMap<String, usize>) {
    let ts = r["c"].as_u64().unwrap_or(0);
    let text = r["x"].as_str().unwrap_or("");
    let user = r["r"] == "user";
    match r["t"].as_str().unwrap_or("") {
        "text" if user => {
            if r["y"].as_i64() != Some(1) {
                push_said(items, text, ts);
            }
        }
        "text" => {
            let t = text.trim();
            if !t.is_empty() {
                items.push(json!({"role": "assistant", "kind": "text", "text": t, "ts": ts}));
            }
        }
        "reasoning" => {
            let t = text.trim();
            if !t.is_empty() {
                items.push(json!({"role": "assistant", "kind": "thinking", "text": clip(t, 20000), "ts": ts}));
            }
        }
        "compaction" => items.push(json!({"role": "system", "kind": "note", "text": "Conversation compacted", "ts": ts})),
        "tool" => {
            let input = r["i"].as_str().and_then(|s| serde_json::from_str::<Value>(s).ok()).unwrap_or(Value::Null);
            let (name, input) = oc_tool(r["n"].as_str().unwrap_or(""), &input);
            let id = r["k"].as_str().unwrap_or("");
            calls.insert(id.into(), items.len());
            let mut t = tool_item(&name, &input, ts);
            t["id"] = Value::String(id.into());
            match r["st"].as_str().unwrap_or("") {
                "completed" => {
                    t["result"] = Value::String(clip(r["o"].as_str().unwrap_or("").trim(), 400));
                    t["error"] = Value::Bool(false);
                }
                "error" => {
                    t["result"] = Value::String(clip(r["e"].as_str().unwrap_or("").trim(), 400));
                    t["error"] = Value::Bool(true);
                }
                _ => {}
            }
            // an image the agent looked at rides on its call: opencode keeps it as a tool attachment
            if let Some(a) = attachments(r["a"].as_str().unwrap_or("")) {
                let pics: Vec<Value> = a.iter()
                    .filter(|f| f["mime"].as_str().unwrap_or("").starts_with("image/"))
                    .filter_map(|f| f["url"].as_str().and_then(media_from_url))
                    .collect();
                if !pics.is_empty() {
                    t["images"] = Value::Array(pics);
                }
            }
            items.push(t);
        }
        // a file sent with the message: an image shows as one of your pictures
        "file" if r["fm"].as_str().unwrap_or("").starts_with("image/") => {
            if let Some(p) = r["fu"].as_str().and_then(media_from_url) {
                let role = if user { "user" } else { "assistant" };
                items.push(json!({"role": role, "kind": "image", "images": [p], "ts": ts}));
            }
        }
        _ => {}
    }
}

// A tool part's attachments (sqlite hands the JSON back as text): the images the agent looked at.
fn attachments(s: &str) -> Option<Vec<Value>> {
    serde_json::from_str::<Value>(s).ok()?.as_array().cloned()
}

// The index's items for each of these sessions: your words, the agent's words and the edits.
pub(super) fn opencode_scan(ids: &[String]) -> Option<HashMap<String, Vec<Value>>> {
    let mut out: HashMap<String, Vec<Value>> = HashMap::new();
    let mut calls = HashMap::new();
    let v2 = v2_ids(ids);
    let (ids2, ids): (Vec<String>, Vec<String>) = ids.iter().cloned().partition(|i| v2.contains(i));
    for chunk in ids2.chunks(200) {
        for r in messages_v2(chunk, 0)? {
            let s = r["s"].as_str().unwrap_or("").to_string();
            oc2_items(&r, out.entry(s).or_default(), true);
        }
    }
    for chunk in ids.chunks(200) {
        for r in parts(chunk, true)? {
            let s = r["s"].as_str().unwrap_or("").to_string();
            oc_item(&r, out.entry(s).or_default(), &mut calls);
        }
    }
    Some(out)
}

// A session's items from its `from`th message on, each tagged with its message's number ("_at"),
// and where to go on: the number of the newest message while it is still being written, else of
// all of them. An older message an interrupted run left unfinished is read as it is.
pub(super) fn opencode_read(key: &Path, from: u64) -> (Vec<Value>, u64) {
    let Some(id) = opencode_id(key) else { return (Vec::new(), from) };
    if v2_ids(std::slice::from_ref(&id)).contains(&id) {
        return read_v2(&id, from);
    }
    let rows = parts_from(&id, from).unwrap_or_default();
    let newest = rows.last().and_then(|r| r["mid"].as_str()).unwrap_or("").to_string();
    let mut items = Vec::new();
    let mut calls = HashMap::new();
    let mut n: u64 = from;
    let mut mid = String::new();
    for r in rows {
        let m = r["mid"].as_str().unwrap_or("");
        if m != mid {
            if !mid.is_empty() {
                n += 1;
            }
            mid = m.to_string();
            if mid == newest && r["done"].as_i64() != Some(1) {
                return (items, n.max(from));
            }
        }
        let first = items.len();
        oc_item(&r, &mut items, &mut calls);
        for it in items[first..].iter_mut() {
            it["_at"] = json!(n);
        }
    }
    let count = if mid.is_empty() { 0 } else { n + 1 };
    (items, count.max(from))
}

// 2's messages from the `from`th on, numbered as opencode_read numbers 1.x's.
fn read_v2(id: &str, from: u64) -> (Vec<Value>, u64) {
    let rows = messages_v2(&[id.to_string()], from).unwrap_or_default();
    let mut items = Vec::new();
    let last = rows.len();
    for (i, r) in rows.iter().enumerate() {
        let n = from + i as u64;
        if i + 1 == last && !done_v2(r) {
            return (items, n);
        }
        let first = items.len();
        oc2_items(r, &mut items, false);
        for it in items[first..].iter_mut() {
            it["_at"] = json!(n);
        }
    }
    (items, from + last as u64)
}

pub(super) fn opencode_cwd(key: &Path) -> String {
    let Some(id) = opencode_id(key).filter(|i| safe_id(i)) else { return String::new() };
    let (v1, v2) = tables();
    let mut sql = Vec::new();
    if v2 {
        sql.push(format!("select directory from session_v2 where id = '{id}'"));
    }
    if v1 {
        sql.push(format!("select directory from session where id = '{id}'"));
    }
    if sql.is_empty() {
        return String::new();
    }
    query(&sql.join(" union all ")).first().and_then(|r| r["directory"].as_str().map(str::to_string)).unwrap_or_default()
}

// Sessions whose words (yours or the agent's) contain `q`: (session id, newest match time, its text),
// newest first, at most 100.
pub(super) fn opencode_search(q: &str) -> Vec<(String, u64, String)> {
    if !opencode_db().exists() {
        return Vec::new();
    }
    let like = q.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_").replace('\'', "''");
    let (v1, v2) = tables();
    let mut found = Vec::new();
    if v2 {
        // your words, and the agent's text blocks
        let sql = format!("select s, max(c) c, x from (\
            select m.session_id s, m.time_created c, json_extract(m.data,'$.text') x from session_message m where m.type = 'user' \
            union all select m.session_id, m.time_created, json_extract(j.value,'$.text') from session_message m, json_each(m.data,'$.content') j \
            where m.type = 'assistant' and json_extract(j.value,'$.type') = 'text') t join session_v2 ss on ss.id = t.s \
            where ss.parent_id is null and ss.time_archived is null and x like '%{like}%' escape '\\' group by s order by c desc limit 100");
        found.extend(query(&sql));
    }
    if !v1 {
        return found.into_iter().map(|r| (r["s"].as_str().unwrap_or("").to_string(), r["c"].as_u64().unwrap_or(0),
                                          r["x"].as_str().unwrap_or("").to_string())).collect();
    }
    let sql = format!("select p.session_id s, max(p.time_created) c, json_extract(p.data,'$.text') x \
        from part p join session ss on ss.id = p.session_id \
        where ss.parent_id is null and ss.time_archived is null and json_extract(p.data,'$.type') = 'text' and coalesce(json_extract(p.data,'$.synthetic'),0) = 0 \
        and json_extract(p.data,'$.text') like '%{like}%' escape '\\' \
        group by p.session_id order by c desc limit 100");
    found.extend(query(&sql));
    found.sort_by_key(|r| std::cmp::Reverse(r["c"].as_u64().unwrap_or(0)));
    found.truncate(100);
    found.into_iter().map(|r| (r["s"].as_str().unwrap_or("").to_string(), r["c"].as_u64().unwrap_or(0),
                                     r["x"].as_str().unwrap_or("").to_string())).collect()
}

// ── session files ───────────────────────────────────────────────────────────────

fn sh_ok(cmd: &mut Command) -> Result<(), String> {
    match cmd.output() {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => Err(String::from_utf8_lossy(&o.stderr).trim().to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn append_line(path: &Path, line: &str) -> Result<(), String> {
    fs::OpenOptions::new().append(true).open(path).and_then(|mut f| writeln!(f, "{line}")).map_err(|e| e.to_string())
}

// An ISO 8601 UTC time with milliseconds, as the agents write them.
fn iso(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // the civil date from days (Howard Hinnant's algorithm)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z", rem / 3600, rem % 3600 / 60, rem % 60, ms % 1000)
}

// A session renamed while no agent process has it open, in the agent's own record of names.
pub fn rename_offline(tool: &str, id: &str, path: &Path, title: &str) -> Result<(), String> {
    let now = super::now();
    match tool {
        // written with "type" first, as the agents write their lines (json! sorts the keys)
        "claude" => append_line(path, &format!(r#"{{"type":"custom-title","customTitle":{},"sessionId":{}}}"#, json!(title), json!(id))),
        "codex" => {
            let index = home().join(".codex/session_index.jsonl");
            fs::OpenOptions::new().create(true).append(true).open(&index)
                .and_then(|mut f| writeln!(f, "{}", json!({"id": id, "thread_name": title, "updated_at": iso(now)})))
                .map_err(|e| e.to_string())
        }
        "pi" => {
            if !path.to_string_lossy().ends_with(&format!("_{id}.jsonl")) {
                return Err(format!("{} is not pi session {id}", path.display()));
            }
            // a session_info entry continues the tree from its last entry, under an id not yet used
            let mut ids = HashSet::new();
            let mut last = Value::Null;
            lines(path, 0, |line| {
                if let Ok(v) = serde_json::from_slice::<Value>(line) {
                    if v["type"] != "session" {
                        if let Some(i) = v["id"].as_str() {
                            ids.insert(i.to_string());
                            last = json!(i);
                        }
                    }
                }
            });
            let mut salt = 0u64;
            let entry = loop {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                (now, std::process::id(), title, salt).hash(&mut h);
                let e = format!("{:08x}", h.finish() as u32);
                if !ids.contains(&e) {
                    break e;
                }
                salt += 1;
            };
            append_line(path, &format!(r#"{{"type":"session_info","id":"{entry}","parentId":{last},"timestamp":"{}","name":{}}}"#, iso(now), json!(title)))
        }
        _ => Err(format!("{tool} sessions are renamed through the agent")),
    }
}

// A Claude Code transcript and the folder beside it (sub-agents, tool results), or a pi session
// file, moved to the Trash.
pub fn trash_session(tool: &str, id: &str, path: &Path) -> Result<(), String> {
    trash_session_with(tool, id, path, &|p| sh_ok(Command::new("gio").args(["trash", "--"]).arg(p)))
}

// `trash`: moves one path to the Trash. The transcript is the session: once it is trashed the session
// is gone from every list, so a folder that then cannot be trashed is left behind and logged, not
// answered as a failure of a delete that happened.
fn trash_session_with(tool: &str, id: &str, path: &Path, trash: &dyn Fn(&Path) -> Result<(), String>) -> Result<(), String> {
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    match tool {
        "claude" if name == format!("{id}.jsonl") => {
            trash(path)?;
            let dir = path.with_extension("");
            if dir.is_dir() {
                if let Err(e) = trash(&dir) {
                    eprintln!("agent-host: session {id}: its folder was not trashed: {e}");
                }
            }
            Ok(())
        }
        "pi" if name.ends_with(&format!("_{id}.jsonl")) => trash(path),
        "claude" | "pi" => Err(format!("{} is not session {id}", path.display())),
        _ => Err(format!("{tool} sessions are not files to trash")),
    }
}

fn proc_start(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // field 22 (starttime); the command name (field 2) may hold spaces and parentheses
    stat.rsplit_once(')')?.1.split_whitespace().nth(19).map(str::to_string)
}

// The pid of a Claude Code process that has this session open. ~/.claude/sessions/<pid>.json
// outlives its process, and the pid can be reused: the process start time it records must still
// match the process.
pub fn terminal_holder(id: &str) -> Option<u32> {
    holder_outside(id, 0)
}

// Whether `pid` runs in a scope agent-host started for session `id` (main's spawn names it agent-<the id's first 8
// characters>-<host pid>-<n>): a turn a previous host's process is finishing, not a terminal.
pub fn in_session_scope(pid: u32, id: &str) -> bool {
    fs::read_to_string(format!("/proc/{pid}/cgroup")).is_ok_and(|c| session_scope(&c, id))
}

fn session_scope(cgroup: &str, id: &str) -> bool {
    let Some(short) = id.get(..8) else { return false };
    cgroup.lines().filter_map(|l| l.rsplit('/').next())
        .any(|unit| unit.strip_prefix("agent-").and_then(|u| u.strip_prefix(short)).is_some_and(|u| u.starts_with('-') && u.ends_with(".scope")))
}

// Whether `pid` is `ancestor` or runs beneath it.
fn runs_under(pid: u32, ancestor: u32) -> bool {
    let mut at = pid;
    for _ in 0..32 {
        if at == ancestor {
            return true;
        }
        let Some(stat) = fs::read_to_string(format!("/proc/{at}/stat")).ok() else { return false };
        // field 4 (ppid), after the command name that may hold spaces and parentheses
        match stat.rsplit_once(')').and_then(|r| r.1.split_whitespace().nth(1)).and_then(|p| p.parse::<u32>().ok()) {
            Some(p) if p > 1 => at = p,
            _ => return false,
        }
    }
    false
}

// Where Claude Code processes keep those records: Personal's ~/.claude/sessions, and the sessions folder in each added
// account's CLAUDE_CONFIG_DIR, which is not linked to Personal's.
pub(super) fn record_dirs() -> Vec<PathBuf> {
    std::iter::once(home().join(".claude")).chain(crate::accounts::claude_homes()).map(|d| d.join("sessions")).collect()
}

// terminal_holder, not counting the process `own` (0: none) and what runs beneath it: the host's own
// process for the session has a record too, and it is not a terminal.
pub fn holder_outside(id: &str, own: u32) -> Option<u32> {
    for f in record_dirs().iter().flat_map(|d| list_dir(d)) {
        if f.extension().map_or(true, |e| e != "json") {
            continue;
        }
        let Some(v) = fs::read(&f).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()) else { continue };
        if v["sessionId"] != id {
            continue;
        }
        let Some(pid) = v["pid"].as_u64().map(|p| p as u32).or_else(|| f.file_stem()?.to_str()?.parse().ok()) else { continue };
        let start = match &v["procStart"] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => continue,
        };
        if proc_start(pid).as_deref() == Some(start.as_str()) && (own == 0 || !runs_under(pid, own)) {
            return Some(pid);
        }
    }
    None
}

// Which agent a session id belongs to, from where its files are.
pub fn tool_of(id: &str) -> Option<&'static str> {
    if !safe_id(id) {
        return None;
    }
    if id.starts_with("ses_") {
        return Some("opencode");
    }
    let file = format!("{id}.jsonl");
    if list_dir(&home().join(".claude/projects")).iter().any(|d| d.join(&file).is_file()) {
        return Some("claude");
    }
    let tail = format!("_{id}.jsonl");
    if list_dir(&pi_dir()).iter().any(|d| list_dir(d).iter().any(|f| f.to_string_lossy().ends_with(&tail))) {
        return Some("pi");
    }
    let tail = format!("-{id}.jsonl");
    let mut found = false;
    super::walk(&home().join(".codex/sessions"), 4, &mut |f| found |= f.to_string_lossy().ends_with(&tail));
    found.then_some("codex")
}

// What a file is, from its extension.
pub(super) fn kind_of(path: &str) -> &'static str {
    let ext = path.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).unwrap_or_default();
    match ext.as_str() {
        "pdf" => "pdf",
        "doc" | "docx" | "odt" | "rtf" | "pages" => "doc",
        "xls" | "xlsx" | "ods" | "csv" | "tsv" | "numbers" => "sheet",
        "ppt" | "pptx" | "odp" | "key" => "slides",
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "bmp" | "tif" | "tiff" | "heic" | "avif" | "ico" => "image",
        "mp4" | "mov" | "webm" | "mkv" | "avi" | "m4v" => "video",
        "txt" | "md" | "markdown" => "text",
        _ => "other",
    }
}

fn ms(t: std::io::Result<std::time::SystemTime>) -> u64 {
    t.ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map_or(0, |d| d.as_millis() as u64)
}

const SKIP: [&str; 5] = ["node_modules", "target", "build", "venv", "__pycache__"];

// Files under `cwd` written since `since_ms`: what a turn made besides its tool-named writes.
// Three folders deep, past no hidden folder, build output or Python cache, 5000 entries at most; in
// the home folder only Documents, Pictures, Downloads and Videos. Folders are read in this order:
// those that gained or lost an entry during the turn (their time is `since` or later), then the
// smallest (a folder's size grows with its entries), so a large folder of old files cannot use up
// the 5000 before the folder the turn wrote into is reached.
pub fn sweep_made(cwd: &str, since_ms: u64) -> Vec<Value> {
    use std::cmp::Reverse;
    let home = home();
    let mut out = Vec::new();
    if cwd.is_empty() {
        return out;
    }
    let roots: Vec<(PathBuf, u32)> = if Path::new(cwd) == home {
        ["Documents", "Pictures", "Downloads", "Videos"].iter().map(|d| (home.join(d), 2)).collect()
    } else {
        vec![(PathBuf::from(cwd), 3)]
    };
    // (unchanged since the turn began, size, the folder, how deep under it to go), the least first
    let mut queue: std::collections::BinaryHeap<Reverse<(bool, u64, PathBuf, u32)>> =
        roots.into_iter().map(|(p, depth)| Reverse((false, 0, p, depth))).collect();
    let mut budget = 5000;
    while let Some(Reverse((_, _, dir, depth))) = queue.pop().filter(|_| budget > 0) {
        for p in list_dir(&dir) {
            if budget == 0 {
                break;
            }
            budget -= 1;
            let Ok(m) = fs::symlink_metadata(&p) else { continue };
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            if m.is_dir() {
                if depth > 0 && !name.starts_with('.') && !SKIP.contains(&name.as_str()) {
                    queue.push(Reverse((ms(m.modified()) < since_ms, m.len(), p, depth - 1)));
                }
            } else if m.is_file() {
                let at = ms(m.modified());
                if at >= since_ms {
                    let path = p.to_string_lossy().into_owned();
                    out.push(json!({"path": path, "kind": kind_of(&path), "size": m.len(), "at": at, "how": "sweep"}));
                }
            }
        }
    }
    out.sort_by(|a, b| b["at"].as_u64().cmp(&a["at"].as_u64()));
    out
}

fn made_file(id: &str) -> Option<PathBuf> {
    safe_id(id).then(|| home().join(".local/state/agent-host/made").join(format!("{id}.json")))
}

pub fn made_load(id: &str) -> Vec<Value> {
    made_file(id).and_then(|f| fs::read(f).ok()).and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .and_then(|v| v.as_array().cloned()).unwrap_or_default()
}

// Adds to what is stored for the session; an entry for a path already stored replaces it.
pub fn made_store(id: &str, made: &[Value]) -> Result<(), String> {
    let file = made_file(id).ok_or(format!("not a session id: {id}"))?;
    let mut all = made_load(id);
    for m in made {
        all.retain(|x| x["path"] != m["path"]);
        all.push(m.clone());
    }
    let _ = fs::create_dir_all(file.parent().unwrap());
    let tmp = file.with_extension("tmp");
    fs::write(&tmp, Value::Array(all).to_string()).and_then(|_| fs::rename(&tmp, &file)).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    // The transcript trashed, its folder not: the session is gone, so the delete is answered as done.
    #[test]
    fn a_folder_that_will_not_trash_does_not_fail_a_delete() {
        let dir = std::env::temp_dir().join(format!("sources-trash-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("abc-1/subagents")).unwrap();
        let file = dir.join("abc-1.jsonl");
        fs::write(&file, "{}\n").unwrap();
        let trashed = RefCell::new(Vec::new());
        let trash = |p: &Path| {
            if p.is_dir() {
                return Err("Operation not permitted".to_string());
            }
            trashed.borrow_mut().push(p.to_path_buf());
            Ok(())
        };
        assert_eq!(trash_session_with("claude", "abc-1", &file, &trash), Ok(()));
        assert_eq!(*trashed.borrow(), vec![file.clone()]);
        let refuse = |_: &Path| Err("no".to_string());
        assert_eq!(trash_session_with("claude", "abc-1", &file, &refuse), Err("no".to_string()), "the transcript failing is a failure");
        assert!(trash_session_with("claude", "other", &file, &trash).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    // Following an opencode session reads it from the message the app is on, with the items and numbers the whole
    // reading gives, on 1.x's tables and 2's. Nothing before that is read: an earlier part sqlite cannot read stands for
    // the work the whole reading did on every poll. (No `session` row, and 2's session has a parent, so the session
    // list of other tests leaves them out.)
    #[test]
    fn following_an_opencode_session_reads_what_is_new() {
        let _db = crate::tests::opencode_db_lock();
        crate::tests::scratch_home();
        let db = opencode_db();
        fs::create_dir_all(db.parent().unwrap()).unwrap();
        let sql = |q: &str| assert!(Command::new("sqlite3").arg(&db).arg(q).status().unwrap().success(), "{q}");
        sql("create table if not exists message (id text primary key, session_id text, time_created integer, data text); \
             create table if not exists part (id text primary key, message_id text, session_id text, time_created integer, data text); \
             create table if not exists session_v2 (id text primary key, directory text, title text, time_created integer, \
                 time_updated integer, parent_id text, time_archived integer, metadata text); \
             create table if not exists session_message (id text primary key, session_id text, seq integer, type text, \
                 time_created integer, data text);");
        // 1.x: a turn with a tool, a message with no parts (not counted), and a turn still being written
        let v1 = [("f1", 1000, r#"{"role":"user"}"#, vec![r#"{"type":"text","text":"first ask"}"#]),
                  ("f2", 1001, r#"{"role":"assistant","time":{"completed":2}}"#,
                   vec![r#"{"type":"text","text":"first answer"}"#, r#"{"type":"tool","tool":"bash","callID":"c1","state":{"status":"completed","input":{"command":"ls"},"output":"a b"}}"#]),
                  ("f3", 1002, r#"{"role":"assistant","time":{"completed":3}}"#, vec![]),
                  ("f4", 1003, r#"{"role":"user"}"#, vec![r#"{"type":"text","text":"second ask"}"#]),
                  ("f5", 1004, r#"{"role":"assistant"}"#, vec![r#"{"type":"text","text":"writing"}"#])];
        for (m, t, data, parts) in &v1 {
            sql(&format!("insert or replace into message values ('msg_{m}', 'ses_follow1', {t}, '{data}')"));
            for (i, p) in parts.iter().enumerate() {
                sql(&format!("insert or replace into part values ('prt_{m}{i}', 'msg_{m}', 'ses_follow1', {t}, '{p}')"));
            }
        }
        // 2: the same shape, and a kind the reading leaves out
        sql("insert or replace into session_v2 (id, directory, parent_id) values ('ses_follow2', '/tmp', 'ses_other')");
        let v2 = [(1, "user", r#"{"text":"first ask"}"#), (2, "assistant", r#"{"time":{"completed":2},"content":[{"type":"text","text":"first answer"}]}"#),
                  (3, "step", "{}"), (4, "user", r#"{"text":"second ask"}"#), (5, "assistant", r#"{"content":[{"type":"text","text":"writing"}]}"#)];
        for (seq, kind, data) in v2 {
            sql(&format!("insert or replace into session_message values ('msg_g{seq}', 'ses_follow2', {seq}, '{kind}', {}, '{data}')", 1000 + seq));
        }
        for (id, items) in [("ses_follow1", 4), ("ses_follow2", 3)] {
            let key = opencode_key(id);
            let (all, next) = opencode_read(&key, 0);
            assert_eq!((all.len(), next), (items, 3), "{id}: three messages read, the fourth still being written");
            for from in 0..=5 {
                let want: Vec<Value> = all.iter().filter(|i| i["_at"].as_u64().unwrap() >= from).cloned().collect();
                assert_eq!(opencode_read(&key, from), (want, next.max(from)), "{id} from {from}");
            }
        }
        let key = opencode_key("ses_follow1");
        let (want, _) = opencode_read(&key, 1);
        sql("insert or replace into part values ('prt_f0bad', 'msg_f1', 'ses_follow1', 1000, 'not json')");
        assert_eq!((opencode_read(&key, 1).0.len(), want.len()), (3, 3), "the first message is not read again");
        sql("delete from part where session_id = 'ses_follow1'; delete from message where session_id = 'ses_follow1'; \
             delete from session_message where session_id = 'ses_follow2'; delete from session_v2 where id = 'ses_follow2'");
    }

    // A file the turn wrote is found past a large folder of old files: the sweep read folders in listing order with
    // one budget of 5000 entries, and such a folder listed first used it up. A new file in a venv or a Python cache is
    // nothing the turn made for you.
    #[test]
    fn a_new_file_is_found_past_a_large_old_folder() {
        let dir = std::env::temp_dir().join(format!("agent-host-sweep-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        // made first, so a listing newest first (tmpfs) has the large folder before it
        for d in ["reports/2026", "venv", "tools/__pycache__", "data"] {
            fs::create_dir_all(dir.join(d)).unwrap();
        }
        for i in 0..6000 {
            fs::write(dir.join(format!("data/{i}.csv")), "").unwrap();
        }
        // file times come from a coarse clock: a pause on each side of the turn's start
        std::thread::sleep(std::time::Duration::from_millis(20));
        let since = ms(Ok(std::time::SystemTime::now()));
        std::thread::sleep(std::time::Duration::from_millis(20));
        for f in ["reports/2026/summary.md", "venv/pyvenv.cfg", "tools/__pycache__/x.cpython-314.pyc"] {
            fs::write(dir.join(f), "new").unwrap();
        }
        let made = sweep_made(&dir.to_string_lossy(), since);
        let _ = fs::remove_dir_all(&dir);
        let names: Vec<&str> = made.iter().map(|m| m["path"].as_str().unwrap().rsplit('/').next().unwrap()).collect();
        assert_eq!(names, ["summary.md"]);
    }

    #[test]
    fn a_session_scope_is_the_one_spawn_names() {
        let slice = "0::/user.slice/user-1000.slice/user@1000.service/app.slice";
        let id = "7f880686-1111-4000-8000-000000000000";
        assert!(session_scope(&format!("{slice}/agent-7f880686-86042-48.scope\n"), id));
        assert!(!session_scope(&format!("{slice}/agent-4a0f1c2e-86042-48.scope"), id), "another session's");
        assert!(!session_scope(&format!("{slice}/app-kitty-1234.scope"), id), "a terminal's");
        assert!(!session_scope(&format!("{slice}/agent-7f8806861-86042-48.scope"), id));
        assert!(!session_scope(&format!("{slice}/agent-7f880686-86042-48.scope"), "short"));
    }
}
