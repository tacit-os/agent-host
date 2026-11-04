//! opencode 2's API (GitHub issue #2): the same `opencode serve`, but every route under /api, replies wrapped in
//! `data`, and an event stream of its own (/api/event: `{type, data}`, one session's turn as
//! session.step/text/reasoning/tool events between session.execution.started and .succeeded, .failed or
//! .interrupted). 1.x keeps the old API (opencode.rs); which one a program speaks is read from its version.
//! Shapes read off opencode 2.1.1 in a sandbox (its own HOME; a 2.x server rewrites the database 1.x reads).

use crate::codex::split_diff;
use crate::opencode::{assistant, stream, user};
use crate::{ask, relay, Host};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::channel;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};
use std::{env, fs, thread};

/// The major version of the opencode `cmd` runs, from `--version` ("opencode v2.1.1", "1.18.31"), read once per
/// binary and change; 1 when it does not say.
pub fn major(cmd: &Command) -> u32 {
    let prog = cmd.get_program();
    let path = cmd.get_envs().find(|(k, _)| *k == "PATH").and_then(|(_, v)| v.map(|v| v.to_os_string())).or_else(|| env::var_os("PATH"));
    let bin = if Path::new(prog).is_absolute() {
        PathBuf::from(prog)
    } else {
        env::split_paths(&path.clone().unwrap_or_default()).map(|d| d.join(prog)).find(|p| p.is_file()).unwrap_or_else(|| PathBuf::from(prog))
    };
    let stamp = fs::metadata(&bin).and_then(|m| m.modified()).ok();
    static SEEN: OnceLock<Mutex<HashMap<PathBuf, (Option<SystemTime>, u32)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(Default::default);
    if let Some((t, v)) = seen.lock().unwrap().get(&bin) {
        if *t == stamp {
            return *v;
        }
    }
    let mut c = Command::new(&bin);
    c.arg("--version");
    if let Some(p) = path {
        c.env("PATH", p);
    }
    let out = crate::signin::output(&mut c, Duration::from_secs(15)).map(|(_, o)| o).unwrap_or_default();
    let v = parse_major(&out);
    seen.lock().unwrap().insert(bin, (stamp, v));
    v
}

/// The models opencode 2 offers the account `cmd` runs as: a short-lived `opencode serve` behind a random password
/// (2's CLI lists names only), GET /api/model, the server stopped (its process group) after.
pub fn models(mut cmd: Command, wait: Duration) -> Result<Vec<Value>, String> {
    use base64::Engine;
    let port = TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map_err(|e| e.to_string())?.port();
    let mut secret = [0u8; 18];
    fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut secret)).map_err(|e| e.to_string())?;
    let password = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let auth = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}")));
    cmd.args(["serve", "--hostname", "127.0.0.1", "--port", &port.to_string()]).env("OPENCODE_SERVER_PASSWORD", &password)
        .current_dir(crate::home()).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).process_group(0);
    let mut child = cmd.spawn().map_err(|e| format!("start opencode: {e}"))?;
    let out = child.stdout.take().ok_or("opencode: no stdout")?;
    let (ready_tx, ready) = channel::<()>();
    thread::spawn(move || {
        for l in BufReader::new(out).lines().map_while(Result::ok) {
            if l.contains("listening on") {
                let _ = ready_tx.send(());
            }
        }
    });
    let r = match ready.recv_timeout(wait) {
        Ok(()) => crate::opencode::http(port, &auth, "GET", "/api/model", None),
        Err(_) => Err("opencode serve did not start".into()),
    };
    let _ = Command::new("kill").arg("--").arg(format!("-{}", child.id())).status();
    let _ = child.wait();
    Ok(r?["data"].as_array().cloned().unwrap_or_default())
}

pub fn parse_major(version: &str) -> u32 {
    let t = version.trim();
    let t = t.strip_prefix("opencode").unwrap_or(t).trim();
    t.strip_prefix('v').unwrap_or(t).split('.').next().and_then(|m| m.trim().parse().ok()).unwrap_or(1)
}

/// The folder a request is about, as 2's `location` query (a deep object: location[directory]).
pub fn loc(cwd: &str) -> String {
    let mut q = String::from("?location%5Bdirectory%5D=");
    for b in cwd.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            q.push(b as char);
        } else {
            q += &format!("%{b:02X}");
        }
    }
    q
}

/// A mode as 2's permission rules (its shell tool is "shell"), and the agent it runs with. The session's rules come
/// after the agent's, which let everything through; the last match wins.
pub fn rules(mode: Option<&str>) -> (Value, &'static str) {
    let r = |shell: &str, edit: &str| json!([{"action": "shell", "resource": "*", "effect": shell}, {"action": "edit", "resource": "*", "effect": edit}]);
    match mode {
        Some("plan") => (r("ask", "deny"), "plan"),
        Some("acceptEdits" | "auto") => (r("ask", "allow"), "build"),
        _ => (r("ask", "ask"), "build"),
    }
}

/// "opencode/big-pickle" as 2's model reference, with the effort as its variant.
pub fn model_ref(model: &str, effort: Option<&str>) -> Option<Value> {
    let (provider, id) = model.split_once('/')?;
    let mut m = json!({"id": id, "providerID": provider});
    if let Some(e) = effort {
        m["variant"] = json!(e);
    }
    Some(m)
}

/// 2's tools under Claude Code's names and input keys (its paths are `path`; 1.x said `filePath`).
pub fn tool_of(name: &str, i: &Value) -> (String, Value) {
    let path = if i["path"].is_null() { i["filePath"].clone() } else { i["path"].clone() };
    match name {
        "shell" | "bash" => ("Bash".into(), json!({"command": i["command"], "description": i["description"]})),
        "read" => ("Read".into(), json!({"file_path": path})),
        "edit" => ("Edit".into(), json!({"file_path": path, "old_string": i["oldString"], "new_string": i["newString"]})),
        "write" => ("Write".into(), json!({"file_path": path, "content": i["content"]})),
        "grep" => ("Grep".into(), json!({"pattern": i["pattern"], "path": i["path"]})),
        "glob" => ("Glob".into(), json!({"pattern": i["pattern"], "path": i["path"]})),
        "webfetch" => ("WebFetch".into(), json!({"url": i["url"]})),
        "websearch" => ("WebSearch".into(), json!({"query": i["query"]})),
        "todowrite" | "todo" => ("TodoWrite".into(), json!({"todos": i["todos"]})),
        "task" | "subagent" => ("Agent".into(), json!({"description": i["description"], "prompt": i["prompt"], "subagent_type": i["subagent_type"].as_str().or(i["agent"].as_str())})),
        _ => (name.to_string(), i.clone()),
    }
}

/// A permission request as the tool Claude Code would have asked about. 2 names the files relative to the session's
/// folder and sends an edit as a patch.
fn permission_input(d: &Value, cwd: &str, call_input: Option<&Value>) -> (String, Value) {
    let first = d["resources"][0].as_str().unwrap_or("");
    let abs = |p: &str| if p.starts_with('/') || cwd.is_empty() { p.to_string() } else { format!("{}/{p}", cwd.trim_end_matches('/')) };
    match d["action"].as_str().unwrap_or("") {
        // the call's whole command: the request lists it in parts ("sleep 30 ; echo woke" as two)
        "shell" => ("Bash".into(), json!({"command": call_input.and_then(|i| i["command"].as_str()).unwrap_or(first)})),
        "edit" => {
            let f = &d["metadata"]["files"][0];
            let path = abs(f["file"].as_str().unwrap_or(first));
            // from its first hunk: the "Index:" and "====" lines before it are no file's text, and a file written as
            // it was has no hunk at all
            let patch = f["patch"].as_str().unwrap_or("");
            let (old, new) = split_diff(patch.find("\n@@").map(|i| &patch[i + 1..]).unwrap_or(""));
            if f["status"] == "added" {
                ("Write".into(), json!({"file_path": path, "content": new}))
            } else {
                ("Edit".into(), json!({"file_path": path, "old_string": old, "new_string": new}))
            }
        }
        other => (other.to_string(), if d["metadata"].is_null() { json!({"resources": d["resources"]}) } else { d["metadata"].clone() }),
    }
}

// A tool's result as text: its content's text parts.
fn content_text(c: &Value) -> String {
    c.as_array().into_iter().flatten().filter_map(|p| p["text"].as_str()).collect::<Vec<_>>().join("\n")
}

/// One event of 2's stream, for a session the host has open on this server, in Claude Code's shape.
pub fn event(h: &mut Host, gen: u64, e: &Value) {
    let d = &e["data"];
    let Some(id) = d["sessionID"].as_str().map(str::to_string) else { return };
    let Some(s) = h.sessions.get_mut(&id).filter(|s| s.tool == "opencode" && s.gen == gen && s.to_child.is_some()) else { return };
    let ty = e["type"].as_str().unwrap_or("");
    let mut out: Vec<Value> = Vec::new();
    match ty {
        "session.inbox.enqueued" if d["item"]["type"] == "user" => {
            if let Some(t) = d["item"]["payload"]["text"].as_str() {
                out.push(user(&id, json!(t)));
            }
        }
        "session.step.started" => out.push(stream(&id, json!({"type": "message_start", "message": {}}))),
        "session.text.started" => out.push(stream(&id, json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}))),
        "session.reasoning.started" => out.push(stream(&id, json!({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}}))),
        "session.text.delta" => out.push(stream(&id, json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": d["delta"]}}))),
        "session.reasoning.delta" => out.push(stream(&id, json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": d["delta"]}}))),
        "session.text.ended" | "session.reasoning.ended" => {
            let t = d["text"].as_str().unwrap_or("");
            if !t.trim().is_empty() {
                if ty == "session.text.ended" {
                    s.native.insert("last".into(), json!(t));
                    out.push(assistant(&id, json!([{"type": "text", "text": t}])));
                } else {
                    out.push(assistant(&id, json!([{"type": "thinking", "thinking": t}])));
                }
            }
        }
        // the tool's name comes as its input starts; the call, with its input, after
        "session.tool.input.started" => {
            s.native.insert(format!("tool:{}", d["id"].as_str().unwrap_or("")), d["name"].clone());
        }
        "session.tool.called" => {
            let call = d["id"].as_str().unwrap_or("").to_string();
            if s.native.insert(format!("call:{call}"), json!(true)).is_none() {
                let name = s.native.get(&format!("tool:{call}")).and_then(|n| n.as_str()).unwrap_or("").to_string();
                s.native.insert(format!("input:{call}"), d["input"].clone());
                let (name, input) = tool_of(&name, &d["input"]);
                out.push(assistant(&id, json!([{"type": "tool_use", "id": call, "name": name, "input": input}])));
            }
        }
        "session.tool.success" | "session.tool.failed" => {
            let call = d["id"].as_str().unwrap_or("").to_string();
            if s.native.insert(format!("result:{call}"), json!(true)).is_none() {
                let failed = ty == "session.tool.failed";
                let text = if failed { d["error"]["message"].as_str().unwrap_or("The tool failed.").to_string() } else { content_text(&d["content"]) };
                out.push(user(&id, json!([{"type": "tool_result", "tool_use_id": call, "content": text, "is_error": failed}])));
            }
        }
        "permission.asked" => {
            let call_input = d["source"]["id"].as_str().and_then(|c| s.native.get(&format!("input:{c}"))).cloned();
            let (tool, input) = permission_input(d, &s.cwd, call_input.as_ref());
            ask(h, &id, gen, &tool, input, json!({"id": d["id"]}), e.clone());
            return;
        }
        // answered somewhere else, or settled by opencode itself
        "permission.replied" => {
            let rid = &d["requestID"];
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
        "session.execution.succeeded" | "session.execution.failed" | "session.execution.interrupted" => {
            if s.state == "idle" {
                return;
            }
            let last = s.native.remove("last").and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
            s.native.retain(|k, _| k.starts_with("req:"));
            let failed = ty == "session.execution.failed";
            let text = if failed { d["error"]["message"].as_str().unwrap_or("The turn failed.").to_string() } else { last };
            let mut r = json!({"type": "result", "subtype": if ty == "session.execution.succeeded" { "success" } else { "error_during_execution" },
                               "is_error": failed, "result": text, "session_id": id});
            // its provider refused the sign-in or key: the host ends the session as signed out
            if d["error"]["type"] == "provider.auth" {
                r["authError"] = json!(true);
            }
            out.push(r);
        }
        _ => {}
    }
    for data in out {
        relay(h, &id, gen, data, Some(e.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(parse_major("opencode v2.1.1\n"), 2);
        assert_eq!(parse_major("1.18.31"), 1);
        assert_eq!(parse_major(""), 1);
    }

    #[test]
    fn edits_asked() {
        let d = json!({"action": "edit", "resources": ["note.txt"], "metadata": {"files": [{"file": "note.txt", "status": "modified",
                       "patch": "Index: note.txt\n===\n--- note.txt\n+++ note.txt\n@@ -1,1 +1,1 @@\n-hello\n+goodbye\n"}]}});
        let (t, i) = permission_input(&d, "/w", None);
        assert_eq!(t, "Edit");
        assert_eq!(i["file_path"], "/w/note.txt");
        assert!(i["old_string"].as_str().unwrap().contains("hello") && i["new_string"].as_str().unwrap().contains("goodbye"));
        let (t, i) = permission_input(&json!({"action": "shell", "resources": ["ls /"]}), "/w", None);
        assert_eq!((t.as_str(), i["command"].as_str()), ("Bash", Some("ls /")));
    }

    #[test]
    fn location_query() {
        assert_eq!(loc("/home/a b"), "?location%5Bdirectory%5D=/home/a%20b");
    }
}
