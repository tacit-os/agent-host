//! pi sessions: one `pi --mode rpc` process each, with agent-host's permission gate (pi-gate.ts,
//! compiled in and written to ~/.local/share/agent-host at spawn). Its events are relayed in Claude
//! Code's shape; the gate's questions are raised as can_use_tool. No prompt reaches the process
//! before the gate has reported itself: until then they wait in `held`. If it has not reported
//! within GATE_WAIT of the process starting, the held ones are refused, and so is anything sent
//! later. Following a session reads its JSONL file (history.rs).

use crate::{agent_command, ask, in_scope, read_err, read_out, relay, wake, with_mentions, write_in, Host, Session, Shared};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use std::{fs, process, thread};

const GATE: &str = include_str!("pi-gate.ts");
const GATE_WAIT: Duration = Duration::from_secs(15);
const ASKED: &str = "agent-host:";
const NO_GATE: &str = "pi's permission gate did not load, so the message was not sent.";

// The gate on disk, written when it is missing or differs from the one compiled in.
fn gate_file() -> Result<PathBuf, String> {
    let path = crate::home().join(".local/share/agent-host/pi-gate.ts");
    if fs::read_to_string(&path).ok().as_deref() != Some(GATE) {
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let tmp = dir.join(format!(".pi-gate.{}.ts", process::id()));
        fs::write(&tmp, GATE).and_then(|_| fs::rename(&tmp, &path)).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(path)
}

// `which`: --session-id <id> (new or resumed) or --fork <file>.
fn command(s: &Session, which: [&str; 2]) -> Result<Command, String> {
    let mut cmd = agent_command("pi", s.account.as_deref())?;
    cmd.args(["--mode", "rpc"]).args(which).arg("--no-extensions").arg("-e").arg(gate_file()?)
        .args(["--praxis-mode", s.mode_req.as_deref().unwrap_or("manual")]);
    if let Some(m) = &s.model_req {
        cmd.args(["--model", m.as_str()]);
    }
    if let Some(e) = &s.effort_req {
        cmd.args(["--thinking", e.as_str()]);
    }
    cmd.current_dir(&s.cwd);
    Ok(in_scope(&cmd, &format!("agent-pi-{}-{}", process::id(), s.gen)))
}

// A session's process starts; its lines are read by main's read_out, which hands them to event().
pub fn spawn(shared: &Shared, h: &mut Host, id: &str) -> Result<(), String> {
    h.next_gen += 1;
    let gen = h.next_gen;
    let s = h.sessions.get_mut(id).ok_or("no such session")?;
    s.gen = gen;
    let mut cmd = command(s, ["--session-id", id])?;
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start pi: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    started(s, write_in(child.stdin.take().unwrap(), id), child.id());
    let (a, b, c) = (shared.clone(), shared.clone(), shared.clone());
    let (ida, idb, idc) = (id.to_string(), id.to_string(), id.to_string());
    thread::spawn(move || read_out(a, ida, gen, stdout, child));
    thread::spawn(move || read_err(b, idb, gen, stderr));
    thread::spawn(move || refuse_late(c, idc, gen));
    Ok(())
}

fn started(s: &mut Session, to: std::sync::mpsc::Sender<String>, pid: u32) {
    s.to_child = Some(to);
    s.pid = pid;
    s.state = "idle";
    s.gate = false;
    s.spawned = Instant::now();
    s.held.clear();
    s.last = Instant::now();
    s.stderr.clear();
    s.closing = None;
    s.live.clear();
    s.calls.clear();
    s.native.clear();
}

// Messages still held when the gate has not loaded in time are not sent: the turn ends with why.
fn refuse_late(shared: Shared, id: String, gen: u64) {
    thread::sleep(GATE_WAIT);
    let mut h = shared.lock().unwrap();
    let Some(s) = h.sessions.get_mut(&id).filter(|s| s.gen == gen && !s.gate) else { return };
    if s.held.is_empty() {
        return;
    }
    s.held.clear();
    eprintln!("[{}] {NO_GATE}", &id[..8]);
    relay(&mut h, &id, gen, json!({"type": "result", "subtype": "error_during_execution", "is_error": true, "result": NO_GATE, "session_id": id}), None);
}

// A line for the process, held back until the gate has reported itself; refused once GATE_WAIT has
// passed without that report.
fn put(h: &mut Host, id: &str, line: Value) -> Result<(), String> {
    let s = h.sessions.get_mut(id).ok_or("no such session")?;
    let to = s.to_child.as_ref().ok_or("session is not running")?;
    if !s.gate {
        if s.spawned.elapsed() >= GATE_WAIT {
            return Err(NO_GATE.into());
        }
        s.held.push(format!("{line}\n"));
        return Ok(());
    }
    to.send(format!("{line}\n")).map_err(|_| "pi is not taking input".to_string())
}

// A message: a new prompt, or a steer into the running turn. Files are named in the text (pi reads
// them with its own tools); images go as base64.
pub fn send(shared: &Shared, h: &mut Host, id: &str, text: &str, images: &[String], files: &[String]) -> Result<(), String> {
    wake(shared, h, id)?;
    let mut line = json!({"type": "prompt", "message": with_mentions(text, files)});
    let pics: Vec<Value> = images.iter().filter_map(|p| {
        use base64::Engine;
        let bytes = fs::read(p).ok()?;
        let mime = crate::image_type(&bytes)?;
        Some(json!({"type": "image", "data": base64::engine::general_purpose::STANDARD.encode(bytes), "mimeType": mime}))
    }).collect();
    if !pics.is_empty() {
        line["images"] = Value::Array(pics);
    }
    if h.sessions.get(id).is_some_and(|s| s.state != "idle") {
        line["streamingBehavior"] = json!("steer");
    }
    put(h, id, line)
}

// Mode goes to the gate, which holds it; a model is "provider/id"; effort is pi's thinking level.
pub fn set(h: &mut Host, id: &str, op: &str, value: &str) -> Result<(), String> {
    if op == "mode" {
        return put(h, id, json!({"type": "prompt", "message": format!("/praxis-mode {value}")}));
    }
    if op == "effort" {
        return put(h, id, json!({"type": "set_thinking_level", "level": value}));
    }
    let (provider, model) = value.split_once('/').ok_or("a pi model is named provider/model")?;
    put(h, id, json!({"type": "set_model", "provider": provider, "modelId": model}))
}

pub fn answer(h: &mut Host, id: &str, native: Value, allow: bool, always: bool) -> Result<(), String> {
    let value = match (allow, always) {
        (true, true) => "Always",
        (true, false) => "Allow",
        _ => "Deny",
    };
    let s = h.sessions.get(id).ok_or("no such session")?;
    let to = s.to_child.as_ref().ok_or("session is not running")?;
    to.send(format!("{}\n", json!({"type": "extension_ui_response", "id": native["id"], "value": value})))
        .map_err(|_| "pi is not taking input".to_string())
}

// Questions still open are dismissed (the gate then refuses the tool), then the turn is aborted.
pub fn interrupt(h: &mut Host, id: &str) -> Result<(), String> {
    let s = h.sessions.get_mut(id).ok_or("no such session")?;
    let to = s.to_child.clone().ok_or("session is not running")?;
    let open: Vec<String> = s.native.keys().filter(|k| k.starts_with("req:")).cloned().collect();
    for k in open {
        if let Some(n) = s.native.remove(&k) {
            let _ = to.send(format!("{}\n", json!({"type": "extension_ui_response", "id": n["id"], "cancelled": true})));
        }
    }
    s.pending.clear();
    to.send(format!("{}\n", json!({"type": "abort"}))).map_err(|_| "pi is not taking input".to_string())
}

// A new session that starts as a copy of the file at `path`, run by the host. pi names it: its id
// is read from get_state before the session is registered, and the lines read up to then (the
// gate's report among them) are handled after.
// `base`: the mode, model and account of the session it forks, when the host runs that one.
pub fn fork(shared: &Shared, path: &Path, cwd: &str, req: &Value, base: (Option<String>, Option<String>, Option<String>)) -> Result<String, String> {
    let from = path.to_string_lossy().to_string();
    let mut s = Session::new("00000000-0000-0000-0000-000000000000", cwd, "pi");
    (s.mode_req, s.model_req, s.account) = base;
    crate::take_requests(&mut s, req);
    let gen = {
        let mut h = shared.lock().unwrap();
        h.next_gen += 1;
        h.next_gen
    };
    s.gen = gen;
    let mut cmd = command(&s, ["--fork", from.as_str()])?;
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("start pi: {e}"))?;
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let to = write_in(child.stdin.take().unwrap(), "pi-fork-");
    let _ = to.send(format!("{}\n", json!({"id": "host-fork", "type": "get_state"})));
    // read on a thread, so a process that never answers is given up on rather than waited for
    let (tx, rx) = channel();
    thread::spawn(move || {
        let mut out = BufReader::new(stdout);
        let mut before: Vec<Value> = Vec::new();
        let mut line = String::new();
        while out.read_line(&mut line).unwrap_or(0) > 0 {
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                if v["type"] == "response" && v["id"] == "host-fork" {
                    let _ = tx.send(Some((v, before, out)));
                    return;
                }
                before.push(v);
            }
            line.clear();
        }
        let _ = tx.send(None);
    });
    let Ok(Some((state, before, out))) = rx.recv_timeout(Duration::from_secs(30)) else {
        let _ = child.kill();
        let _ = child.wait();
        return Err("pi did not start the fork".into());
    };
    let id = state["data"]["sessionId"].as_str().unwrap_or("").to_string();
    if !crate::valid_id(&id) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("pi gave no session id: {}", state["error"].as_str().unwrap_or("")));
    }
    let mut h = shared.lock().unwrap();
    s.id = id.clone();
    s.used = true;
    started(&mut s, to, child.id());
    h.sessions.insert(id.clone(), s);
    for v in before {
        event(&mut h, &id, gen, v);
    }
    let (a, b, c) = (shared.clone(), shared.clone(), shared.clone());
    let (ida, idb, idc) = (id.clone(), id.clone(), id.clone());
    thread::spawn(move || read_out(a, ida, gen, out, child));
    thread::spawn(move || read_err(b, idb, gen, stderr));
    thread::spawn(move || refuse_late(c, idc, gen));
    h.announce(&id);
    Ok(id)
}

fn assistant(id: &str, content: Value) -> Value {
    json!({"type": "assistant", "message": {"role": "assistant", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

fn user(id: &str, content: Value) -> Value {
    json!({"type": "user", "message": {"role": "user", "content": content}, "parent_tool_use_id": null, "session_id": id})
}

fn stream(id: &str, e: Value) -> Value {
    json!({"type": "stream_event", "event": e, "parent_tool_use_id": null, "session_id": id})
}

fn text_of(c: &Value) -> String {
    match c.as_str() {
        Some(s) => s.to_string(),
        None => c.as_array().into_iter().flatten().filter(|b| b["type"] == "text").filter_map(|b| b["text"].as_str()).collect::<Vec<_>>().join("\n"),
    }
}

// pi's tools under Claude Code's names and input keys (as history.rs reads them from pi's files).
fn tool_input(name: &str, a: &Value, cwd: &str) -> (String, Value) {
    let path = |k: &str| {
        let p = a[k].as_str().unwrap_or("");
        if p.is_empty() || p.starts_with('/') || p.starts_with('~') { p.to_string() } else { format!("{cwd}/{p}") }
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

// A question from the gate in Claude Code's can_use_tool shape: an edit is asked as Edit (its first
// change, with all of them under "edits" when there are several).
fn asked(title: &str, cwd: &str) -> Option<(String, Value)> {
    let q: Value = serde_json::from_str(title.strip_prefix(ASKED)?).ok()?;
    let (name, mut input) = tool_input(q["tool"].as_str()?, &q["input"], cwd);
    if name != "MultiEdit" {
        return Some((name, input));
    }
    let edits = input["edits"].as_array().cloned().unwrap_or_default();
    let first = edits.first().cloned().unwrap_or(json!({}));
    input["old_string"] = first["old_string"].clone();
    input["new_string"] = first["new_string"].clone();
    if edits.len() < 2 {
        input.as_object_mut().unwrap().remove("edits");
    }
    Some(("Edit".into(), input))
}

// One line of a pi process, from read_out.
pub fn event(h: &mut Host, id: &str, gen: u64, v: Value) {
    let Some(s) = h.sessions.get_mut(id).filter(|s| s.gen == gen) else { return };
    let mut out: Vec<Value> = Vec::new();
    match v["type"].as_str().unwrap_or("") {
        "extension_ui_request" => {
            match v["method"].as_str().unwrap_or("") {
                "setStatus" if v["statusKey"] == "praxis-gate" => {
                    s.gate = true;
                    s.mode = v["statusText"].as_str().unwrap_or("").to_string();
                    if let Some(to) = &s.to_child {
                        for l in s.held.drain(..) {
                            let _ = to.send(l);
                        }
                    }
                    h.announce(id);
                }
                "select" if v["title"].as_str().is_some_and(|t| t.starts_with(ASKED)) => {
                    let Some((tool, input)) = asked(v["title"].as_str().unwrap_or(""), &s.cwd.clone()) else { return };
                    ask(h, id, gen, &tool, input, json!({"id": v["id"]}), v.clone());
                }
                // a dialog nothing here can show is dismissed rather than left waiting
                "select" | "confirm" | "input" | "editor" => {
                    if let Some(to) = &s.to_child {
                        let _ = to.send(format!("{}\n", json!({"type": "extension_ui_response", "id": v["id"], "cancelled": true})));
                    }
                }
                _ => {}
            }
            return;
        }
        "response" => {
            if v["success"] == false {
                let err = v["error"].as_str().unwrap_or("pi refused").to_string();
                eprintln!("[{}] {}: {err}", &id[..8], v["command"]);
                // a prompt pi would not take ends the turn it was to start
                if v["command"] == "prompt" && s.state != "idle" {
                    out.push(json!({"type": "result", "subtype": "error_during_execution", "is_error": true, "result": err, "session_id": id}));
                }
            }
        }
        "message_start" if v["message"]["role"] == "assistant" => out.push(stream(id, json!({"type": "message_start", "message": {}}))),
        "message_update" => {
            let e = &v["assistantMessageEvent"];
            let block = |b: Value| stream(id, json!({"type": "content_block_start", "index": 0, "content_block": b}));
            let delta = |d: Value| stream(id, json!({"type": "content_block_delta", "index": 0, "delta": d}));
            match e["type"].as_str().unwrap_or("") {
                "text_start" => out.push(block(json!({"type": "text", "text": ""}))),
                "thinking_start" => out.push(block(json!({"type": "thinking", "thinking": ""}))),
                "text_delta" => out.push(delta(json!({"type": "text_delta", "text": e["delta"]}))),
                "thinking_delta" => out.push(delta(json!({"type": "thinking_delta", "thinking": e["delta"]}))),
                _ => {}
            }
        }
        "message_end" => {
            let m = &v["message"];
            match m["role"].as_str().unwrap_or("") {
                "user" => out.push(user(id, json!(text_of(&m["content"])))),
                "assistant" => {
                    let cwd = s.cwd.clone();
                    let content: Vec<Value> = m["content"].as_array().into_iter().flatten().filter_map(|b| match b["type"].as_str()? {
                        "text" if !b["text"].as_str()?.trim().is_empty() => Some(json!({"type": "text", "text": b["text"]})),
                        "thinking" if !b["thinking"].as_str()?.trim().is_empty() => Some(json!({"type": "thinking", "thinking": b["thinking"]})),
                        "toolCall" => {
                            let (name, input) = tool_input(b["name"].as_str().unwrap_or(""), &b["arguments"], &cwd);
                            Some(json!({"type": "tool_use", "id": b["id"], "name": name, "input": input}))
                        }
                        _ => None,
                    }).collect();
                    let said = text_of(&m["content"]);
                    if !said.trim().is_empty() {
                        s.native.insert("last".into(), json!(said));
                    }
                    match m["stopReason"].as_str().unwrap_or("") {
                        "error" => {
                            s.native.insert("error".into(), m["errorMessage"].clone());
                        }
                        "aborted" => {
                            s.native.insert("aborted".into(), json!(true));
                        }
                        _ => {}
                    }
                    if !content.is_empty() {
                        out.push(assistant(id, Value::Array(content)));
                    }
                }
                "toolResult" => out.push(user(id, json!([{"type": "tool_result", "tool_use_id": m["toolCallId"], "content": text_of(&m["content"]),
                                                            "is_error": m["isError"] == true}]))),
                _ => {}
            }
        }
        // pi tries a failed reply again itself (overloaded, a rate limit, a 5xx, a dropped connection) and settles
        // only after: the error it retries did not end the turn, and a retry that fails again says so anew
        "auto_retry_start" => {
            s.native.remove("error");
        }
        "agent_settled" => {
            if s.state == "idle" {
                return;
            }
            let err = s.native.remove("error").filter(|e| !e.is_null());
            let aborted = s.native.remove("aborted").is_some();
            let last = s.native.remove("last").and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
            s.native.retain(|k, _| k.starts_with("req:"));
            let failed = err.is_some() && !aborted;
            let text = if failed { err.and_then(|e| e.as_str().map(str::to_string)).unwrap_or_else(|| "The turn failed.".into()) } else { last };
            out.push(json!({"type": "result", "subtype": if failed || aborted { "error_during_execution" } else { "success" },
                            "is_error": failed, "result": text, "session_id": id}));
        }
        "extension_error" => {
            let line = format!("gate: {} {}", v["event"].as_str().unwrap_or(""), v["error"].as_str().unwrap_or(""));
            eprintln!("[{}] {line}", &id[..8]);
            s.stderr.push_back(line);
        }
        _ => {}
    }
    for data in out {
        relay(h, id, gen, data, Some(v.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A turn whose reply failed once and came on pi's own retry ends as a success with the reply, not as the error.
    #[test]
    fn a_reply_that_came_on_a_retry_is_not_a_failed_turn() {
        let id = "5c0f1c2e-0000-4000-8000-00000000e001";
        let mut h = Host::default();
        let mut s = Session::new(id, "/", "pi");
        s.state = "working";
        h.sessions.insert(id.into(), s);
        let said = |m: Value| json!({"type": "message_end", "message": m});
        // as pi 0.86.1's agent-session.js sends them: the failed reply, the retry, the reply, then settled
        for v in [said(json!({"role": "assistant", "content": [], "stopReason": "error", "errorMessage": "529 overloaded_error: Overloaded"})),
                  json!({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 2000, "errorMessage": "529 overloaded_error: Overloaded"}),
                  said(json!({"role": "assistant", "content": [{"type": "text", "text": "Done: the tests pass."}], "stopReason": "stop"})),
                  json!({"type": "auto_retry_end", "success": true, "attempt": 1}),
                  json!({"type": "agent_settled"})] {
            event(&mut h, id, 0, v);
        }
        let (_, line) = h.sessions[id].events.back().unwrap();
        let r = &serde_json::from_str::<Value>(line).unwrap()["data"];
        assert_eq!((r["type"].as_str(), r["is_error"].as_bool(), r["result"].as_str()), (Some("result"), Some(false), Some("Done: the tests pass.")), "{r}");
        assert_eq!(r["subtype"], "success");
    }
}
