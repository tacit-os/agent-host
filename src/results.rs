//! Results (hello feature "results"): what a session's agents found, for the app's Results page.
//!
//!   → {"op":"results","id":ID}
//!   ← {"ev":"results","id":ID,"findings":[..],"reports":[..],"failures":[..],"pictures":[..],"changes":[..],
//!      "counts":{..},"unread":{count,cost},"reading":N,"error":?TEXT}
//!   → {"op":"read_reports","id":ID}   ← {"ev":"read_reports","id":ID,"queued":N}
//!   → {"op":"ask","id":ID,"q":TEXT}   ← {"ev":"ask","id":ID,"q":TEXT,"answer":TEXT,"sources":[{source,name}],"cost":USD}
//!
//! A source is a sub-agent or workflow agent (Claude Code sessions), or, in a session without them
//! (any agent), one of the session's own turns. A structured report (a workflow agent's
//! StructuredOutput) is read field by field, with no model: its findings, issues, risks, what it
//! left unverified, what it fixed, each with the agent's own severity; a checker's verdicts mark the
//! findings of its workflow they name by id as checked, and as not a bug when not real. A written report gives
//! findings only once read by Sonnet (`claude -p`, no tools, no hooks, no session kept), which
//! happens only on read_reports, two calls at a time: each finding must quote the report or it is
//! dropped, and a reading is kept in ~/.cache/agent-host/results/<sha1>.json, so a report is never
//! read twice. `unread` counts the written reports not read yet, with an estimate of their cost.
//! A finding that needs a look whose file was written after its report ended carries
//! "changedSince" (the file's mtime, ms): it may have been fixed since, by work this session's
//! reports do not tell of.
//! Owner, 2026-10-07: no model calls unless asked for.

use crate::{accounts, catalog, history, keys};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{mpsc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::Duration;

// Change when the prompt or the schema changes: earlier readings are then not used.
const READ_VERSION: &str = "read-sonnet-1";
const MODEL: &str = "sonnet";
const WORKERS: usize = 2;
const CALL_WAIT: Duration = Duration::from_secs(240);

const STATUSES: [&str; 6] = ["problem", "open", "fixed", "done", "confirmed", "not_a_bug"];

const READ_SYSTEM: &str = "You read one report that an AI coding agent wrote at the end of its task, and list what \
the person who started it needs to know: problems it found, what it fixed or built, what it left open or did not \
check, and checks that came out fine. Write plainly, without praise. title: at most 12 words. detail: one or two \
sentences with the specifics (names, numbers, places). status: problem (something is wrong and not fixed), open \
(left undone, not verified, or waiting for a decision), fixed (was wrong, is fixed), done (built or changed as \
asked), confirmed (checked, and it holds or is real), not_a_bug (checked, and it is not a problem). quote: copy 5 \
to 30 consecutive words exactly from the report that say it. files: the paths the finding is about, as the report \
writes them. At most 8 findings, the most important first; a short report may have one or none.";

const READ_SCHEMA: &str = r#"{"type":"object","properties":{"findings":{"type":"array","maxItems":8,"items":{"type":"object","properties":{"title":{"type":"string"},"detail":{"type":"string"},"status":{"type":"string","enum":["problem","open","fixed","done","confirmed","not_a_bug"]},"quote":{"type":"string"},"files":{"type":"array","items":{"type":"string"}}},"required":["title","detail","status","quote"]}}},"required":["findings"]}"#;

const ASK_SYSTEM: &str = "You answer a question about what AI agents did and found in one session, using only \
the findings and reports given. Be brief and specific; say so when the material does not tell. sources: the ids \
in square brackets of the reports your answer rests on.";

const ASK_SCHEMA: &str = r#"{"type":"object","properties":{"answer":{"type":"string"},"sources":{"type":"array","items":{"type":"string"}}},"required":["answer","sources"]}"#;

// One report and what came with it.
struct Source {
    key: String,     // agent id, or "turn-N"
    name: String,
    run: String,     // the workflow run of a workflow agent
    state: String,   // running / done / failed / stopped
    asked: String,
    report: String,
    structured: bool,
    ended: u64,
    files: Vec<Value>,
    pictures: Vec<Value>,
}

fn sources(path: &Path, tool: &str, agents: &Value, working: bool) -> Vec<Source> {
    let mut out = Vec::new();
    let text = |v: &Value, k: &str| v[k].as_str().unwrap_or("").to_string();
    let mut add = |id: &str, name: String, run: String, state: String| {
        let d = history::agent_digest(path, id);
        let list = |k: &str| d[k].as_array().cloned().unwrap_or_default();
        // a structured report that is prose in a field (report, markdown) is a written one
        let (mut report, mut structured) = (text(&d, "report"), d["reportKind"] == "json");
        if let Some(p) = structured.then(|| prose_of(&report)).flatten() {
            (report, structured) = (p, false);
        }
        out.push(Source { key: id.to_string(), name, run, state, asked: text(&d, "asked"), report, structured,
                          ended: d["ended"].as_u64().unwrap_or(0), files: list("files"), pictures: list("pictures") });
    };
    for a in agents["agents"].as_array().into_iter().flatten() {
        let name = [text(a, "description"), text(a, "name")].into_iter().find(|s| !s.is_empty()).unwrap_or("Agent".into());
        add(a["agent"].as_str().unwrap_or(""), name, String::new(), text(a, "state"));
    }
    for r in agents["workflows"].as_array().into_iter().flatten() {
        for m in r["agents"].as_array().into_iter().flatten() {
            let name = Some(text(m, "label")).filter(|s| !s.is_empty()).unwrap_or("Workflow agent".into());
            add(m["agent"].as_str().unwrap_or(""), name, text(r, "run"), text(m, "state"));
        }
    }
    if out.is_empty() {
        out = turns(path, tool, working);
    }
    out.sort_by_key(|s| s.ended);
    out
}

static TURNS: OnceLock<Mutex<HashMap<PathBuf, ((u64, u64), Vec<Value>)>>> = OnceLock::new();

// A session's own turns as sources (its last 30 with words at the end): what was asked, the last
// thing said, what its steps changed and the pictures they read. The newest turn counts as running
// while the host's session is working (`working`); a transcript written a moment ago says nothing,
// the turn that just ended wrote it.
fn turns(path: &Path, tool: &str, working: bool) -> Vec<Source> {
    // kept until the transcript changes (an opencode session's key is no file: its length was always 0)
    let mark = history::transcript_mark(path);
    let cache = TURNS.get_or_init(Default::default);
    let cached = cache.lock().unwrap().get(path).filter(|(m, _)| *m == mark).map(|(_, v)| v.clone());
    let list = cached.unwrap_or_else(|| {
        let (items, _) = history::transcript_from(path, tool, 0, true);
        let mut list: Vec<Value> = Vec::new();
        let mut step = 0usize;
        for it in &items {
            let user = it["role"] == "user" && it["kind"] == "text";
            if user || list.is_empty() {
                list.push(json!({"asked": if user { it["text"].clone() } else { json!("") }, "report": "", "ended": it["ts"], "files": [], "pictures": []}));
                step = 0;
                if user {
                    continue;
                }
            }
            let t = list.last_mut().unwrap();
            if it["ts"].as_u64().is_some() {
                t["ended"] = it["ts"].clone();
            }
            if it["role"] == "assistant" && it["kind"] == "text" {
                t["report"] = it["text"].clone();
            }
            if it["kind"] == "tool" {
                for f in it["files"].as_array().into_iter().flatten() {
                    t["files"].as_array_mut().unwrap().push(json!({"path": f["path"], "add": f["add"], "del": f["del"], "step": step}));
                }
                for p in it["images"].as_array().into_iter().flatten() {
                    t["pictures"].as_array_mut().unwrap().push(json!({"path": p, "step": step, "what": it["summary"]}));
                }
                step += 1;
            }
        }
        cache.lock().unwrap().insert(path.to_path_buf(), (mark, list.clone()));
        list
    });
    let n = list.len();
    list.into_iter().enumerate().filter(|(_, t)| t["report"] != "").collect::<Vec<_>>().into_iter().rev().take(30).rev()
        .map(|(i, t)| Source {
            key: format!("turn-{}", i + 1), name: format!("Turn {}", i + 1), run: String::new(),
            state: if working && i + 1 == n { "running" } else { "done" }.into(),
            asked: t["asked"].as_str().unwrap_or("").into(), report: t["report"].as_str().unwrap_or("").into(),
            structured: false, ended: t["ended"].as_u64().unwrap_or(0),
            files: t["files"].as_array().cloned().unwrap_or_default(), pictures: t["pictures"].as_array().cloned().unwrap_or_default(),
        }).collect()
}

// ── structured reports, read field by field ─────────────────────────────────

// What a field's items are, by the field's name in lower case without separators: findings to look
// at, things left open, fixed, confirmed, not a bug, items done, a checker's verdicts; None for
// fields that are not findings (files, commits, renders, notes, ...).
fn field_status(key: &str) -> Option<&'static str> {
    let k: String = key.chars().filter(|c| c.is_alphanumeric()).collect::<String>().to_lowercase();
    Some(match k.as_str() {
        "findings" | "issues" | "problems" | "bugs" | "gaps" | "regressions" | "blockers" | "defects" | "missed" | "holes"
        | "missing" => "problem",
        "risks" | "openissues" | "openquestions" | "open" | "deferred" | "todo" | "todos" | "unverified"
        | "deviceunverified" | "onlybuilt" | "notverified" | "needsfromotherlanes" | "followups" | "unresolved"
        | "deviations" | "leftopen" | "ownerquestions" | "questions" | "disputes" => "open",
        "fixed" | "fixes" | "resolved" | "corrections" => "fixed",
        "verified" | "verifiedbyrunning" | "confirmed" => "confirmed",
        "notreal" | "falsepositives" | "rejected" => "not_a_bug",
        "items" => "done",
        "verdicts" => "verdicts",
        _ => return None,
    })
}

fn first_text(o: &Map<String, Value>, keys: &[&str]) -> String {
    keys.iter().filter_map(|k| o.get(*k).and_then(Value::as_str)).map(str::trim).find(|s| !s.is_empty()).unwrap_or("").to_string()
}

// P1 / blocker / critical / high → high, P2 / major / medium → medium, P3 / minor / low / note → low
fn level(severity: &str) -> &'static str {
    match severity.to_lowercase().as_str() {
        "p0" | "p1" | "blocker" | "critical" | "high" | "severe" => "high",
        "p2" | "major" | "medium" | "moderate" => "medium",
        "" => "",
        _ => "low",
    }
}

// A sentence or a short paragraph as a title and the rest.
fn split_title(s: &str) -> (String, String) {
    let s = s.trim();
    let end = s.char_indices().find(|&(i, c)| matches!(c, '.' | ';' | ':') && i >= 12 && s[i + 1..].starts_with(' ')).map(|(i, _)| i);
    match end {
        Some(i) if i < 160 => (s[..i].to_string(), s[i + 1..].trim().to_string()),
        _ if s.chars().count() > 160 => (s.chars().take(150).collect::<String>() + "…", s.to_string()),
        _ => (s.to_string(), String::new()),
    }
}

fn item_finding(item: &Value, status: &str) -> Option<Value> {
    if let Some(s) = item.as_str() {
        let (title, detail) = split_title(s);
        return (!title.is_empty()).then(|| json!({"title": title, "detail": detail, "status": status, "files": []}));
    }
    let o = item.as_object()?;
    let title = first_text(o, &["title", "problem", "claim", "what", "issue", "summary", "symptom", "finding", "name", "description", "what_changed"]);
    if title.is_empty() {
        return None;
    }
    let (title, more) = split_title(&title);
    let said = first_text(o, &["problem", "claim", "symptom", "cause", "failureScenario", "scenario", "why", "evidence", "what_changed", "reason"]);
    let detail = [more, said].into_iter().filter(|d| !d.is_empty() && !d.starts_with(&title)).collect::<Vec<_>>().join(" ");
    let severity = first_text(o, &["severity", "priority", "impact"]);
    let status = if o.get("real") == Some(&json!(false)) {
        "not_a_bug".to_string()
    } else if o.get("fixed") == Some(&json!(true)) {
        "fixed".to_string()
    } else if let Some(done) = o.get("done").and_then(Value::as_bool) {
        if done { "done" } else { "open" }.to_string()
    } else {
        o.get("status").and_then(Value::as_str).map(str::to_lowercase).filter(|s| STATUSES.contains(&s.as_str()))
            .unwrap_or(status.to_string())
    };
    let file = first_text(o, &["file", "where", "path"]);
    let files: Vec<Value> = if !file.is_empty() { vec![json!(file)] } else {
        o.get("files").and_then(Value::as_array).map(|f| f.iter().filter(|x| x.is_string()).cloned().collect()).unwrap_or_default()
    };
    Some(json!({"title": title, "detail": detail, "status": status, "severity": severity, "level": level(&severity),
                "line": o.get("line").cloned().unwrap_or(Value::Null), "fix": first_text(o, &["fix", "better_fix"]),
                "id": o.get("id").cloned().unwrap_or(Value::Null), "files": files}))
}

// A structured report's findings, its checker verdicts ({id, real, verdict, why, severity_should_be})
// and its summary. A field holding an object (a report nested under "result") is looked into once.
fn structured(report: &str) -> (Vec<Value>, Vec<Value>, String) {
    let Ok(Value::Object(top)) = serde_json::from_str::<Value>(report) else { return (Vec::new(), Vec::new(), String::new()) };
    let (mut findings, mut verdicts) = (Vec::new(), Vec::new());
    let mut summary = first_text(&top, &["summary", "verdict", "outcome", "status"]);
    let mut fields: Vec<(String, Value)> = top.clone().into_iter().collect();
    for (_, v) in top.iter().filter(|(k, v)| v.is_object() && field_status(k).is_none()) {
        let inner = v.as_object().unwrap();
        fields.extend(inner.clone());
        if summary.is_empty() {
            summary = first_text(inner, &["summary", "verdict"]);
        }
    }
    for (k, v) in fields {
        let (Some(status), Some(list)) = (field_status(&k), v.as_array()) else { continue };
        if status == "verdicts" {
            verdicts.extend(list.iter().filter(|x| x.is_object()).cloned());
            continue;
        }
        findings.extend(list.iter().filter_map(|x| item_finding(x, status)));
    }
    (findings, verdicts, summary)
}

// The prose of a structured report that lists nothing: a field named as prose (report, markdown, ...)
// of 400 characters or more. A checker's single verdict (real, why) is not prose.
fn prose_of(report: &str) -> Option<String> {
    let (found, verdicts, _) = structured(report);
    if !found.is_empty() || !verdicts.is_empty() {
        return None;
    }
    let Ok(Value::Object(o)) = serde_json::from_str::<Value>(report) else { return None };
    ["report", "markdown", "text", "body", "content", "document"].iter().filter_map(|k| o.get(*k).and_then(Value::as_str))
        .filter(|s| s.chars().count() >= 400).max_by_key(|s| s.len()).map(str::to_string)
}

// ── written reports, read by Sonnet on request ──────────────────────────────

fn dir() -> PathBuf {
    history::cache_dir().join("results")
}
fn clip(s: &str, n: usize) -> &str {
    s.char_indices().nth(n).map_or(s, |(i, _)| &s[..i])
}
fn read_prompt(s: &Source) -> String {
    format!("Agent: {}\nTask: {}\n\nReport:\n{}", s.name, clip(s.asked.trim(), 1500), clip(&s.report, 24000))
}
fn read_key(s: &Source) -> String {
    catalog::sha1_hex(format!("{READ_VERSION}\n{}", read_prompt(s)).as_bytes())
}
fn cached(key: &str) -> Option<Value> {
    serde_json::from_slice(&fs::read(dir().join(format!("{key}.json"))).ok()?).ok()
}
// What one reading costs, from five measured on 2026-10-07 ($0.044-0.067 for 10-24k characters).
fn cost_of(s: &Source) -> f64 {
    0.035 + clip(&s.report, 24000).chars().count() as f64 * 0.0000015
}
fn written(s: &Source) -> bool {
    !s.structured && s.state != "running" && !s.report.trim().is_empty()
}

struct Job {
    key: String,
    session: String,
    prompt: String,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    queued: HashMap<String, String>, // key -> session
    workers: usize,
    errors: HashMap<String, String>, // session -> why its last reading failed
}

static QUEUE: OnceLock<Mutex<Queue>> = OnceLock::new();

fn queue() -> MutexGuard<'static, Queue> {
    QUEUE.get_or_init(Default::default).lock().unwrap()
}

// read_reports: the session's written reports not read yet, queued.
pub fn read_reports(path: &Path, tool: &str, agents: &Value, session: &str, working: bool) -> usize {
    let mut n = 0;
    for s in sources(path, tool, agents, working).iter().filter(|s| written(s)) {
        let key = read_key(s);
        if cached(&key).is_some() {
            continue;
        }
        let mut q = queue();
        if q.queued.contains_key(&key) {
            continue;
        }
        q.queued.insert(key.clone(), session.to_string());
        q.jobs.push_back(Job { key, session: session.to_string(), prompt: read_prompt(s) });
        q.errors.remove(session);
        n += 1;
        if q.workers < WORKERS {
            q.workers += 1;
            thread::spawn(work);
        }
    }
    n
}

fn work() {
    loop {
        let job = {
            let mut q = queue();
            match q.jobs.pop_front() {
                Some(j) => j,
                None => {
                    q.workers -= 1;
                    return;
                }
            }
        };
        let done = call(READ_SYSTEM, READ_SCHEMA, &job.prompt).and_then(|(v, cost)| {
            let v = json!({"findings": v["findings"], "cost": cost, "session": job.session});
            let p = dir().join(format!("{}.json", job.key));
            let tmp = dir().join(format!(".{}.tmp", job.key));
            fs::create_dir_all(dir()).and_then(|_| fs::write(&tmp, v.to_string())).and_then(|_| fs::rename(&tmp, &p))
                .map_err(|e| format!("keep a reading: {e}"))
        });
        settle(&mut queue(), &job, done);
    }
}

// A reading ended: the session that asked for it is told if it failed, and only that session; a reading
// that worked clears the session's earlier failure.
fn settle(q: &mut Queue, job: &Job, done: Result<(), String>) {
    q.queued.remove(&job.key);
    match done {
        Ok(()) => {
            q.errors.remove(&job.session);
        }
        Err(e) => {
            eprintln!("[results] {}: {e}", job.key);
            q.errors.insert(job.session.clone(), e);
        }
    }
}

// `claude` on the default Claude account, its key on fd 3 as a session gets it. Run on Personal, a person whose
// default is an API-key account or one with a folder of its own got "Not logged in", or a reading billed to Personal.
pub(crate) fn reader() -> Result<Command, String> {
    let (mut cmd, key) = accounts::command_with("claude", accounts::default_account("claude").as_deref())?;
    if let Some(k) = key {
        keys::pass_fd3(&mut cmd, &k).map_err(|e| format!("pass the key: {e}"))?;
    }
    Ok(cmd)
}

// One `claude -p` call with a JSON schema: its structured output and what it cost.
fn call(system: &str, schema: &str, prompt: &str) -> Result<(Value, f64), String> {
    let _ = fs::create_dir_all(dir());
    let mut child = reader()?
        .args(["-p", "--model", MODEL, "--output-format", "json", "--no-session-persistence",
               "--settings", r#"{"disableAllHooks":true}"#, "--tools", "", "--strict-mcp-config",
               "--system-prompt", system, "--json-schema", schema])
        .current_dir(dir())
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|e| format!("start claude: {e}"))?;
    let mut stdin = child.stdin.take().unwrap();
    let text = prompt.to_string();
    thread::spawn(move || {
        let _ = stdin.write_all(text.as_bytes());
    });
    let mut stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout.read_to_string(&mut s);
        let _ = tx.send(s);
    });
    let out = match rx.recv_timeout(CALL_WAIT) {
        Ok(s) => s,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("Claude Code did not answer within {} s", CALL_WAIT.as_secs()));
        }
    };
    let _ = child.wait();
    let v: Value = serde_json::from_str(&out).map_err(|_| format!("Claude Code answered: {}", clip(out.trim(), 300)))?;
    if v["is_error"] == true || v["structured_output"].is_null() {
        return Err(v["result"].as_str().unwrap_or("Claude Code gave no structured answer").to_string());
    }
    Ok((v["structured_output"].clone(), v["total_cost_usd"].as_f64().unwrap_or(0.0)))
}

// ── results ─────────────────────────────────────────────────────────────────

// Letters and digits only, lower case, single spaces: a quote is found in its report even when the
// model dropped a backtick or a line break.
fn plain(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for w in s.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()) {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&w.to_lowercase());
    }
    out
}

fn need(status: &str) -> &'static str {
    if matches!(status, "problem" | "open") { "look" } else { "settled" }
}

pub fn results(path: &Path, tool: &str, agents: &Value, session: &str, working: bool) -> Value {
    let all = sources(path, tool, agents, working);
    let mut findings: Vec<Value> = Vec::new();
    let mut reports = Vec::new();
    let (mut unread, mut unread_cost) = (0, 0.0);
    let mut verdicts: HashMap<(String, String), Value> = HashMap::new(); // (workflow run, finding id)
    for s in all.iter().filter(|s| s.state != "running" && !s.report.trim().is_empty()) {
        let first = s.report.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("").to_string();
        let (found, kind, line) = if s.structured {
            let (found, vs, summary) = structured(&s.report);
            for v in vs {
                let id = v["id"].as_str().map(str::to_string).or(v["id"].as_u64().map(|n| n.to_string()));
                if let Some(id) = id {
                    verdicts.insert((s.run.clone(), id), v);
                }
            }
            (found, "structured", summary)
        } else {
            match cached(&read_key(s)) {
                Some(r) => {
                    let report = plain(&s.report);
                    let found = r["findings"].as_array().into_iter().flatten().filter(|f| {
                        let q = plain(f["quote"].as_str().unwrap_or(""));
                        q.split(' ').count() >= 3 && report.contains(&q) && STATUSES.contains(&f["status"].as_str().unwrap_or(""))
                    }).map(|f| json!({"title": f["title"], "detail": f["detail"], "status": f["status"], "quote": f["quote"],
                                      "files": f["files"].as_array().cloned().unwrap_or_default()})).collect();
                    (found, "read", first)
                }
                None => {
                    unread += 1;
                    unread_cost += cost_of(s);
                    (Vec::new(), "written", first)
                }
            }
        };
        reports.push(json!({"source": s.key, "name": s.name, "state": s.state, "kind": kind,
                            "line": clip(&line, 300), "findings": found.len()}));
        for mut f in found {
            f["sources"] = json!([{"source": s.key, "name": s.name}]);
            f["run"] = json!(s.run);
            findings.push(f);
        }
    }
    // a checker's verdict on a finding of its own workflow: a real problem stays one, now checked
    // ("checked": {real, verdict, why, severity}); one that is not real is not a bug
    for f in findings.iter_mut() {
        let id = f["id"].as_str().map(str::to_string).or(f["id"].as_u64().map(|n| n.to_string()));
        let v = id.and_then(|id| verdicts.get(&(f["run"].as_str().unwrap_or("").to_string(), id)));
        if let Some(v) = v {
            let real = v["real"].as_bool().unwrap_or(!matches!(v["verdict"].as_str(), Some("rejected" | "not_real" | "false")));
            if !real {
                f["status"] = json!("not_a_bug");
            }
            f["checked"] = json!({"real": real, "verdict": v["verdict"], "why": v["why"].as_str().or(v["reason"].as_str()),
                                  "severity": v["severity_should_be"]});
        }
        let st = f["status"].as_str().unwrap_or("open").to_string();
        f["need"] = json!(need(&st));
        if let Some(o) = f.as_object_mut() {
            o.remove("run");
        }
    }
    // a file written after the report that named it: the finding may be fixed since
    let ended: HashMap<&str, u64> = all.iter().map(|s| (s.key.as_str(), s.ended)).collect();
    let edited: Vec<&str> = all.iter().flat_map(|s| s.files.iter().filter_map(|f| f["path"].as_str())).collect();
    let cwd = PathBuf::from(history::session_cwd(path, tool));
    let mut mtimes: HashMap<String, u64> = HashMap::new();
    for f in findings.iter_mut().filter(|f| f["need"] == "look") {
        let Some(name) = f["files"][0].as_str() else { continue };
        let at = ended.get(f["sources"][0]["source"].as_str().unwrap_or("")).copied().unwrap_or(0);
        let m = *mtimes.entry(name.to_string()).or_insert_with(|| file_of(name, &cwd, &edited).map_or(0, |p| history::mtime_ms(&p)));
        if at > 0 && m > at + 60_000 {
            f["changedSince"] = json!(m);
        }
    }
    findings.sort_by_key(|f| (STATUSES.iter().position(|s| f["status"] == *s).unwrap_or(9),
                              ["high", "medium", "low"].iter().position(|l| f["level"] == *l).unwrap_or(3)));

    // failures by what stopped them: the first line of their last words, numbers and paths aside
    let mut failures: Vec<Value> = Vec::new();
    let mut by_cause: HashMap<String, usize> = HashMap::new();
    for s in all.iter().filter(|s| s.state == "failed") {
        let line = s.report.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("It ended without a report");
        let cause: String = line.split_whitespace().map(|w| if w.contains('/') { "…" } else { w })
            .collect::<Vec<_>>().join(" ").chars().map(|c| if c.is_ascii_digit() { '#' } else { c }).collect();
        let at = *by_cause.entry(cause).or_insert_with(|| {
            failures.push(json!({"cause": clip(line, 300), "sources": []}));
            failures.len() - 1
        });
        failures[at]["sources"].as_array_mut().unwrap().push(json!({"source": s.key, "name": s.name}));
    }

    // pictures, newest source first; changes net per file with who made them
    let mut pictures = Vec::new();
    for s in all.iter().rev() {
        for p in s.pictures.iter().rev() {
            if pictures.len() < 80 {
                pictures.push(json!({"path": p["path"], "source": s.key, "name": s.name, "step": p["step"], "what": p["what"]}));
            }
        }
    }
    let mut changes: Vec<Value> = Vec::new();
    let mut at: HashMap<String, usize> = HashMap::new();
    for s in &all {
        for f in &s.files {
            let Some(p) = f["path"].as_str() else { continue };
            let i = *at.entry(p.to_string()).or_insert_with(|| {
                changes.push(json!({"path": p, "add": 0, "del": 0, "by": []}));
                changes.len() - 1
            });
            let c = &mut changes[i];
            c["add"] = json!(c["add"].as_u64().unwrap_or(0) + f["add"].as_u64().unwrap_or(0));
            c["del"] = json!(c["del"].as_u64().unwrap_or(0) + f["del"].as_u64().unwrap_or(0));
            let by = c["by"].as_array_mut().unwrap();
            match by.iter_mut().find(|b| b["source"] == s.key.as_str()) {
                Some(b) => b["steps"].as_array_mut().unwrap().push(f["step"].clone()),
                None => by.push(json!({"source": s.key, "name": s.name, "steps": [f["step"]]})),
            }
        }
    }
    changes.sort_by_key(|c| std::cmp::Reverse(c["add"].as_u64().unwrap_or(0) + c["del"].as_u64().unwrap_or(0)));

    let count = |st: &str| all.iter().filter(|s| s.state == st).count();
    let (reading, error) = {
        let q = queue();
        (q.queued.values().filter(|s| *s == session).count(), q.errors.get(session).cloned().unwrap_or_default())
    };
    json!({"findings": findings, "reports": reports, "failures": failures, "pictures": pictures, "changes": changes,
           "counts": {"sources": all.len(), "running": count("running"), "done": count("done"), "failed": count("failed"),
                      "stopped": count("stopped"), "reports": reports.len()},
           "unread": {"count": unread, "cost": (unread_cost * 100.0).round() / 100.0},
           "reading": reading, "error": if error.is_empty() { Value::Null } else { json!(error) }})
}

// The file a finding names ("path", "path:12", "~/path", or a path relative to the session's folder
// or to a file the session's agents edited), if it is there.
fn file_of(name: &str, cwd: &Path, edited: &[&str]) -> Option<PathBuf> {
    let name = name.trim();
    let name = match name.rsplit_once(':') {
        Some((p, l)) if !l.is_empty() && l.chars().all(|c| c.is_ascii_digit() || c == '-') => p,
        _ => name,
    };
    let p = if let Some(rest) = name.strip_prefix("~/") {
        PathBuf::from(std::env::var_os("HOME")?).join(rest)
    } else if name.starts_with('/') {
        PathBuf::from(name)
    } else {
        let here = cwd.join(name);
        if here.is_file() {
            here
        } else {
            PathBuf::from(edited.iter().find(|e| e.ends_with(&format!("/{name}")))?)
        }
    };
    p.is_file().then_some(p)
}

// A question about the results, answered by Sonnet from the findings and the reports; the sources
// it names are kept only when they are this session's.
pub fn ask(path: &Path, tool: &str, agents: &Value, session: &str, working: bool, q: &str) -> Result<Value, String> {
    let all = sources(path, tool, agents, working);
    let r = results(path, tool, agents, session, working);
    let mut m = String::from("Findings:\n");
    for f in r["findings"].as_array().into_iter().flatten().take(300) {
        let ids: Vec<&str> = f["sources"].as_array().into_iter().flatten().filter_map(|s| s["source"].as_str()).collect();
        m += &format!("- [{}] {} ({})\n", f["status"].as_str().unwrap_or(""), f["title"].as_str().unwrap_or(""), ids.join(", "));
    }
    m += "\nReports, newest first:\n";
    for s in all.iter().rev().filter(|s| !s.report.trim().is_empty()) {
        if m.len() > 120_000 {
            break;
        }
        m += &format!("\n[{}] {} ({})\n{}\n", s.key, s.name, s.state, clip(s.report.trim(), 3000));
    }
    let (a, cost) = call(ASK_SYSTEM, ASK_SCHEMA, &format!("{m}\nQuestion: {q}"))?;
    let names: HashMap<&str, &str> = all.iter().map(|s| (s.key.as_str(), s.name.as_str())).collect();
    let mut seen = HashSet::new();
    let sources: Vec<Value> = a["sources"].as_array().into_iter().flatten().filter_map(|s| s.as_str())
        .map(|s| s.trim_matches(|c| c == '[' || c == ']'))
        .filter_map(|s| names.get(s).filter(|_| seen.insert(s.to_string())).map(|n| json!({"source": s, "name": n})))
        .collect();
    Ok(json!({"answer": a["answer"], "sources": sources, "cost": cost}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_are_found_past_punctuation() {
        let report = plain("The `Timer` in **Conversation.qml** now\nwaits 120 ms; nothing else changed.");
        assert!(report.contains(&plain("Timer in Conversation.qml now waits 120 ms")));
        assert!(!report.contains(&plain("Timer now waits 200 ms")));
    }

    #[test]
    fn structured_reports_are_read_by_field() {
        let r = json!({
            "summary": "Two bugs, one fixed.",
            "findings": [{"id": "F1", "title": "Weather never loads", "severity": "blocker", "file": "Weather.qml", "fix": "retry"},
                         {"id": "F2", "problem": "Laps are not saved: the list is rebuilt on every tick.", "severity": "P3", "real": false}],
            "open_issues": ["Not seen on the device"],
            "files_changed": ["a.qml"],
            "result": {"fixed": [{"id": "x", "what_changed": "Timer waits 120 ms", "files": ["b.qml"]}]}
        }).to_string();
        let (f, v, summary) = structured(&r);
        assert_eq!(summary, "Two bugs, one fixed.");
        assert!(v.is_empty());
        let t: Vec<(&str, &str)> = f.iter().map(|x| (x["title"].as_str().unwrap(), x["status"].as_str().unwrap())).collect();
        assert!(t.contains(&("Weather never loads", "problem")));
        assert!(t.contains(&("Laps are not saved", "not_a_bug")));
        assert!(t.contains(&("Not seen on the device", "open")));
        assert!(t.contains(&("Timer waits 120 ms", "fixed")));
        assert_eq!(f.len(), 4, "files_changed is not findings");
        let w = f.iter().find(|x| x["title"] == "Weather never loads").unwrap();
        assert_eq!((w["level"].as_str(), w["files"][0].as_str(), w["fix"].as_str()), (Some("high"), Some("Weather.qml"), Some("retry")));
    }

    #[test]
    fn files_are_found_by_how_reports_name_them() {
        let dir = std::env::temp_dir().join(format!("results-file-of-{}", std::process::id()));
        fs::create_dir_all(dir.join("a/b")).unwrap();
        fs::write(dir.join("a/b/F.qml"), "x").unwrap();
        let abs = dir.join("a/b/F.qml").to_string_lossy().to_string();
        assert_eq!(file_of(&format!("{abs}:324"), Path::new("/nowhere"), &[]), Some(dir.join("a/b/F.qml")));
        assert_eq!(file_of("a/b/F.qml", &dir, &[]), Some(dir.join("a/b/F.qml")));
        assert_eq!(file_of("b/F.qml", Path::new("/nowhere"), &[abs.as_str()]), Some(dir.join("a/b/F.qml")));
        assert_eq!(file_of("Gone.qml", &dir, &[abs.as_str()]), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn levels() {
        assert_eq!([level("P1"), level("major"), level("minor"), level("note"), level("")], ["high", "medium", "low", "low", ""]);
    }

    // A session without sub-agents: the turn that just ended is not "running" because its transcript is fresh;
    // only the host saying the session is working makes it so.
    #[test]
    fn a_fresh_turn_is_running_only_while_the_session_works() {
        let dir = std::env::temp_dir().join(format!("results-turns-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("turns-test.jsonl");
        let ts = "2026-10-25T00:00:00.000Z";
        let lines = [
            json!({"type": "user", "uuid": "u1", "timestamp": ts, "message": {"role": "user", "content": "check the timer"}}),
            json!({"type": "assistant", "uuid": "a1", "timestamp": ts, "message": {"role": "assistant", "content": [{"type": "text", "text": "The timer is fine."}]}}),
        ];
        fs::write(&file, lines.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n").unwrap();
        let idle = turns(&file, "claude", false);
        assert_eq!((idle.len(), idle[0].state.as_str()), (1, "done"));
        let r = results(&file, "claude", &json!({}), "turns-test", false);
        assert_eq!((r["counts"]["running"].clone(), r["reports"].as_array().unwrap().len()), (json!(0), 1));
        assert_eq!(turns(&file, "claude", true)[0].state, "running");
        let _ = fs::remove_dir_all(&dir);
    }

    // An opencode session (a key into opencode's database, not a file) shows its new turns: its cache was kept by the
    // key's length, always 0, so the first reading stayed until the host restarted.
    #[test]
    fn an_opencode_sessions_new_turns_are_read() {
        let _db = crate::tests::opencode_db_lock();
        let home = crate::tests::scratch_home();
        let db = home.join(".local/share/opencode/opencode.db");
        fs::create_dir_all(db.parent().unwrap()).unwrap();
        let sql = |q: &str| assert!(std::process::Command::new("sqlite3").arg(&db).arg(q).status().unwrap().success(), "{q}");
        sql("create table if not exists message (id text primary key, session_id text, time_created integer, data text); \
             create table if not exists part (id text primary key, message_id text, session_id text, time_created integer, data text);");
        let turn = |n: u64, asked: &str, said: &str| {
            let (u, a) = (format!("msg_r{}", 2 * n), format!("msg_r{}", 2 * n + 1));
            let t = 1_790_000_000_000 + n * 1000;
            sql(&format!("insert into message values ('{u}', 'ses_results', {t}, '{{\"role\":\"user\"}}'); \
                          insert into part values ('prt_{u}', '{u}', 'ses_results', {t}, '{{\"type\":\"text\",\"text\":\"{asked}\"}}'); \
                          insert into message values ('{a}', 'ses_results', {}, '{{\"role\":\"assistant\",\"time\":{{\"completed\":{}}}}}'); \
                          insert into part values ('prt_{a}', '{a}', 'ses_results', {}, '{{\"type\":\"text\",\"text\":\"{said}\"}}');", t + 1, t + 2, t + 1));
        };
        let key = PathBuf::from(format!("{}#ses_results", db.display()));
        turn(1, "check the timer", "The timer is fine.");
        assert_eq!(turns(&key, "opencode", false).len(), 1);
        // a write in a later millisecond, as any later turn is
        thread::sleep(Duration::from_millis(20));
        turn(2, "and the clock?", "The clock is fine too.");
        let now: Vec<String> = turns(&key, "opencode", false).into_iter().map(|s| s.report).collect();
        assert_eq!(now, ["The timer is fine.", "The clock is fine too."]);
    }

    // A failed reading is shown to the session that asked for it, not to every session.
    #[test]
    fn a_reading_error_belongs_to_its_session() {
        let job = |session: &str| Job { key: format!("k-{session}"), session: session.into(), prompt: String::new() };
        let mut q = Queue::default();
        q.queued.insert("k-a".into(), "a".into());
        settle(&mut q, &job("a"), Err("Claude Code did not answer".into()));
        assert_eq!(q.errors.get("a").map(String::as_str), Some("Claude Code did not answer"));
        assert!(q.errors.get("b").is_none());
        assert!(q.queued.is_empty());
        settle(&mut q, &job("a"), Ok(()));
        assert!(q.errors.is_empty(), "a reading that worked clears the failure");
    }
}
