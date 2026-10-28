//! API keys (plan §2.14, §4): one Secret Service item per key, labelled "Tacit: …" with the
//! attributes application=tacit-agents kind=api-key provider=<p> key=<id>. Values go to
//! `secret-tool store` on stdin and come back from `secret-tool lookup` on stdout; never in argv,
//! never in a file. `secret-tool search` is never run: it prints secrets. Every lookup first reads
//! the login collection's Locked property with busctl, so no password window ever opens.
//! A key is checked with `curl --config -`, its header on stdin. Every value looked up or stored is
//! remembered for the redactor, which masks those values and key-shaped text in whatever the host
//! passes on or logs.

use std::borrow::Cow;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::{io, thread};

const ATTRS: [&str; 4] = ["application", "tacit-agents", "kind", "api-key"];

static KNOWN: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// The login keyring: "open", "locked", or "absent" when there is no Secret Service to ask.
pub fn keyring() -> &'static str {
    let out = run("busctl", &["--user", "get-property", "org.freedesktop.secrets", "/org/freedesktop/secrets/collection/login",
                             "org.freedesktop.Secret.Collection", "Locked"], None, Duration::from_secs(5));
    match out {
        Some((true, o)) if o.trim() == "b false" => "open",
        Some((true, o)) if o.trim() == "b true" => "locked",
        _ => "absent",
    }
}

fn unlocked() -> Result<(), String> {
    match keyring() {
        "open" => Ok(()),
        "locked" => Err("The keyring is locked".into()),
        _ => Err("There is no keyring to keep keys in".into()),
    }
}

/// Printable ASCII with no space, quote or backslash: what a provider's key looks like, and what
/// curl's config syntax carries unescaped.
pub fn well_formed(secret: &str) -> bool {
    secret.len() >= 8 && secret.len() <= 512 && secret.chars().all(|c| c.is_ascii_graphic() && c != '"' && c != '\\')
}

pub fn store(id: &str, provider: &str, label: &str, secret: &str) -> Result<(), String> {
    unlocked()?;
    remember(secret);
    let mut args = vec!["store", "--label", label];
    args.extend(ATTRS);
    args.extend(["provider", provider, "key", id]);
    match run("secret-tool", &args, Some(secret), Duration::from_secs(20)) {
        Some((true, _)) => Ok(()),
        _ => Err("The keyring did not take the key".into()),
    }
}

/// The key's value. Err: the keyring is locked or absent, or it holds no such key.
pub fn lookup(id: &str) -> Result<String, String> {
    unlocked()?;
    let mut args = vec!["lookup"];
    args.extend(ATTRS);
    args.extend(["key", id]);
    match run("secret-tool", &args, None, Duration::from_secs(10)) {
        Some((true, v)) if !v.is_empty() => {
            let v = v.trim_end_matches(['\n', '\r']).to_string();
            remember(&v);
            Ok(v)
        }
        _ => Err("The keyring holds no such key".into()),
    }
}

pub fn clear(id: &str) -> Result<(), String> {
    unlocked()?;
    let mut args = vec!["clear"];
    args.extend(ATTRS);
    args.extend(["key", id]);
    run("secret-tool", &args, None, Duration::from_secs(10)).map(|_| ()).ok_or("The keyring did not answer".into())
}

/// What the provider says of the key: "ok", "refused" or "offline" (no answer, or one that says
/// nothing about the key).
pub fn check(provider: &str, secret: &str) -> &'static str {
    if !well_formed(secret) {
        return "refused";
    }
    remember(secret);
    let (url, headers): (&str, Vec<String>) = match provider {
        "anthropic" => ("https://api.anthropic.com/v1/models", vec![format!("x-api-key: {secret}"), "anthropic-version: 2023-06-01".into()]),
        "openai" => ("https://api.openai.com/v1/models", vec![format!("Authorization: Bearer {secret}")]),
        // /models answers anyone; /key answers only a valid key
        "openrouter" => ("https://openrouter.ai/api/v1/key", vec![format!("Authorization: Bearer {secret}")]),
        "google" => ("https://generativelanguage.googleapis.com/v1beta/models", vec![format!("x-goog-api-key: {secret}")]),
        _ => return "offline",
    };
    let mut cfg = format!("url = \"{url}\"\nsilent\nshow-error\noutput = \"/dev/null\"\nwrite-out = \"%{{http_code}}\"\nmax-time = 15\n");
    for h in headers {
        cfg += &format!("header = \"{h}\"\n");
    }
    let code = run("curl", &["--config", "-"], Some(&cfg), Duration::from_secs(20)).map(|(_, o)| o.trim().to_string()).unwrap_or_default();
    match code.as_str() {
        c if c.starts_with('2') && c.len() == 3 => "ok",
        "401" | "403" => "refused",
        // Google answers a bad key with 400 API_KEY_INVALID
        "400" if provider == "google" => "refused",
        _ => "offline",
    }
}

/// The child gets `secret` on fd 3: the read end of a pipe holding it, dup'd there just before
/// exec. Nothing of it is in the child's env or argv.
pub fn pass_fd3(cmd: &mut Command, secret: &str) -> io::Result<()> {
    extern "C" {
        fn dup(fd: i32) -> i32;
        fn dup2(old: i32, new: i32) -> i32;
        fn close(fd: i32) -> i32;
    }
    remember(secret);
    let (reader, mut writer) = io::pipe()?;
    writer.write_all(secret.as_bytes())?;
    drop(writer);
    // dup first: the pipe's own fds are close-on-exec, and dup2 onto the same number would keep that
    unsafe {
        cmd.pre_exec(move || {
            let t = dup(reader.as_raw_fd());
            if t < 0 {
                return Err(io::Error::last_os_error());
            }
            if t != 3 {
                if dup2(t, 3) < 0 {
                    return Err(io::Error::last_os_error());
                }
                close(t);
            }
            Ok(())
        });
    }
    Ok(())
}

pub fn remember(secret: &str) {
    if secret.len() >= 8 {
        let mut k = KNOWN.lock().unwrap();
        if !k.iter().any(|s| s == secret) {
            k.push(secret.to_string());
        }
    }
}

pub fn last4(secret: &str) -> String {
    secret.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect()
}

fn mask(secret: &str) -> String {
    format!("••••{}", last4(secret))
}

fn token_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

/// `s` with every key value the host has handed out, and anything shaped like a key (sk-ant-,
/// sk-or-v1-, sk-proj-, sk- and 20 more, AIza and 35 more, a bearer token), as "••••" and its last 4.
pub fn redact(s: &str) -> Cow<'_, str> {
    let known: Vec<String> = KNOWN.lock().unwrap().iter().filter(|k| s.contains(k.as_str())).cloned().collect();
    if known.is_empty() && !s.contains("sk-") && !s.contains("AIza") && !s.contains("earer ") {
        return Cow::Borrowed(s);
    }
    let mut out = s.to_string();
    for k in known {
        out = out.replace(&k, &mask(&k));
    }
    let b = out.as_bytes();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let rest = &b[i..];
        let start_ok = i == 0 || !token_char(b[i - 1]);
        let (skip, min) = if !start_ok {
            (0, 0)
        } else if rest.starts_with(b"sk-") {
            (0, 23)
        } else if rest.starts_with(b"AIza") {
            (0, 39)
        } else if rest.starts_with(b"Bearer ") || rest.starts_with(b"bearer ") {
            (7, 7 + 16)
        } else {
            (0, 0)
        };
        if min > 0 {
            let mut j = i + skip;
            while j < b.len() && (token_char(b[j]) || b[j] == b'.') {
                j += 1;
            }
            if j - i >= min {
                spans.push((i + skip, j));
                i = j;
                continue;
            }
        }
        i += 1;
    }
    if spans.is_empty() {
        return Cow::Owned(out);
    }
    let mut r = String::with_capacity(out.len());
    let mut at = 0;
    for (a, z) in spans {
        r.push_str(&out[at..a]);
        r.push_str(&mask(&out[a..z]));
        at = z;
    }
    r.push_str(&out[at..]);
    Cow::Owned(r)
}

/// Every string in `v` redacted in place; true when something was masked.
pub fn redact_value(v: &mut serde_json::Value) -> bool {
    use serde_json::Value;
    match v {
        Value::String(s) => match redact(s) {
            Cow::Owned(r) if r != *s => {
                *s = r;
                true
            }
            _ => false,
        },
        Value::Array(a) => a.iter_mut().fold(false, |m, x| redact_value(x) | m),
        Value::Object(o) => o.values_mut().fold(false, |m, x| redact_value(x) | m),
        _ => false,
    }
}

// Success and stdout of a command given `input` on stdin, or None when it could not run or took
// longer than `wait` (then it is killed). Its stderr is dropped: it may echo what it was given.
fn run(program: &str, args: &[&str], input: Option<&str>, wait: Duration) -> Option<(bool, String)> {
    let mut child = Command::new(program).args(args)
        .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped()).stderr(Stdio::null()).spawn().ok()?;
    if let (Some(text), Some(mut w)) = (input, child.stdin.take()) {
        let text = text.to_string();
        thread::spawn(move || {
            let _ = w.write_all(text.as_bytes());
        });
    }
    let mut out = child.stdout.take()?;
    let reader = thread::spawn(move || {
        let mut s = String::new();
        let _ = out.read_to_string(&mut s);
        s
    });
    let until = Instant::now() + wait;
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) if Instant::now() < until => thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    Some((status.success(), reader.join().unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_key_shapes_and_known_values() {
        let s = "key sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAA1234 and AIzaSyAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9876";
        let r = redact(s);
        assert!(!r.contains("AAAAAAAAAAAAAAAAAAAA"), "{r}");
        assert!(r.contains("••••1234") && r.contains("••••9876"), "{r}");
        assert_eq!(redact("Authorization: Bearer abcdefghijklmnopqrstuvwxyz5678"), "Authorization: Bearer ••••5678");
        assert_eq!(redact("task-sk-1 and sk-short"), "task-sk-1 and sk-short");
        remember("plain-value-0001x");
        assert_eq!(redact("it said plain-value-0001x."), "it said ••••001x.");
        let mut v = serde_json::json!({"a": ["x plain-value-0001x"], "b": 1});
        assert!(redact_value(&mut v));
        assert_eq!(v["a"][0], "x ••••001x");
    }

    #[test]
    fn well_formed_keys() {
        assert!(well_formed("sk-or-v1-0123456789abcdef"));
        assert!(!well_formed("has space 0123456789"));
        assert!(!well_formed("quote\"0123456789"));
        assert!(!well_formed("short"));
    }
}
