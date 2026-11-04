# Which control requests a headless Claude Code session (the way agent-host runs one) answers:
#   python3 tests/control_probe.py WORKDIR SESSION-UUID
# Two short turns, then side_question, rewind_files (dry run), fork_conversation and
# rewind_conversation, each printed with its reply; after the rewind, one turn shows what it kept.
import json, subprocess, sys, threading, queue, time

CWD, SID = sys.argv[1], sys.argv[2]
p = subprocess.Popen(["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
                      "--replay-user-messages", "--model", "sonnet", "--permission-mode", "bypassPermissions",
                      "--session-id", SID], cwd=CWD, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
q = queue.Queue()
threading.Thread(target=lambda: [q.put(json.loads(l)) for l in p.stdout if l.strip()], daemon=True).start()
n = 0

def send(o):
    p.stdin.write(json.dumps(o) + "\n"); p.stdin.flush()

def until(pred, t=90):
    end = time.time() + t
    while time.time() < end:
        try: m = q.get(timeout=1)
        except queue.Empty: continue
        if pred(m): return m
    return None

def control(req):
    global n
    n += 1
    rid = f"probe-{n}"
    send({"type": "control_request", "request_id": rid, "request": req})
    m = until(lambda m: m.get("type") == "control_response" and m["response"].get("request_id") == rid, 120)
    return m["response"] if m else "NO REPLY"

def turn(text):
    send({"type": "user", "message": {"role": "user", "content": text}, "parent_tool_use_id": None, "session_id": SID})
    uuid = None
    while True:
        m = until(lambda m: True, 120)
        if m is None: return uuid, "TIMEOUT"
        if m.get("type") == "user" and uuid is None and m.get("uuid"): uuid = m["uuid"]
        if m.get("type") == "result": return uuid, m.get("result")

print("initialize:", json.dumps(control({"subtype": "initialize"}))[:160])
u1, r1 = turn("Remember the word apple. Reply with just: ok")
print("turn 1:", u1, r1)
u2, r2 = turn("Forget that: the word is now banana. Reply with just: ok")
print("turn 2:", u2, r2)
print("side_question:", json.dumps(control({"subtype": "side_question", "question": "Which word did I ask you to remember first? One word.", "history": []}))[:300])
print("rewind_files dry run:", json.dumps(control({"subtype": "rewind_files", "user_message_id": u1, "dry_run": True}))[:300])
print("fork_conversation:", json.dumps(control({"subtype": "fork_conversation", "target_message_uuid": u2, "title": "probe fork"}))[:300])
print("rewind_conversation:", json.dumps(control({"subtype": "rewind_conversation", "target_message_uuid": u2}))[:300])
u3, r3 = turn("What is the word now? One word.")
print("after rewind, the word:", r3)
p.stdin.close()
p.wait(timeout=30)
