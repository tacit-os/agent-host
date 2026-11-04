# End-to-end check of side_question, rewind_files and rewind against a running host:
#   python3 tests/rewind_e2e.py SOCKET WORKDIR SESSION-UUID
import socket, json, threading, queue, time, sys, os
SOCK, CWD, SID = sys.argv[1], sys.argv[2], sys.argv[3]

class C:
    def __init__(s):
        s.k = socket.socket(socket.AF_UNIX); s.k.connect(SOCK); s.q = queue.Queue(); s.f = s.k.makefile()
        threading.Thread(target=s.rd, daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put(json.loads(l))
    def send(s, o): s.k.sendall((json.dumps(o) + "\n").encode())
    def until(s, pred, t=180):
        end = time.time() + t
        while time.time() < end:
            try: m = s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): return m
        raise SystemExit("TIMEOUT")
    def ask(s, o, t=120):
        o["tag"] = tag = f"t{time.time()}"
        s.send(o)
        return s.until(lambda m: m.get("tag") == tag, t)

a = C(); a.until(lambda m: m.get("ev") == "hello")
def turn(text, start=False):
    if start:
        r = a.ask({"op": "start", "cwd": CWD, "id": SID, "mode": "bypassPermissions", "model": "sonnet", "text": text})
        a.send({"op": "watch", "id": SID, "from": 0})
    else:
        r = a.ask({"op": "send", "id": SID, "text": text})
    a.until(lambda m: m.get("ev") == "event" and m["data"].get("type") == "result")
    return r
def users():
    t = a.ask({"op": "transcript", "id": SID})
    return [(i.get("rec"), i.get("text", "")[:40]) for i in t["items"] if i.get("role") == "user" and i.get("kind") == "text"]
def said():
    t = a.ask({"op": "transcript", "id": SID})
    return [i.get("text", "")[:40] for i in t["items"] if i.get("kind") == "text"]

turn("Remember the word apple. Reply with just: ok", start=True)
turn("Forget that: the word is now banana. Reply with just: ok")
u = users()
print("user messages with ids:", u)
sq = a.ask({"op": "side_question", "id": SID, "question": "Which word did I ask you to remember first? One word.", "history": []})
print("side_question:", sq.get("ev"), json.dumps(sq.get("response"))[:120])
turn("Use the Write tool to create notes.txt containing the single line: hello. Then reply with just: written")
u = users()
dry = a.ask({"op": "rewind_files", "id": SID, "message": u[-1][0], "dry": True})
print("rewind_files dry run from the write message:", dry.get("ev"), json.dumps(dry.get("response"))[:200])
real = a.ask({"op": "rewind_files", "id": SID, "message": u[-1][0]})
print("rewind_files for real:", real.get("ev"), json.dumps(real.get("response"))[:120], "| notes.txt still there:", os.path.exists(os.path.join(CWD, "notes.txt")))
rw = a.ask({"op": "rewind", "id": SID, "message": u[1][0], "lastSeen": u[-1][0]})
print("rewind to the banana message:", rw.get("ev"), json.dumps(rw.get("response"))[:200])
print("conversation now:", said())
turn("What is the word now? One word.")
print("after one more message:", said())
bad = a.ask({"op": "rewind", "id": SID, "message": "00000000-0000-0000-0000-000000000000"})
# an aside while a turn is running
a.send({"op": "send", "id": SID, "text": "Use Bash to run: sleep 20; echo done. Then reply with just: finished"})
time.sleep(6)
t0 = time.time()
mid = a.ask({"op": "side_question", "id": SID, "question": "What word is the conversation about? One word.", "history": []})
print(f"aside mid-turn after {time.time()-t0:.1f}s:", mid.get("ev"), json.dumps(mid.get("response"))[:100])
a.until(lambda m: m.get("ev") == "event" and m["data"].get("type") == "result")
print("the turn still finished:", said()[-1])
# rewinding a session the host has closed: opened again first, nothing sent
a.send({"op": "close", "id": SID})
time.sleep(4)
u = users()
r = a.ask({"op": "start", "cwd": CWD, "resume": SID, "mode": "bypassPermissions"})
rw2 = a.ask({"op": "rewind", "id": SID, "message": u[-1][0], "lastSeen": u[-1][0]})
print("rewind after reopening:", r.get("ev"), rw2.get("ev"), json.dumps(rw2.get("response"))[:120])
print("rewind to an unknown message:", bad.get("ev"), bad.get("message") or json.dumps(bad.get("response"))[:120])
a.send({"op": "close", "id": SID})
