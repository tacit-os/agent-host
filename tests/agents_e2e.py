# End-to-end check of "agents" and "stop_task" against a running host:
#   python3 tests/agents_e2e.py SOCKET WORKDIR SESSION-UUID
# A real session starts one background sub-agent that sleeps; the host must list it as running
# with its newest step, keep its messages out of the parent's live items, and stop it on stop_task.
import socket, json, threading, queue, time, sys
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
    def ask(s, o, t=60):
        o["tag"] = tag = int(time.time() * 1000) % 1000000
        s.send(o)
        return s.until(lambda m: m.get("tag") == tag, t)

t0 = time.time(); T = lambda: f"{time.time() - t0:5.1f}s"
a = C(); a.until(lambda m: m.get("ev") == "hello")
prompt = ("Use the Agent tool exactly once with run_in_background set to true, subagent_type general-purpose, "
          "description 'Sleep probe', and prompt: 'Use the Bash tool in the foreground (not in the background) to run exactly: "
          "python3 -c \"import time; time.sleep(45)\" ; then reply with the word done.' "
          "After launching it, reply with just: launched")
print(T(), "start:", a.ask({"op": "start", "cwd": CWD, "id": SID, "mode": "bypassPermissions", "model": "sonnet", "text": prompt}).get("ev"))
a.send({"op": "watch", "id": SID, "from": 0})

# the sub-agent appears and runs its sleep
agent = None
end = time.time() + 150
while time.time() < end:
    r = a.ask({"op": "agents", "id": SID})
    running = [x for x in r.get("agents", []) if x["state"] == "running" and (x.get("last") or {}).get("tool") == "Bash"]
    if running:
        agent = running[0]
        break
    time.sleep(2)
if not agent:
    print(T(), "FAIL: no running sub-agent with a Bash step; agents =", json.dumps(r.get("agents"))[:400])
    sys.exit(1)
print(T(), "running:", agent["description"], "| step:", agent["last"], "| background:", agent["background"], "| task:", agent["task"])

# its messages stay out of the parent's live items
live = a.ask({"op": "watch", "id": SID, "from": 10**9})
sleeps = [i for i in live.get("items", []) if i.get("kind") == "tool" and "time.sleep(45)" in (i.get("summary") or "")]
print(T(), "parent live items:", len(live.get("items", [])), "| sub-agent's sleep in them:", len(sleeps))

# Stop: the task stops, the agent reads as stopped
print(T(), "stop_task:", a.ask({"op": "stop_task", "id": SID, "task": agent["task"]}).get("ev"))
state = None
end = time.time() + 40
while time.time() < end:
    r = a.ask({"op": "agents", "id": SID})
    x = next((y for y in r["agents"] if y["agent"] == agent["agent"]), None)
    state = x and x["state"]
    if state and state != "running":
        break
    time.sleep(2)
print(T(), "after stop:", state)
ok = state == "stopped" and not sleeps
print(T(), "PASS" if ok else "FAIL")
a.send({"op": "close", "id": SID})
sys.exit(0 if ok else 1)
