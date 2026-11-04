# Codex through the host: python3 tests/codex_e2e.py SOCKET WORKDIR
# Start the host with AGENT_HOST_IDLE=8. WORKDIR: a fresh scratch folder. Tiny turns (Codex's free
# plan): allow, deny, an attached file, interrupt, then a resume after the idle close; then rename,
# fork and delete (archive) on the threads this test made, and a delete in the middle of a running
# command, which must stop it. Prints the thread ids it made.
# Codex resumes and forks threads through ~/.codex/thread_history_1.sqlite; where that database
# cannot be opened, those two steps print BLOCKED with Codex's error instead of failing.
import socket,json,threading,queue,time,os,sys,subprocess,tempfile
SOCK,CWD=sys.argv[1],sys.argv[2]
class C:
    def __init__(s):
        s.k=socket.socket(socket.AF_UNIX); s.k.connect(SOCK); s.q=queue.Queue(); s.f=s.k.makefile(); s.tag=0
        threading.Thread(target=s.rd,daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put(json.loads(l))
    def send(s,o): s.k.sendall((json.dumps(o)+"\n").encode())
    def until(s,pred,t=180):
        end=time.time()+t
        while time.time()<end:
            try: m=s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): return m
        raise SystemExit("TIMEOUT")
    def ask(s,o,t=180):
        s.tag+=1; tag=s.tag; o["tag"]=tag; s.send(o); return s.until(lambda m:m.get("tag")==tag,t)
fails=[]; made=[]; blocked=[]
def db_error(r): return r.get("ev")=="error" and "thread history" in r.get("message","")
def check(ok,what):
    print("ok  " if ok else "FAIL",what)
    if not ok: fails.append(what)
t0=time.time(); T=lambda: f"{time.time()-t0:5.1f}s"
def waiting(c,sid): return c.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["state"]=="waiting")["session"]["pending"][0]
def result(c,sid,t=180): return c.until(lambda m:m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="result",t)["data"]
def servers(): return subprocess.run(["pgrep","-f","codex app-server -c notify="],capture_output=True,text=True).stdout.split()
# Questions on `sid` are allowed until cond() holds or the result comes; returns the result, if any.
def allow_until(c,sid,cond=lambda:False,t=180):
    seen=set(); end=time.time()+t
    while time.time()<end and not cond():
        try: m=c.q.get(timeout=0.5)
        except queue.Empty: continue
        if m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="result": return m["data"]
        if m.get("ev")=="state" and m["session"]["id"]==sid:
            for p in m["session"]["pending"]:
                if p["request"] not in seen:
                    seen.add(p["request"]); c.send({"op":"answer","id":sid,"request":p["request"],"allow":True})
def rollout(sid):
    r=subprocess.run(["bash","-c",f"ls ~/.codex/sessions/*/*/*/rollout-*-{sid}.jsonl 2>/dev/null"],capture_output=True,text=True).stdout.split()
    return r[0] if r else ""

a=C(); a.until(lambda m:m.get("ev")=="hello"); a.ask({"op":"hello","proto":5})
before=set(servers())
r=a.ask({"op":"start","cwd":CWD,"agent":"codex","mode":"manual","text":"Run the shell command `touch ok.txt`, then reply with the word ok."})
check(r.get("ev")=="started" and r.get("tool")=="codex",f"{T()} start: {r}")
sid=r["id"]; made.append(sid)
a.send({"op":"watch","id":sid,"from":0})
p=waiting(a,sid)
check(p["tool"]=="Bash" and "touch ok.txt" in p["input"].get("command",""),f"{T()} pending: {p['tool']} {json.dumps(p['input'])[:80]}")
# does Codex write the rollout during a turn? (the app follows a thread by reading it)
path=rollout(sid); size_mid=os.path.getsize(path) if path else 0
mid=a.ask({"op":"transcript","id":sid,"limit":50})
a.ask({"op":"answer","id":sid,"request":p["request"],"allow":True})
res=result(a,sid)
check("ok" in res.get("result","").lower() and os.path.exists(os.path.join(CWD,"ok.txt")),f"{T()} allow: {res.get('result')!r}, ok.txt there: {os.path.exists(os.path.join(CWD,'ok.txt'))}")
size_end=os.path.getsize(path) if path else 0
print(f"     rollout during the turn: {size_mid} bytes, {len(mid.get('items',[]))} items read; after it: {size_end} bytes")
tr=a.ask({"op":"transcript","id":sid,"limit":50})
check(any(i.get("tool")=="Bash" and "touch ok.txt" in json.dumps(i) for i in tr.get("items",[])),f"{T()} transcript has the Bash call ({len(tr.get('items',[]))} items)")

a.ask({"op":"send","id":sid,"text":"Run the shell command `rm ok.txt`, then reply with the word done."})
p=waiting(a,sid)
a.ask({"op":"answer","id":sid,"request":p["request"],"allow":False})
res=result(a,sid)
check(os.path.exists(os.path.join(CWD,"ok.txt")),f"{T()} deny: {res.get('result')!r}, ok.txt still there")

# outside the workdir, under a name that cannot be guessed: only the attachment leads to it
note=os.path.join(tempfile.mkdtemp(prefix="cx-",dir=os.path.dirname(CWD)),"note.txt"); open(note,"w").write("zebra-4417\n")
a.ask({"op":"send","id":sid,"text":"Reply with the exact text inside the attached file, nothing else.","files":[note]})
res=allow_until(a,sid) or {}
check("zebra-4417" in res.get("result",""),f"{T()} attached file: {res.get('result')!r}")

a.ask({"op":"send","id":sid,"text":"Count from 1 to 400, one number per line, no tools."})
a.until(lambda m:m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="stream_event" and m["data"]["event"].get("type")=="content_block_delta",120)
ti=time.time(); a.ask({"op":"interrupt","id":sid})
res=result(a,sid,60)
check(res.get("subtype")=="error_during_execution",f"{T()} interrupt: result {time.time()-ti:.1f}s after, subtype={res.get('subtype')}")

m=a.until(lambda m:m.get("ev")=="exit" and m["id"]==sid,90)
check(m.get("reason")=="idle",f"{T()} idle close: {m.get('reason')}")
time.sleep(32)   # the reaper closes the server on a later pass
check(not (set(servers())-before),f"{T()} app-server closed after the idle close: {sorted(set(servers())-before)}")

r=a.ask({"op":"send","id":sid,"text":"What file did you create with touch at the start? Reply with just the file name, no tools."})
if db_error(r):
    blocked.append("resume"); print(f"BLOCKED {T()} resume: {r['message']}")
else:
    res=result(a,sid)
    check("ok.txt" in res.get("result",""),f"{T()} after resume: {res.get('result')!r}")

rn=a.ask({"op":"rename","id":sid,"title":"codex e2e"})
check(rn.get("ev")=="renamed",f"{T()} rename: {rn}")
f=a.ask({"op":"fork","id":sid}); fid=None
if db_error(f):
    blocked.append("fork"); print(f"BLOCKED {T()} fork: {f['message']}")
else:
    check(f.get("ev")=="forked" and f.get("tool")=="codex" and f.get("from")==sid and f.get("id")!=sid,f"{T()} fork: {f}")
    fid=f.get("id"); made.append(fid)
    check(bool(rollout(fid)),f"{T()} the fork has its rollout")
rows={r["id"]:r for r in a.ask({"op":"history"})["sessions"]}
check(rows.get(sid,{}).get("title")=="codex e2e",f"{T()} history title: {rows.get(sid,{}).get('title')!r}")
for x in [x for x in (fid,sid) if x]:
    d=a.ask({"op":"delete","id":x},t=60)
    check(d.get("ev")=="deleted" and d.get("how")=="archive" and not rollout(x),f"{T()} delete {x[:8]}: {d}")
left={r["id"] for r in a.ask({"op":"history"})["sessions"]}
check(sid not in left and fid not in left,f"{T()} archived threads left the history")
r=a.ask({"op":"start","cwd":CWD,"agent":"codex","mode":"manual","text":"Run the shell command `touch started.txt && sleep 20 && touch late.txt`, then reply with the word ok."})
tid=r.get("id"); made.append(tid)
a.send({"op":"watch","id":tid,"from":0})
allow_until(a,tid,lambda:os.path.exists(os.path.join(CWD,"started.txt")),120)
running=os.path.exists(os.path.join(CWD,"started.txt"))
d=a.ask({"op":"delete","id":tid},t=60)
time.sleep(25)
check(running and d.get("ev")=="deleted" and not os.path.exists(os.path.join(CWD,"late.txt")),
      f"{T()} delete mid-command: running {running}, {d}, late.txt there: {os.path.exists(os.path.join(CWD,'late.txt'))}")
print(f"{T()} threads made: {made}")
print(("PASS" if not fails else f"{len(fails)} FAILED")+(f"; blocked: {blocked}" if blocked else ""))
sys.exit(1 if fails else 0)
