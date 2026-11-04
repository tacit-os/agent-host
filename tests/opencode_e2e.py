# opencode through the host: python3 tests/opencode_e2e.py SOCKET WORKDIR [MODEL]
# Start the host with AGENT_HOST_IDLE=8. WORKDIR: a fresh scratch folder. MODEL defaults to
# opencode/big-pickle. Tiny turns: allow, deny, a write in acceptEdits (unasked) and back in manual
# (asked), interrupt, then a resume after the idle close; then rename, fork (a command in the fork
# must ask) and delete (archive) on the sessions this test made. Prints their ids.
import socket,json,threading,queue,time,os,sys,subprocess
SOCK,CWD=sys.argv[1],sys.argv[2]
MODEL=sys.argv[3] if len(sys.argv)>3 else "opencode/big-pickle"
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
fails=[]; made=[]
def check(ok,what):
    print("ok  " if ok else "FAIL",what)
    if not ok: fails.append(what)
t0=time.time(); T=lambda: f"{time.time()-t0:5.1f}s"
def waiting(c,sid): return c.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["state"]=="waiting")["session"]["pending"][0]
def result(c,sid,t=180): return c.until(lambda m:m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="result",t)["data"]
def servers(): return [l for l in subprocess.run(["pgrep","-af","opencode serve --hostname 127.0.0.1"],capture_output=True,text=True).stdout.splitlines() if "pgrep" not in l]
ok_txt=os.path.join(CWD,"ok.txt")
# A turn to its result; questions are answered with `allow`. Returns the result and the tools asked about.
def turn(c,sid,text,allow=False):
    c.ask({"op":"send","id":sid,"text":text}); asked=[]; seen=set()
    while True:
        m=c.until(lambda m:(m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["state"]=="waiting") or
                  (m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="result"))
        if m.get("ev")=="event": return m["data"],asked
        for p in m["session"]["pending"]:
            if p["request"] in seen: continue
            seen.add(p["request"]); asked.append(p["tool"])
            c.ask({"op":"answer","id":sid,"request":p["request"],"allow":allow})

a=C(); a.until(lambda m:m.get("ev")=="hello"); a.ask({"op":"hello","proto":5})
before=set(servers())
r=a.ask({"op":"start","cwd":CWD,"agent":"opencode","mode":"manual","model":MODEL,"text":"Run the shell command `touch ok.txt`, then reply with the word ok."},t=120)
check(r.get("ev")=="started" and r.get("tool")=="opencode" and r.get("id","").startswith("ses_"),f"{T()} start: {r}")
sid=r["id"]; made.append(sid)
a.send({"op":"watch","id":sid,"from":0})
p=waiting(a,sid)
check(p["tool"]=="Bash" and "touch ok.txt" in p["input"].get("command",""),f"{T()} pending: {p['tool']} {json.dumps(p['input'])[:80]}")
a.ask({"op":"answer","id":sid,"request":p["request"],"allow":True})
res=result(a,sid)
check(os.path.exists(ok_txt),f"{T()} allow: {res.get('result')!r}, ok.txt there: {os.path.exists(ok_txt)}")
tr=a.ask({"op":"transcript","id":sid,"limit":50})
check(any(i.get("tool")=="Bash" and "touch ok.txt" in json.dumps(i) for i in tr.get("items",[])),f"{T()} transcript has the Bash call ({len(tr.get('items',[]))} items)")

a.ask({"op":"send","id":sid,"text":"Run the shell command `rm ok.txt`, then reply with the word done."})
p=waiting(a,sid)
a.ask({"op":"answer","id":sid,"request":p["request"],"allow":False})
res=result(a,sid)
check(os.path.exists(ok_txt),f"{T()} deny: {res.get('result')!r}, ok.txt still there")

a.ask({"op":"mode","id":sid,"mode":"acceptEdits"})
res,asked=turn(a,sid,"Use your write tool to create the file w1.txt containing the word hi, then reply with the word ok.")
check(not {"Write","Edit"}&set(asked) and os.path.exists(os.path.join(CWD,"w1.txt")),f"{T()} acceptEdits: no write asked about ({asked}), w1.txt there: {os.path.exists(os.path.join(CWD,'w1.txt'))}")
a.ask({"op":"mode","id":sid,"mode":"manual"})
res,asked=turn(a,sid,"Use your write tool to create the file w2.txt containing the word hi, then reply with the word ok.")
check(asked[:1] in (["Write"],["Edit"]) and not os.path.exists(os.path.join(CWD,"w2.txt")),f"{T()} manual again: asked about {asked}, w2.txt there: {os.path.exists(os.path.join(CWD,'w2.txt'))}")

a.ask({"op":"send","id":sid,"text":"Count from 1 to 400, one number per line, no tools."})
a.until(lambda m:m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="stream_event" and m["data"]["event"].get("type")=="content_block_delta",120)
ti=time.time(); a.ask({"op":"interrupt","id":sid})
res=result(a,sid,60)
check(res.get("subtype")=="error_during_execution" and not res.get("is_error"),f"{T()} interrupt: result {time.time()-ti:.1f}s after, subtype={res.get('subtype')}")

m=a.until(lambda m:m.get("ev")=="exit" and m["id"]==sid,90)
check(m.get("reason")=="idle",f"{T()} idle close: {m.get('reason')}")
time.sleep(32)   # the reaper stops the server on a later pass
check(not (set(servers())-before),f"{T()} opencode serve stopped after the idle close: {sorted(set(servers())-before)}")

a.ask({"op":"send","id":sid,"text":"What file did you create with touch at the start? Reply with just the file name, no tools."})
res=result(a,sid)
check("ok.txt" in res.get("result",""),f"{T()} after resume: {res.get('result')!r}")

rn=a.ask({"op":"rename","id":sid,"title":"opencode e2e"})
check(rn.get("ev")=="renamed",f"{T()} rename: {rn}")
f=a.ask({"op":"fork","id":sid})
check(f.get("ev")=="forked" and f.get("tool")=="opencode" and f.get("from")==sid and f.get("id")!=sid,f"{T()} fork: {f}")
fid=f.get("id"); made.append(fid)
rows={r["id"]:r for r in a.ask({"op":"history"})["sessions"]}
check(rows.get(sid,{}).get("title")=="opencode e2e" and fid in rows,f"{T()} history: title {rows.get(sid,{}).get('title')!r}, fork listed: {fid in rows}")
ft=a.ask({"op":"transcript","id":fid,"limit":50})
check(any("touch ok.txt" in json.dumps(i) for i in ft.get("items",[])),f"{T()} the fork holds the conversation ({len(ft.get('items',[]))} items)")
a.send({"op":"watch","id":fid,"from":0})
res,asked=turn(a,fid,"Run the shell command `touch fork.txt`, then reply with the word ok.")
check(asked[:1]==["Bash"] and not os.path.exists(os.path.join(CWD,"fork.txt")),f"{T()} the fork asks before a command: asked about {asked}, fork.txt there: {os.path.exists(os.path.join(CWD,'fork.txt'))}")
for x in (fid,sid):
    d=a.ask({"op":"delete","id":x},t=60)
    check(d.get("ev")=="deleted" and d.get("how")=="archive",f"{T()} delete {x}: {d}")
left={r["id"] for r in a.ask({"op":"history"})["sessions"]}
check(sid not in left and fid not in left,f"{T()} archived sessions left the history")
print(f"{T()} sessions made: {made}")
print("PASS" if not fails else f"{len(fails)} FAILED")
sys.exit(1 if fails else 0)
