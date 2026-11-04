# Protocol 5 against a private host: python3 tests/folders_e2e.py SOCKET WORKDIR [--no-codex]
# WORKDIR: a fresh scratch folder. Writes touch only sessions this test starts there: two Claude
# turns (trivial prompts), a fork with one more, then rename and delete. Codex: one thread with one
# trivial turn, closed, resumed with no agent, archived. Prints the session ids it made.
import socket,json,threading,queue,time,os,sys
SOCK,CWD=sys.argv[1],sys.argv[2]
CODEX="--no-codex" not in sys.argv
class C:
    def __init__(s):
        s.k=socket.socket(socket.AF_UNIX); s.k.connect(SOCK); s.q=queue.Queue(); s.f=s.k.makefile(); s.n=0; s.tag=0
        threading.Thread(target=s.rd,daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put((len(l),json.loads(l)))
    def send(s,o): s.k.sendall((json.dumps(o)+"\n").encode())
    def until(s,pred,t=180):
        end=time.time()+t
        while time.time()<end:
            try: n,m=s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): s.n=n; return m
        raise SystemExit("TIMEOUT")
    def ask(s,o,t=180):
        s.tag+=1; tag=s.tag; o["tag"]=tag; s.send(o); return s.until(lambda m:m.get("tag")==tag,t)
fails=[]; made=[]
def check(ok,what):
    print("ok  " if ok else "FAIL",what)
    if not ok: fails.append(what)
def result(c,sid,t=180): return c.until(lambda m:m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")=="result",t)
t0=time.time()

# the hello
a=C(); hello=a.until(lambda m:m.get("ev")=="hello")
check(hello.get("proto")==6,f"hello proto {hello.get('proto')}")
ag=hello.get("agents",{})
check(set(ag)=={"claude","codex","pi","opencode"},f"hello agents {sorted(ag)}")
for k,v in ag.items():
    check(set(v)>={"installed","version","signedIn","reason","caps"} and set(v["caps"])=={"fork","rename","delete","rewind","rewindFiles","aside","tasks","steer","effort","modes"}
          and isinstance(v["signedIn"],bool) and (v["reason"]=="")==v["signedIn"],
          f"{k}: installed={v.get('installed')} version={v.get('version')} signedIn={v.get('signedIn')} reason={v.get('reason')!r}")

# history: a protocol 4 connection, then this one after hello proto 5
old=C(); old.until(lambda m:m.get("ev")=="hello")
r4=old.ask({"op":"history"})["sessions"]; n4=old.n
check(a.ask({"op":"hello","proto":6}).get("ev")=="ok","hello op answered")
r5=a.ask({"op":"history"})["sessions"]; n5=a.n
t4={r["tool"] for r in r4}; t5={r["tool"] for r in r5}
check(t4<= {"claude","codex"} and "files" not in r4[0],f"protocol 4: {len(r4)} rows, tools {sorted(t4)}, {n4/1e3:.0f} KB")
check(all({"files","lastReply","resume","repo","root","local"}<=set(r) for r in r5),f"protocol 5: {len(r5)} rows, tools {sorted(t5)}, {n5/1e3:.0f} KB")
check(len(r5)>=len(r4),"protocol 5 lists at least the protocol 4 rows")
again=a.ask({"op":"history","ifChanged":True})
check(again.get("unchanged") is True or "sessions" in again,f"ifChanged answered {'unchanged' if again.get('unchanged') else 'with rows'} ({a.n} bytes)")

# a Claude session, with a file given as an @-mention and a file the turn makes outside any tool
note=os.path.join(CWD,"note.txt"); open(note,"w").write("The word is banana.\n")
r=a.ask({"op":"start","cwd":CWD,"mode":"auto","text":"Reply with just the word in the attached file, nothing else.","files":[note]})
check(r.get("ev")=="started" and r.get("tool")=="claude",f"start: {r}")
sid=r["id"]; made.append(("claude",sid))
a.send({"op":"watch","id":sid,"from":0})
time.sleep(0.5); open(os.path.join(CWD,"swept.txt"),"w").write("made while the turn ran\n")
res=result(a,sid)["data"]
check("banana" in str(res.get("result","")).lower(),f"@-mention read: {res.get('result')!r}")
live=a.ask({"op":"list"})["sessions"]; me=[s for s in live if s["id"]==sid][0]
check(me.get("tool")=="claude" and me.get("caps",{}).get("rewind") is True,f"summary tool={me.get('tool')} caps={me.get('caps')}")
time.sleep(1.5)
ch=a.ask({"op":"changes","id":sid})
check("repo" in ch and any(m["path"].endswith("swept.txt") and m.get("how")=="sweep" for m in ch.get("made",[])),f"changes repo={ch.get('repo')} made={[(os.path.basename(m['path']),m.get('how')) for m in ch.get('made',[])]}")

# fork it: a new session that starts as a copy
f=a.ask({"op":"fork","id":sid})
check(f.get("ev")=="forked" and f.get("from")==sid and f.get("tool")=="claude" and f.get("id")!=sid,f"fork: {f}")
fid=f.get("id"); made.append(("claude",fid))
a.send({"op":"watch","id":fid,"from":0})
a.ask({"op":"send","id":fid,"text":"What was the word? Reply with just the word."})
res=result(a,fid)["data"]
check("banana" in str(res.get("result","")).lower(),f"the fork remembers: {res.get('result')!r}")

# rename the fork live and the original after it closes
rn=a.ask({"op":"rename","id":fid,"title":"folders e2e fork"})
check(rn.get("ev")=="renamed",f"rename live: {rn}")
a.ask({"op":"close","id":sid}); a.until(lambda m:m.get("ev")=="exit" and m["id"]==sid,60)
rn=a.ask({"op":"rename","id":sid,"title":"folders e2e original"})
check(rn.get("ev")=="renamed",f"rename closed: {rn}")
rows={r["id"]:r for r in a.ask({"op":"history"})["sessions"]}
check(rows.get(fid,{}).get("title")=="folders e2e fork" and rows.get(sid,{}).get("title")=="folders e2e original",
      f"history titles: {rows.get(fid,{}).get('title')!r}, {rows.get(sid,{}).get('title')!r}")

# delete both: the live fork is closed first; the files go to the Trash
for x in (fid,sid):
    path=rows.get(x,{}).get("path","")
    d=a.ask({"op":"delete","id":x},t=60)
    check(d.get("ev")=="deleted" and d.get("how")=="trash" and path and not os.path.exists(path),f"delete {x[:8]}: {d}")
left={r["id"] for r in a.ask({"op":"history"})["sessions"]}
check(sid not in left and fid not in left,"deleted sessions left the history")
check(all(s["id"] not in (sid,fid) for s in a.ask({"op":"list"})["sessions"]),"deleted sessions left the live list")
check(a.ask({"op":"start","cwd":CWD,"agent":"nope"}).get("ev")=="error","an unknown agent is refused")

if CODEX:
    # a Codex thread started here, then resumed with no agent named
    r=a.ask({"op":"start","cwd":CWD,"agent":"codex","mode":"manual","text":"Reply with just: ok"},t=120)
    check(r.get("ev")=="started" and r.get("tool")=="codex",f"codex start: {r}")
    cid=r.get("id"); made.append(("codex",cid))
    a.send({"op":"watch","id":cid,"from":0}); result(a,cid)
    a.send({"op":"close","id":cid}); a.until(lambda m:m.get("ev")=="exit" and m["id"]==cid,60)
    r=a.ask({"op":"start","cwd":CWD,"resume":cid},t=120)
    if r.get("ev")=="error" and "thread history" in r.get("message",""):
        # Codex itself refused to reopen the thread (its history database): the start reached Codex
        print("BLOCKED resume with no agent reached Codex, which refused:",r["message"])
    else:
        check(r.get("ev")=="started" and r.get("tool")=="codex",f"resume with no agent: {r}")
        me=[s for s in a.ask({"op":"list"})["sessions"] if s["id"]==cid]
        check(bool(me) and me[0]["alive"] and me[0]["tool"]=="codex","the resumed thread runs on Codex")
    d=a.ask({"op":"delete","id":cid},t=60)
    check(d.get("ev")=="deleted" and d.get("how")=="archive",f"codex delete: {d}")

print(f"{time.time()-t0:.0f}s; sessions made: {made}")
print("PASS" if not fails else f"{len(fails)} FAILED")
sys.exit(1 if fails else 0)
