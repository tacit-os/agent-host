# The session index against a private host: python3 tests/history_e2e.py SOCKET CACHE-DIR
# (FULL=1 compares every session with its changes and transcript instead of a sample)
# Start the host with AGENT_HOST_CACHE=CACHE-DIR (empty, to time the first index build). Reads only;
# starts no agent. A host that serves protocol 5 or later (after {"op":"hello","proto":6}) is checked on its
# rows. Protocol 4 rows keep their old fields: then files and lastReply are checked in the saved
# index, and the other protocol 5 fields by `cargo test rows_by_protocol`.
import socket,json,threading,queue,time,os,subprocess,sys,random
SOCK,CACHE=sys.argv[1],sys.argv[2]
PROBE="01a0ffbd-2e99-7da0-88e9-23f84fb37d2a"
class C:
    def __init__(s):
        s.k=socket.socket(socket.AF_UNIX); s.k.connect(SOCK); s.q=queue.Queue(); s.f=s.k.makefile(); s.n=0
        threading.Thread(target=s.rd,daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put((len(l),json.loads(l)))
    def send(s,o): s.k.sendall((json.dumps(o)+"\n").encode())
    def until(s,pred,t=300):
        end=time.time()+t
        while time.time()<end:
            try: n,m=s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): s.n=n; return m
        raise SystemExit("TIMEOUT")
    def ask(s,o,t=300):
        s.tag=getattr(s,"tag",0)+1; tag=s.tag; o["tag"]=tag; s.send(o); return s.until(lambda m:m.get("tag")==tag,t)
fails=[]
def check(ok,what):
    if not ok: fails.append(what); print("FAIL",what)
t0=time.time()
a=C(); hello=a.until(lambda m:m.get("ev")=="hello")
live={s["id"] for s in hello.get("sessions",[])}
P5=a.ask({"op":"hello","proto":6},t=30).get("ev")!="error"   # an older host answers with an error
r=a.ask({"op":"history"}); rows=r["sessions"]
print(f"first history: {time.time()-t0:.1f}s, {len(rows)} rows, {a.n/1e6:.3f} MB, protocol {5 if P5 else 4}")
ms=[]
for _ in range(9):
    t1=time.time(); a.ask({"op":"history"}); ms.append((time.time()-t1)*1000)
print(f"history again: median {sorted(ms)[4]:.0f} ms of 9, {a.n/1e6:.3f} MB")
tools={}
for s in rows: tools[s.get("tool")]=tools.get(s.get("tool"),0)+1
print("rows per tool:",tools)
idx=json.load(open(os.path.join(CACHE,"index.json")))
if not P5:
    OLD={"id","tool","path","cwd","title","prompt","lastPrompt","started","updated","bytes","entry","parent","own"}
    for s in rows:
        check(set(s)==OLD,f"protocol 4 row {s['id']} has {sorted(set(s)^OLD)}")
    check(all(s["tool"] in ("claude","codex") for s in rows),"protocol 4 rows include pi or opencode")
    print("protocol 4: every row has the old fields only")
    # files and lastReply as protocol 5 serves them, from the saved index
    saved={e["path"]:e for e in idx["entries"]}
    for s in rows:
        e=saved.get(s["path"],{}); seen=e.get("seen",[])
        s["files"]={"n":len(seen),"add":e.get("add",0),"del":e.get("del",0),"names":[x.replace("\\","/").rsplit("/",1)[-1] for x in seen[::-1][:3]]}
        s["lastReply"]=e.get("reply","")

# every row has the new fields
for s in rows if P5 else []:
    f=s.get("files")
    ok=(s.get("tool") in ("claude","codex","pi","opencode") and isinstance(f,dict)
        and all(isinstance(f.get(k),int) for k in ("n","add","del")) and isinstance(f.get("names"),list) and len(f["names"])<=min(3,f["n"])
        and isinstance(s.get("lastReply"),str) and len(s["lastReply"])<=300 and s["lastReply"]==" ".join(s["lastReply"].split())
        and isinstance(s.get("resume"),str) and s["id"] in s["resume"]
        and isinstance(s.get("repo"),bool) and isinstance(s.get("root"),str) and s["repo"]==bool(s["root"])
        and isinstance(s.get("local"),bool) and s["local"]==(bool(s["cwd"]) and os.path.isdir(s["cwd"])))
    check(ok,f"fields of {s['tool']} {s['id']}: {json.dumps({k:s.get(k) for k in ('files','lastReply','resume','repo','root','local','cwd')})[:300]}")
    if s["local"] and s["tool"] in ("claude","codex"):
        want="cd '"+s["cwd"].replace("'","'\\''")+"' && "+("claude --resume " if s["tool"]=="claude" else "codex resume ")+s["id"]
        check(s["resume"]==want,f"resume of {s['id']}: {s['resume']}")
ids=[s["id"] for s in rows]
check(len(set(ids))==len(ids),f"{len(ids)-len(set(ids))} rows share an id")
print("checked the fields of",len(rows),"rows" if P5 else "rows (protocol 4: resume, repo, root, local not served)")

# files.n against changes, lastReply against the transcript's last words: the newest and some at random
def sample(tool,n):
    mine=[s for s in rows if s["tool"]==tool and s["id"] not in live]
    if os.environ.get("FULL"): return mine
    random.seed(7); return mine[:n]+random.sample(mine[n:],min(n,max(0,len(mine)-n)))
edits=replies=written=0
for tool in ("claude","codex","opencode","pi"):
    for s in sample(tool,12):
        tc=time.time(); c=a.ask({"op":"changes","id":s["id"]})
        if os.environ.get("VERBOSE"): print(f"  {tool} {s['id']} {s['bytes']/1e6:.0f} MB changes {time.time()-tc:.1f}s",flush=True)
        n=len(c.get("files",[]))
        check(s["files"]["n"]==n,f"{tool} {s['id']}: files.n {s['files']['n']} != changes {n}")
        check(isinstance(c.get("repo"),bool) and isinstance(c.get("made"),list),f"changes of {s['id']} lacks repo/made")
        for m in c.get("made",[]):
            check(set(m)>={"path","kind","size","at","how"} and m["how"] in ("tool","image","sweep") and os.path.isfile(m["path"]),f"made entry {m}")
        written+=len(c.get("made",[]))
        edits+=n>0
        t=a.ask({"op":"transcript","id":s["id"],"limit":400})
        said=[i["text"] for i in t["items"] if i.get("role")=="assistant" and i.get("kind")=="text"]
        if said:
            w=" ".join(said[-1].split()); want=w if len(w)<=300 else w[:299]+"…"
            check(s["lastReply"]==want,f"{tool} {s['id']}: lastReply {s['lastReply'][:60]!r} != {want[:60]!r}")
            replies+=1
print(f"compared files.n with changes and lastReply with transcripts ({edits} sessions with edits, {replies} with replies, {written} made files)")

# opencode: one row per top-level session (archived ones are hidden, as Codex's are)
db=os.path.expanduser("~/.local/share/opencode/opencode.db")
if os.path.exists(db):
    q=lambda sql:int(subprocess.run(["sqlite3","-readonly",db,sql],capture_output=True,text=True).stdout.strip())
    top=q("select count(*) from session where parent_id is null"); shown=q("select count(*) from session where parent_id is null and time_archived is null")
    served=[s for s in rows if s["tool"]=="opencode"]
    if served:
        n,where=len(served),"history"
    else:
        idx=json.load(open(os.path.join(CACHE,"index.json")))
        n,where=sum(1 for e in idx["entries"] if e["tool"]=="opencode" and e["id"]),"saved index (this host serves no opencode rows)"
    print(f"opencode: {n} in the {where}; sqlite {top} without a parent, {shown} of them not archived")
    check(n==shown,f"opencode rows {n} != {shown}")
    for s in served[:5]:
        t=a.ask({"op":"transcript","id":s["id"],"limit":400}); c=a.ask({"op":"changes","id":s["id"]})
        check(t.get("ev")=="transcript" and c.get("ev")=="changes" and s["files"]["n"]==len(c["files"]),f"opencode {s['id']} transcript/changes")

# search reaches pi (rg) and opencode (SQL), when the host serves their rows
for tool in ("opencode","pi"):
    for s in [s for s in rows if s["tool"]==tool and len(s["prompt"].split())>=3][:3]:
        q=" ".join(s["prompt"].split()[:4])
        res=a.ask({"op":"search","q":q})["results"]
        check(s["id"] in {x["id"] for x in res} or len(res)>=100,f"search {q!r} misses {tool} {s['id']}")
        print(f"search {q!r}: {len(res)} results, {tool} {s['id']} {'found' if s['id'] in {x['id'] for x in res} else 'cut at 100'}")

# pi: one row per session file
pi=[os.path.join(d,f) for d,_,fs in os.walk(os.path.expanduser("~/.pi/agent/sessions")) for f in fs if f.endswith(".jsonl")]
pis=[e for e in idx["entries"] if e["tool"]=="pi"]
check(len(pis)==len(pi) and all(e["id"] and e["cwd"] for e in pis),f"pi: {len(pis)} indexed, {len(pi)} files")
print(f"pi: {len(pis)} indexed of {len(pi)} files")

# the Codex code-mode probe: the command run, not the JavaScript that ran it
t=a.ask({"op":"transcript","id":PROBE,"limit":400})
tools_=[(i.get("tool"),i.get("summary") or i.get("text","")) for i in t.get("items",[]) if i.get("kind")=="tool"]
print("probe tools:",tools_)
check(any(n=="Bash" and "touch ok.txt" in str(x) for n,x in tools_),"probe shows no Bash touch ok.txt")
check(not any(n in ("exec","custom") or "await tools" in str(x) for n,x in tools_),"probe shows the JavaScript")
probe=[s for s in rows if s["id"]==PROBE]
check(bool(probe) and probe[0]["lastReply"]=="ok","probe lastReply")

# Codex: a v2 FileChange shows as an edit with its files; generated images are in changes' made
byid={s["id"]:s for s in rows}
fc="01a071ed-8df4-7640-84af-2088463bc84b"
if fc in byid:
    t=a.ask({"op":"transcript","id":fc,"limit":400})
    ed=[i for i in t["items"] if i.get("kind")=="tool" and i.get("tool") in ("Edit","Write") and i.get("files")]
    check(bool(ed) and byid[fc]["files"]["n"]>0,f"v2 FileChange {fc}: {len(ed)} edits, files.n {byid[fc]['files']['n']}")
    print(f"v2 FileChange {fc}: {len(ed)} edit items, files {byid[fc]['files']}")
for img in ("01a0e0d5-0842-7e31-a699-672332836415","019f13a9-684c-7481-8a55-a9fed1ebf04d"):
    if img in byid:
        made=a.ask({"op":"changes","id":img})["made"]
        pics=[m for m in made if m["how"]=="image" and m["kind"]=="image" and os.path.isfile(m["path"])]
        check(bool(pics),f"codex {img}: no generated image in made")
        print(f"codex {img}: {len(pics)} generated images in made, e.g. {pics[0]['path'] if pics else None}")

print("PASS" if not fails else f"{len(fails)} FAILED")
sys.exit(1 if fails else 0)
