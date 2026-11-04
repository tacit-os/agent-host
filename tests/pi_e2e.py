# pi through the host: python3 tests/pi_e2e.py [HOST_BINARY] [--live SOCKET WORKDIR]
#
# Gate wire (always): starts a private host of its own whose HOME is a scratch folder. That home's
# pi has one provider, a scripted OpenAI-compatible model this test serves on 127.0.0.1, so nothing
# reaches the network, a real provider or the real ~/.pi. Covers the gate's report, allow, deny,
# always, plan refusing a write, acceptEdits letting one through, interrupt, idle close, resume,
# rename (live and closed), fork and delete (to the Trash). Then, with a pi that drops the gate's
# arguments: the held message is refused after the gate's wait, and a later one at once.
#
# Live (--live, against the given host): one trivial turn on pi's own provider. Skipped while the
# hello says pi is not signed in.
import socket,json,threading,queue,time,os,sys,subprocess,tempfile,re,shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
HERE=os.path.dirname(os.path.abspath(__file__))
args=[a for a in sys.argv[1:]]
LIVE=None
if "--live" in args:
    i=args.index("--live"); LIVE=(args[i+1],args[i+2]); del args[i:i+3]
BIN=args[0] if args else os.path.join(HERE,"..","target","release","agent-host")

# ── the scripted model ───────────────────────────────────────────────────────
# `touch X` / `rm X` in the newest user message: a bash call; `write X`: a write call; "count":
# numbers streamed slowly; otherwise (or after a tool result) a short text: the newest touched file
# name when the message asks for the "file", else "ok".
def text(m): return m["content"] if isinstance(m.get("content"),str) else " ".join(c.get("text","") for c in m.get("content") or [] if isinstance(c,dict))
def reply(messages):
    last=messages[-1] if messages else {}
    said=text(last)
    if last.get("role")=="user":
        m=re.search(r"`((?:touch|rm) [\w.]+)`",said)
        if m: return {"tool":"bash","args":{"command":m.group(1)}}
        m=re.search(r"`write ([\w.]+)`",said)
        if m: return {"tool":"write","args":{"path":m.group(1),"content":"written\n"}}
        if "count" in said.lower(): return {"count":True}
    names=re.findall(r"touch ([\w.]+)"," ".join(text(m) for m in messages if m.get("role")=="user"))
    return {"text":names[-1] if names and "file" in said.lower() else "ok"}
class Model(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def log_message(self,*a): pass
    def chunk(self,obj):
        data=f"data: {obj if isinstance(obj,str) else json.dumps(obj)}\n\n".encode()
        self.wfile.write(f"{len(data):x}\r\n".encode()+data+b"\r\n"); self.wfile.flush()
    def do_POST(self):
        req=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))) or b"{}")
        r=reply(req.get("messages",[]))
        self.send_response(200); self.send_header("Content-Type","text/event-stream"); self.send_header("Transfer-Encoding","chunked"); self.end_headers()
        base={"id":"fake","object":"chat.completion.chunk","created":int(time.time()),"model":"fake-1"}
        delta=lambda d,fin=None: dict(base,choices=[{"index":0,"delta":d,"finish_reason":fin}])
        try:
            if "tool" in r:
                self.chunk(delta({"role":"assistant","tool_calls":[{"index":0,"id":f"call_{time.time_ns()}","type":"function",
                                                                     "function":{"name":r["tool"],"arguments":json.dumps(r["args"])}}]}))
                self.chunk(delta({},"tool_calls"))
            elif "count" in r:
                for i in range(1,401): self.chunk(delta({"content":f"{i}\n"})); time.sleep(0.05)
                self.chunk(delta({},"stop"))
            else:
                self.chunk(delta({"role":"assistant","content":r["text"]})); self.chunk(delta({},"stop"))
            self.chunk("[DONE]"); self.wfile.write(b"0\r\n\r\n"); self.wfile.flush()
        except (BrokenPipeError,ConnectionResetError): pass

class C:
    def __init__(s,sock):
        s.k=socket.socket(socket.AF_UNIX); s.k.connect(sock); s.q=queue.Queue(); s.f=s.k.makefile(); s.tag=0
        threading.Thread(target=s.rd,daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put(json.loads(l))
    def send(s,o): s.k.sendall((json.dumps(o)+"\n").encode())
    def until(s,pred,t=60):
        end=time.time()+t
        while time.time()<end:
            try: m=s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): return m
        raise SystemExit(f"TIMEOUT {T()}")
    def ask(s,o,t=60):
        s.tag+=1; tag=s.tag; o["tag"]=tag; s.send(o); return s.until(lambda m:m.get("tag")==tag,t)
fails=[]; made=[]
def check(ok,what):
    print("ok  " if ok else "FAIL",what)
    if not ok: fails.append(what)
t0=time.time(); T=lambda: f"{time.time()-t0:5.1f}s"
def is_ev(m,sid,ty): return m.get("ev")=="event" and m["id"]==sid and m["data"].get("type")==ty
def result(c,sid,t=60): return c.until(lambda m:is_ev(m,sid,"result"),t)["data"]
def waiting(c,sid): return c.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["state"]=="waiting")["session"]["pending"][0]

# pi without agent-host's gate: `-e <gate>` and `--praxis-mode <mode>` are dropped.
NO_GATE_PI='''#!/bin/bash
a=(); skip=0
for x in "$@"; do
  if [ $skip = 1 ]; then skip=0; continue; fi
  case "$x" in -e|--praxis-mode) skip=1;; *) a+=("$x");; esac
done
exec PI "${a[@]}"
'''

def gate_wire(test,gateless=False):
    tmp=tempfile.mkdtemp(prefix="pi-e2e-",dir="/tmp/claude-1000" if os.path.isdir("/tmp/claude-1000") else None)
    home,rt,work=f"{tmp}/home",f"{tmp}/rt",f"{tmp}/work"
    os.makedirs(f"{home}/.pi/agent"); os.makedirs(work); os.mkdir(rt,0o700)
    uid=os.getuid()
    for n in ("bus","systemd"):   # the user's systemd, for systemd-run --user
        if os.path.exists(f"/run/user/{uid}/{n}"): os.symlink(f"/run/user/{uid}/{n}",f"{rt}/{n}")
    srv=ThreadingHTTPServer(("127.0.0.1",0),Model); port=srv.server_address[1]
    threading.Thread(target=srv.serve_forever,daemon=True).start()
    json.dump({"providers":{"fake":{"baseUrl":f"http://127.0.0.1:{port}/v1","api":"openai-completions","apiKey":"fake",
               "compat":{"supportsDeveloperRole":False,"supportsReasoningEffort":False},
               "models":[{"id":"fake-1","name":"Fake","reasoning":False,"input":["text"],"contextWindow":100000,"maxTokens":4000}]}}},
              open(f"{home}/.pi/agent/models.json","w"))
    json.dump({"defaultProvider":"fake","defaultModel":"fake-1"},open(f"{home}/.pi/agent/settings.json","w"))
    env=dict(os.environ,HOME=home,XDG_RUNTIME_DIR=rt,AGENT_HOST_IDLE="8",AGENT_HOST_CACHE=f"{tmp}/cache",PI_OFFLINE="1")
    for k in ("XDG_DATA_HOME","XDG_STATE_HOME","XDG_CONFIG_HOME","PI_CODING_AGENT_DIR"): env.pop(k,None)
    if gateless:
        os.mkdir(f"{tmp}/bin"); open(f"{tmp}/bin/pi","w").write(NO_GATE_PI.replace("PI",shutil.which("pi"))); os.chmod(f"{tmp}/bin/pi",0o755)
        env["PATH"]=f"{tmp}/bin:{env['PATH']}"
    sock=f"{rt}/agent-host.sock"
    host=subprocess.Popen(["nice","-n","10",BIN,"--socket",sock],env=env,stdout=subprocess.DEVNULL,stderr=open(f"{tmp}/host.log","w"))
    try:
        for _ in range(50):
            if os.path.exists(sock): break
            time.sleep(0.1)
        test(C(sock),work,home)
    finally:
        host.terminate(); host.wait(10); srv.shutdown()
        print(f"     scratch: {tmp} (host log: {tmp}/host.log)")

def run(a,W,home):
    hello=a.until(lambda m:m.get("ev")=="hello")
    a.ask({"op":"hello","proto":6})
    pi=hello.get("agents",{}).get("pi",{})
    # Q9: a key in pi's own file does not make pi ready (only an account's provider does), so the
    # scratch home's provider leaves signedIn False; a session still starts (the steps below)
    check(pi.get("installed") and pi.get("signedIn") is False,f"{T()} hello: pi {pi.get('version')} signedIn={pi.get('signedIn')} (a key in pi's own file is not ready, Q9)")
    check(pi.get("caps",{}).get("steer") is True and pi.get("caps",{}).get("rewind") is False,f"{T()} pi caps {pi.get('caps')}")
    gate=os.path.join(home,".local/share/agent-host/pi-gate.ts")

    r=a.ask({"op":"start","cwd":W,"agent":"pi","mode":"manual","text":"Run `touch ok.txt`, then reply ok."})
    check(r.get("ev")=="started" and r.get("tool")=="pi",f"{T()} start: {r}")
    sid=r["id"]; made.append(sid)
    a.send({"op":"watch","id":sid,"from":0})
    st=a.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["mode"]=="manual")["session"]
    check(os.path.exists(gate),f"{T()} the gate reported mode {st['mode']!r} (written to {os.path.relpath(gate,home)})")
    p=waiting(a,sid)
    check(p["tool"]=="Bash" and p["input"].get("command")=="touch ok.txt",f"{T()} pending: {p['tool']} {json.dumps(p['input'])}")
    a.ask({"op":"answer","id":sid,"request":p["request"],"allow":True})
    res=result(a,sid)
    check(res.get("subtype")=="success" and os.path.exists(f"{W}/ok.txt"),f"{T()} allow: {res.get('result')!r}, ok.txt there: {os.path.exists(f'{W}/ok.txt')}")
    tr=a.ask({"op":"transcript","id":sid,"limit":50})
    check(any(i.get("tool")=="Bash" and "touch ok.txt" in json.dumps(i) for i in tr.get("items",[])),f"{T()} transcript has the Bash call ({len(tr.get('items',[]))} items)")

    a.ask({"op":"send","id":sid,"text":"Run `rm ok.txt`, then reply done."})
    p=waiting(a,sid)
    a.ask({"op":"answer","id":sid,"request":p["request"],"allow":False})
    res=result(a,sid)
    check(os.path.exists(f"{W}/ok.txt"),f"{T()} deny: {res.get('result')!r}, ok.txt still there")

    a.ask({"op":"send","id":sid,"text":"Run `touch a.txt`."})
    p=waiting(a,sid)
    a.ask({"op":"answer","id":sid,"request":p["request"],"allow":True,"permissions":p.get("suggestions")})
    result(a,sid)
    asked=[]
    def none_asked(m):
        if m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["state"]=="waiting": asked.append(1)
        return is_ev(m,sid,"result")
    a.ask({"op":"send","id":sid,"text":"Run `touch b.txt`."}); a.until(none_asked)
    check(not asked and os.path.exists(f"{W}/b.txt"),f"{T()} always: the second bash ran unasked: {not asked}, b.txt there: {os.path.exists(f'{W}/b.txt')}")

    a.ask({"op":"mode","id":sid,"mode":"plan"})
    a.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["mode"]=="plan")
    a.ask({"op":"send","id":sid,"text":"Now `write plan.txt`."})
    tr=a.until(lambda m:is_ev(m,sid,"user") and "tool_result" in json.dumps(m["data"]))["data"]["message"]["content"][0]
    result(a,sid)
    check(tr.get("is_error") and not os.path.exists(f"{W}/plan.txt"),f"{T()} plan: the write was refused unasked: {str(tr.get('content'))[:70]!r}")
    a.ask({"op":"mode","id":sid,"mode":"acceptEdits"})
    a.until(lambda m:m.get("ev")=="state" and m["session"]["id"]==sid and m["session"]["mode"]=="acceptEdits")
    asked.clear(); a.ask({"op":"send","id":sid,"text":"Now `write edits.txt`."}); a.until(none_asked)
    check(not asked and os.path.exists(f"{W}/edits.txt"),f"{T()} acceptEdits: the write ran unasked: {os.path.exists(f'{W}/edits.txt')}")

    a.ask({"op":"send","id":sid,"text":"Now count to 400."})
    a.until(lambda m:is_ev(m,sid,"stream_event") and m["data"]["event"].get("type")=="content_block_delta")
    ti=time.time(); a.ask({"op":"interrupt","id":sid})
    res=result(a,sid)
    check(res.get("subtype")=="error_during_execution" and not res.get("is_error"),f"{T()} interrupt: result {time.time()-ti:.1f}s after, subtype={res.get('subtype')}")

    m=a.until(lambda m:m.get("ev")=="exit" and m["id"]==sid,60)
    check(m.get("reason")=="idle",f"{T()} idle close: {m.get('reason')}")
    a.ask({"op":"send","id":sid,"text":"Which file did you create most recently? Reply with the file name."})
    res=result(a,sid)
    check(res.get("result")=="b.txt",f"{T()} after resume: {res.get('result')!r}")

    rn=a.ask({"op":"rename","id":sid,"title":"pi e2e"})
    check(rn.get("ev")=="renamed",f"{T()} rename live: {rn}")
    f=a.ask({"op":"fork","id":sid})
    check(f.get("ev")=="forked" and f.get("tool")=="pi" and f.get("from")==sid and f.get("id")!=sid,f"{T()} fork: {f}")
    fid=f.get("id"); made.append(fid)
    a.send({"op":"watch","id":fid,"from":0})
    a.ask({"op":"send","id":fid,"text":"Which file did you create most recently? Reply with the file name."})
    res=result(a,fid)
    check(res.get("result")=="b.txt",f"{T()} the fork remembers: {res.get('result')!r}")
    a.ask({"op":"close","id":sid}); a.until(lambda m:m.get("ev")=="exit" and m["id"]==sid,30)
    rn=a.ask({"op":"rename","id":fid,"title":"pi e2e fork"})
    check(rn.get("ev")=="renamed",f"{T()} rename live fork: {rn}")
    rn=a.ask({"op":"rename","id":sid,"title":"pi e2e closed"})
    check(rn.get("ev")=="renamed",f"{T()} rename closed: {rn}")
    time.sleep(0.5)
    rows={r["id"]:r for r in a.ask({"op":"history"})["sessions"]}
    check(rows.get(sid,{}).get("title")=="pi e2e closed" and rows.get(fid,{}).get("title")=="pi e2e fork",
          f"{T()} history titles: {rows.get(sid,{}).get('title')!r}, {rows.get(fid,{}).get('title')!r}")
    for x in (fid,sid):
        path=rows.get(x,{}).get("path","")
        d=a.ask({"op":"delete","id":x},t=60)
        check(d.get("ev")=="deleted" and d.get("how")=="trash" and path and not os.path.exists(path),f"{T()} delete {x[:8]}: {d}")
    left={r["id"] for r in a.ask({"op":"history"})["sessions"]}
    check(sid not in left and fid not in left,f"{T()} deleted sessions left the history")

def no_gate(a,W,home):
    a.until(lambda m:m.get("ev")=="hello"); a.ask({"op":"hello","proto":6})
    r=a.ask({"op":"start","cwd":W,"agent":"pi","mode":"manual","text":"Run `touch held.txt`, then reply ok."})
    sid=r.get("id"); made.append(sid)
    a.send({"op":"watch","id":sid,"from":0})
    ti=time.time(); res=result(a,sid,40)
    check(res.get("is_error") and "gate did not load" in res.get("result","") and time.time()-ti>10,f"{T()} no gate: the held message refused {time.time()-ti:.1f}s later: {res.get('result')!r}")
    r=a.ask({"op":"send","id":sid,"text":"Run `touch late.txt`, then reply ok."})
    st=a.ask({"op":"list"}).get("sessions",[])
    state=next((x["state"] for x in st if x["id"]==sid),None)
    check(r.get("ev")=="error" and "gate did not load" in r.get("message","") and state=="idle",f"{T()} no gate: a later message refused at once: {r}, state {state}")
    check(not os.path.exists(f"{W}/held.txt") and not os.path.exists(f"{W}/late.txt"),f"{T()} no gate: nothing ran")
    a.ask({"op":"close","id":sid})

def live(sock,W):
    a=C(sock); hello=a.until(lambda m:m.get("ev")=="hello"); a.ask({"op":"hello","proto":6})
    pi=hello.get("agents",{}).get("pi",{})
    if pi.get("signedIn") is not True:
        print(f"SKIP live: pi is not signed in ({pi.get('reason') or pi.get('signedIn')})"); return
    r=a.ask({"op":"start","cwd":W,"agent":"pi","text":"Reply with just: ok"},t=120)
    check(r.get("ev")=="started" and r.get("tool")=="pi",f"{T()} live start: {r}")
    sid=r["id"]; made.append(sid)
    a.send({"op":"watch","id":sid,"from":0})
    res=result(a,sid,180)
    check("ok" in str(res.get("result","")).lower(),f"{T()} live reply: {res.get('result')!r}")
    d=a.ask({"op":"delete","id":sid},t=60)
    check(d.get("ev")=="deleted",f"{T()} live delete: {d}")

gate_wire(run)
gate_wire(no_gate,gateless=True)
if LIVE: live(*LIVE)
else: print("SKIP live: no --live SOCKET WORKDIR given")
print(f"{T()} sessions made: {made}")
print("PASS" if not fails else f"{len(fails)} FAILED")
sys.exit(1 if fails else 0)
