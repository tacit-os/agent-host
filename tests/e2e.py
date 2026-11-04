# End-to-end check against a running host: python3 tests/e2e.py SOCKET WORKDIR SESSION-UUID
# Start the host with AGENT_HOST_IDLE=8 so the idle-close step finishes. Uses real Claude turns.
import socket,json,threading,queue,time,os,subprocess,sys
SOCK,CWD,SID=sys.argv[1],sys.argv[2],sys.argv[3]
class C:
    def __init__(s):
        s.k=socket.socket(socket.AF_UNIX); s.k.connect(SOCK); s.q=queue.Queue(); s.f=s.k.makefile()
        threading.Thread(target=s.rd,daemon=True).start()
    def rd(s):
        for l in s.f: s.q.put(json.loads(l))
    def send(s,o): s.k.sendall((json.dumps(o)+"\n").encode())
    def until(s,pred,t=120):
        end=time.time()+t
        while time.time()<end:
            try: m=s.q.get(timeout=1)
            except queue.Empty: continue
            if pred(m): return m
        raise SystemExit("TIMEOUT")
    def close(s): s.k.close()
def ev(t): return lambda m: m.get("ev")=="event" and m["data"].get("type")==t
def st(state): return lambda m: m.get("ev")=="state" and m["session"]["state"]==state
t0=time.time(); T=lambda: f"{time.time()-t0:5.1f}s"
a=C(); print(T(),"hello:",a.until(lambda m:m.get("ev")=="hello")["ev"])
a.send({"op":"start","cwd":CWD,"id":SID,"mode":"manual","tag":1,"text":"Use the Bash tool to run: date +%s > probe.txt   then reply with just: ok"})
print(T(),"start:",a.until(lambda m:m.get("tag")==1))
a.send({"op":"watch","id":SID,"from":0})
m=a.until(st("waiting")); p=m["session"]["pending"][0]; print(T(),"waiting:",p["tool"],json.dumps(p["input"])[:70])
a.send({"op":"answer","id":SID,"request":p["request"],"allow":True})
r=a.until(ev("result")); print(T(),"turn 1:",repr(r["data"].get("result")),"| file:",open("probe.txt").read().strip() if os.path.exists("probe.txt") else None)
a.close()
b=C(); b.until(lambda m:m.get("ev")=="hello"); b.send({"op":"watch","id":SID,"from":0,"tag":2})
n=0
while True:
    m=b.until(lambda m:True)
    if m.get("tag")==2: break
    n+=1
print(T(),"reconnect replayed",n,"events, host seq",m["seq"])
b.send({"op":"send","id":SID,"text":"What number did you write to probe.txt? Answer from memory, number only, no tools."})
r=b.until(ev("result")); print(T(),"turn 2:",repr(r["data"].get("result")))
b.send({"op":"send","id":SID,"text":"Use the Bash tool to run: rm probe.txt"})
m=b.until(st("waiting")); p=m["session"]["pending"][0]; b.send({"op":"answer","id":SID,"request":p["request"],"allow":False})
r=b.until(ev("result")); print(T(),"deny:",repr(r["data"].get("result"))[:90],"| file still there:",os.path.exists("probe.txt"))
out=subprocess.run(["bash","-c",f"pgrep -f 'session-id {SID}|resume {SID}' | head -1 | xargs -r ps -o rss= -p"],capture_output=True,text=True).stdout.strip()
print(T(),"claude RSS while idle:",int(out)//1024 if out else "?","MB")
b.send({"op":"send","id":SID,"text":"Write the numbers 1 to 400, one per line, nothing else."})
b.until(lambda m:m.get("ev")=="event" and m["data"].get("type")=="stream_event" and m["data"]["event"].get("type")=="content_block_delta")
ti=time.time(); b.send({"op":"interrupt","id":SID})
r=b.until(ev("result")); print(T(),f"interrupt: result {time.time()-ti:.1f}s after, subtype={r['data'].get('subtype')}")
m=b.until(lambda m:m.get("ev")=="exit",t=90); print(T(),"idle close: exit code",m["code"])
b.send({"op":"send","id":SID,"text":"What number did you write to probe.txt at the start? Number only, no tools."})
r=b.until(ev("result")); print(T(),"after resume:",repr(r["data"].get("result")))
b.send({"op":"close","id":SID}); m=b.until(lambda m:m.get("ev")=="exit"); print(T(),"closed, code",m["code"])
