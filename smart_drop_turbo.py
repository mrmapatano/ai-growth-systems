import json,csv,time,sys,os,threading,argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
try:
    from google import genai
    from google.genai import types
except ImportError:
    print("Run: pip3 install google-genai"); sys.exit(1)

OUTPUT_CSV="smart_drop_output.csv"
PROGRESS_FILE="smart_drop_progress.json"
MODEL="gemini-2.0-flash"
MAX_RETRIES=2

# ── FIX: normalize unicode BEFORE stripping, so JSON stays valid ──
def normalize(t):
    if not isinstance(t,str): return t
    replacements={
        '‘':"'",'’':"'",   # curly single quotes
        '“':'"','”':'"',   # curly double quotes  ← THIS was breaking JSON
        '–':'-','—':'-',   # em/en dash
        '…':'...',              # ellipsis
        ' ':' ',               # non-breaking space
    }
    for k,v in replacements.items(): t=t.replace(k,v)
    return t

def clean(t):
    if not isinstance(t,str): return t
    return normalize(t).encode("ascii","ignore").decode("ascii")

def clean_dict(o):
    if isinstance(o,dict): return {k:clean_dict(v) for k,v in o.items()}
    if isinstance(o,str): return clean(o)
    return o

def load_keys():
    keys=[]
    for i in range(1,10):
        k=os.environ.get(f"GEMINI_KEY_{i}")
        if k: keys.append(k)
    if not keys:
        k=os.environ.get("GEMINI_API_KEY")
        if k: keys.append(k)
    return keys

def print_key_help():
    print("""
  ─────────────────────────────────────────────────────
  HOW TO ADD MORE FREE GEMINI KEYS (= faster + no limits)
  ─────────────────────────────────────────────────────
  1. Go to https://aistudio.google.com/apikey
  2. Sign in with a Google account → click "Create API key"
  3. Repeat with different Google accounts for more keys
  4. In your terminal, run:

     export GEMINI_KEY_1="your-first-key"
     export GEMINI_KEY_2="your-second-key"
     export GEMINI_KEY_3="your-third-key"

     (Each key = 1,500 free requests/day. 5 keys = 7,500/day)

  5. To save them permanently, add those lines to ~/.zshrc
  ─────────────────────────────────────────────────────
""")

class KeyPool:
    def __init__(self,keys):
        self.clients=[genai.Client(api_key=k) for k in keys]
        self.index=0; self.lock=threading.Lock()
        self.cooldowns=[0.0]*len(keys)
        self.dead=[False]*len(keys)
    def get_client(self):
        while True:
            with self.lock:
                now=time.time()
                available=[i for i in range(len(self.clients)) if not self.dead[i] and self.cooldowns[i]<=now]
                if available:
                    idx=available[self.index%len(available)]; self.index+=1
                    return self.clients[idx],idx
                elif all(self.dead):
                    print("\n  ALL KEYS EXHAUSTED for today. Run again tomorrow."); sys.exit(0)
            time.sleep(5)
    def mark_rl(self,idx,w=65):
        with self.lock: self.cooldowns[idx]=time.time()+w
    def mark_dead(self,idx):
        with self.lock: self.dead[idx]=True; print(f"  Key {idx+1} daily quota done.")

def make_prompt(fn,biz,phone):
    return (f"Search Google for this business and write a personalized cold text. "
            f"Business: {biz}. Contact: {fn}. Phone: {phone}. "
            f"Find review count, ads running, website quality, ONE specific detail. "
            f"Write cold text opening with that detail, pitching trades-exclusive digital "
            f"marketing with 2-3x ROAS guarantee in 90 days or free, ending "
            f"Worth 10 min this week, signed - A, under 320 chars. "
            f'Return ONLY plain ASCII JSON no unicode: {{"reviews":"...","ads_running":"yes/no/unknown","website":"strong/basic/none","key_detail":"...","text":"..."}}')

def parse_contacts(fp):
    contacts=[]
    with open(fp,"r",encoding="utf-8",errors="replace") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            parts=[p.strip() for p in line.split("\t") if p.strip()]
            if len(parts)>=2:
                fn=clean(parts[0]); first=fn.split()[0]
                biz=clean(parts[1]); phone=clean(parts[2]) if len(parts)>=3 else ""
                contacts.append({"full_name":fn,"first_name":first,"business":biz,"phone":phone,"id":f"{fn}|{biz}"})
    return contacts

_plock=threading.Lock()

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE,encoding="utf-8") as f: return json.load(f)
    return {}

def save_progress(p):
    with _plock:
        with open(PROGRESS_FILE,"w",encoding="utf-8") as f: json.dump(p,f,ensure_ascii=True)

def research(client,contact):
    r=client.models.generate_content(
        model=MODEL,
        contents=make_prompt(contact["first_name"],contact["business"],contact["phone"]),
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.3))
    # normalize FIRST (fix curly quotes), then strip non-ASCII
    raw=clean(normalize(r.text.strip())).replace("```json","").replace("```","").strip()
    s=raw.find("{"); e=raw.rfind("}")
    if s==-1 or e==-1: raise ValueError(f"No JSON in response: {raw[:100]}")
    return clean_dict(json.loads(raw[s:e+1]))

def process(contact,pool,progress,counters,clock,plock):
    cid=contact["id"]
    for attempt in range(MAX_RETRIES+1):
        client,idx=pool.get_client()
        try:
            data=research(client,contact)
            with _plock: progress[cid]={"status":"done","data":data}
            save_progress(progress)
            with clock: counters["done"]+=1; total=counters["done"]+counters["skipped"]
            with plock: print(f"  [OK] [{total}] {contact['first_name']} — {contact['business'][:40]}\n"
                              f"       {data.get('text','')[:110]}")
            return
        except Exception as e:
            err=clean(str(e))
            if "day" in err.lower() and ("quota" in err.lower() or "exhaust" in err.lower()):
                pool.mark_dead(idx); time.sleep(2)
            elif "429" in err or "quota" in err.lower() or "rate" in err.lower():
                pool.mark_rl(idx,65)
                with plock: print(f"  [RL] Key {idx+1} rate limited — cooling 65s...")
                time.sleep(2)
            elif attempt<MAX_RETRIES:
                time.sleep(3)
            else:
                with _plock: progress[cid]={"status":"error","error":err[:200]}
                save_progress(progress)
                with clock: counters["failed"]+=1
                with plock: print(f"  [X] {contact['first_name']} — {err[:70]}")
                return

def write_csv(contacts,progress):
    with open(OUTPUT_CSV,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["First Name","Full Name","Business","Phone","READY-TO-SEND TEXT","Key Detail","Reviews","Ads","Website","Status"])
        for c in contacts:
            r=progress.get(c["id"],{}); d=r.get("data",{})
            w.writerow([
                clean(c["first_name"]),clean(c["full_name"]),clean(c["business"]),clean(c["phone"]),
                clean(d.get("text","")),clean(d.get("key_detail","")),clean(d.get("reviews","")),
                clean(d.get("ads_running","")),clean(d.get("website","")),
                r.get("status","pending")])

def main():
    p=argparse.ArgumentParser(description="M&C Smart Drop TURBO")
    p.add_argument("contacts_file")
    p.add_argument("--resume",action="store_true",help="Resume from saved progress")
    p.add_argument("--workers",type=int,default=None,help="Parallel workers (default=# of keys)")
    args=p.parse_args()

    keys=load_keys()
    if not keys:
        print("\n  ERROR: No API keys found.")
        print_key_help()
        sys.exit(1)

    n=args.workers or len(keys)
    print(f"\n{'='*55}")
    print(f"  M&C Smart Drop TURBO")
    print(f"  Keys loaded : {len(keys)}  |  Workers: {n}")
    print(f"  Output      : {OUTPUT_CSV}")
    print(f"{'='*55}\n")

    if len(keys)==1:
        print("  TIP: Add more keys to run faster! See instructions:")
        print_key_help()

    contacts=parse_contacts(args.contacts_file)
    progress=load_progress() if args.resume else {}

    # Reset errors so they get retried on resume
    if args.resume:
        for k in list(progress.keys()):
            if progress[k].get("status")=="error":
                del progress[k]

    pending=[c for c in contacts if progress.get(c["id"],{}).get("status")!="done"]
    skipped=len(contacts)-len(pending)
    est=len(pending)/(len(keys)*12) if len(keys) else len(pending)/12
    print(f"  Contacts : {len(contacts):,}")
    print(f"  Done     : {skipped:,}")
    print(f"  To do    : {len(pending):,}")
    print(f"  Est time : ~{est:.0f} min with {len(keys)} key(s)\n")

    pool=KeyPool(keys)
    counters={"done":0,"failed":0,"skipped":skipped}
    clock=threading.Lock(); plock=threading.Lock()

    with ThreadPoolExecutor(max_workers=n) as ex:
        futures={ex.submit(process,c,pool,progress,counters,clock,plock):c for c in pending}
        try:
            for f in as_completed(futures): f.result()
        except KeyboardInterrupt:
            print("\n  Stopped by user. Progress saved.")
            ex.shutdown(wait=False,cancel_futures=True)

    write_csv(contacts,progress)
    done=counters["done"]; failed=counters["failed"]
    print(f"\n{'='*55}")
    print(f"  Done    : {done:,}")
    print(f"  Skipped : {skipped:,}")
    print(f"  Failed  : {failed:,}")
    print(f"  Output  : {OUTPUT_CSV}")
    print(f"{'='*55}\n")

if __name__=="__main__": main()
