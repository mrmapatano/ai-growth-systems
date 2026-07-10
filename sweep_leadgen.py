#!/usr/bin/env python3
"""
================================================================================
  THE SWEEP — Compliant Lead Engine  (M&C x PSI)
================================================================================
  Repurposes the Gemini + Google-Search engine from smart_drop_turbo.py into a
  full lead-generation pipeline that produces TEXTABLE, DNC-SCRUBBED leads ready
  to import into Smarter Contact.

  PIPELINE (every lead passes through this gate):
     source (scrape OR enrich)
        -> normalize phone to E.164
        -> internal suppression list  (your own opt-outs — always applied, free)
        -> textable check             (Twilio Lookup line-type: keep mobile/VoIP)
        -> DNC scrub                  (Federal + Colorado + litigator)
        -> personalize cold text      (Gemini + live Google Search) [send-ready only]
        -> write send_ready.csv  (import this to Smarter Contact)
                 + audit.csv     (everything, with reasons)

  A lead is only marked SEND_READY when it is: not suppressed, textable, and DNC-clear.
  If a compliance API key is missing, that lead is flagged *_UNVERIFIED and is
  NEVER marked send-ready (fail-closed).

  --------------------------------------------------------------------------
  APIS USED
    google-genai   : research + copywriting   (free keys, rotated)   [required]
    apify-client   : Google Maps scrape        (APIFY_TOKEN)          [--scrape only]
    Twilio Lookup  : textable / line-type      (TWILIO_SID/TOKEN)     [recommended]
    RealValidation : DNC + litigator scrub     (REALVALIDATION_TOKEN) [recommended]
  --------------------------------------------------------------------------

  USAGE
    # Scrape fresh trades businesses in NOCO, then run the full gate:
    python3 sweep_leadgen.py --scrape --search "HVAC,plumber,electrician,roofing" \
        --location "Fort Collins, Colorado" --max 120

    # Enrich a list you already have (TSV/CSV: name<TAB>business<TAB>phone):
    python3 sweep_leadgen.py --enrich leads.tsv

    # Resume an interrupted run:
    python3 sweep_leadgen.py --enrich leads.tsv --resume
================================================================================
"""
import json, csv, time, sys, os, threading, argparse, datetime

try:
    from google import genai
    from google.genai import types
except ImportError:
    print("Run: pip3 install google-genai"); sys.exit(1)

# requests is used for Twilio + DNC HTTP calls (optional unless those keys are set)
try:
    import requests
except ImportError:
    requests = None

from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG  —  edit these
# ─────────────────────────────────────────────────────────────────────────────
MODEL = "gemini-2.0-flash"
MAX_RETRIES = 2

AUDIT_CSV      = "sweep_audit.csv"        # every lead + why it passed/failed
SENDREADY_CSV  = "sweep_send_ready.csv"   # <-- import THIS to Smarter Contact
PROGRESS_FILE  = "sweep_progress.json"
SUPPRESSION    = "suppression.txt"        # one phone per line = never contact

# The offer the cold text pitches. Edit this ONE string to change the angle.
OFFER = ("a trades-exclusive marketing pilot run by The Sweep operator team — "
         "we build the lead system and guarantee 2-3x ROAS in 90 days or it's free")
SIGNOFF = "A"   # how the text signs off:  "- A"

# Twilio line types that CAN receive a text:
TEXTABLE_TYPES = {"mobile", "nonFixedVoip", "voip"}

# ─────────────────────────────────────────────────────────────────────────────
#  TEXT CLEANING  (carried over from smart_drop_turbo — keeps JSON valid)
# ─────────────────────────────────────────────────────────────────────────────
def normalize(t):
    if not isinstance(t, str): return t
    rep = {'‘':"'", '’':"'", '“':'"', '”':'"', '–':'-', '—':'-', '…':'...', ' ':' '}
    for k, v in rep.items(): t = t.replace(k, v)
    return t

def clean(t):
    if not isinstance(t, str): return t
    return normalize(t).encode("ascii", "ignore").decode("ascii")

def clean_dict(o):
    if isinstance(o, dict): return {k: clean_dict(v) for k, v in o.items()}
    if isinstance(o, str): return clean(o)
    return o

# ─────────────────────────────────────────────────────────────────────────────
#  PHONE NORMALIZATION  (US -> E.164)
# ─────────────────────────────────────────────────────────────────────────────
def to_e164(raw):
    if not raw: return ""
    d = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(d) == 11 and d.startswith("1"): d = d[1:]
    if len(d) != 10: return ""          # not a valid US 10-digit number
    if d[0] in "01": return ""          # area code can't start with 0/1
    return "+1" + d

def ten_digit(e164):
    return e164[2:] if e164.startswith("+1") else e164

# ─────────────────────────────────────────────────────────────────────────────
#  GEMINI KEY POOL  (carried over — rotates free keys, handles rate limits)
# ─────────────────────────────────────────────────────────────────────────────
def load_keys():
    keys = []
    for i in range(1, 10):
        k = os.environ.get(f"GEMINI_KEY_{i}")
        if k: keys.append(k)
    if not keys:
        k = os.environ.get("GEMINI_API_KEY")
        if k: keys.append(k)
    return keys

def print_key_help():
    print("""
  ─────────────────────────────────────────────────────
  ADD FREE GEMINI KEYS (= faster, no limits)
  ─────────────────────────────────────────────────────
  1. https://aistudio.google.com/apikey  ->  Create API key
  2. Repeat with different Google accounts for more keys
  3. In terminal:
       export GEMINI_KEY_1="key-one"
       export GEMINI_KEY_2="key-two"
     (each key = 1,500 free requests/day)
  4. Save permanently in ~/.zshrc
  ─────────────────────────────────────────────────────
""")

class KeyPool:
    def __init__(self, keys):
        self.clients = [genai.Client(api_key=k) for k in keys]
        self.index = 0; self.lock = threading.Lock()
        self.cooldowns = [0.0] * len(keys); self.dead = [False] * len(keys)
    def get_client(self):
        while True:
            with self.lock:
                now = time.time()
                avail = [i for i in range(len(self.clients))
                         if not self.dead[i] and self.cooldowns[i] <= now]
                if avail:
                    idx = avail[self.index % len(avail)]; self.index += 1
                    return self.clients[idx], idx
                elif all(self.dead):
                    print("\n  ALL GEMINI KEYS EXHAUSTED for today. Run again tomorrow.")
                    sys.exit(0)
            time.sleep(5)
    def mark_rl(self, idx, w=65):
        with self.lock: self.cooldowns[idx] = time.time() + w
    def mark_dead(self, idx):
        with self.lock:
            self.dead[idx] = True; print(f"  Gemini key {idx+1} daily quota done.")

# ─────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 1 — internal suppression list  (always on, free)
# ─────────────────────────────────────────────────────────────────────────────
def load_suppression():
    s = set()
    if os.path.exists(SUPPRESSION):
        with open(SUPPRESSION, encoding="utf-8") as f:
            for line in f:
                e = to_e164(line)
                if e: s.add(e)
    return s

# ─────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 2 — textable / line-type  (Twilio Lookup v2)
#    Returns: ("mobile"/"landline"/..., textable_bool_or_None)
#    None textable => UNVERIFIED (no key) => fail-closed downstream
# ─────────────────────────────────────────────────────────────────────────────
_TW_SID   = os.environ.get("TWILIO_SID", "")
_TW_TOKEN = os.environ.get("TWILIO_TOKEN", "")

def check_textable(e164):
    if not (_TW_SID and _TW_TOKEN):
        return ("UNVERIFIED", None)
    if requests is None:
        return ("UNVERIFIED", None)
    try:
        r = requests.get(
            f"https://lookups.twilio.com/v2/PhoneNumbers/{e164}",
            params={"Fields": "line_type_intelligence"},
            auth=(_TW_SID, _TW_TOKEN), timeout=20)
        if r.status_code != 200:
            return ("UNVERIFIED", None)
        lti = (r.json() or {}).get("line_type_intelligence") or {}
        lt = lti.get("type") or "unknown"
        return (lt, lt in TEXTABLE_TYPES)
    except Exception:
        return ("UNVERIFIED", None)

# ─────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 3 — DNC + litigator scrub  (RealValidation-style)
#    Returns: ("clear"/"dnc"/"litigator"/"UNVERIFIED")
#    Covers Federal DNC + state lists + known TCPA litigators in one call.
# ─────────────────────────────────────────────────────────────────────────────
_RV_TOKEN = os.environ.get("REALVALIDATION_TOKEN", "")

def check_dnc(e164):
    if not _RV_TOKEN:
        return "UNVERIFIED"
    if requests is None:
        return "UNVERIFIED"
    phone = ten_digit(e164)
    try:
        r = requests.get(
            "https://api.realvalidation.com/rpvWebService/DNCLookup.php",
            params={"phone": phone, "token": _RV_TOKEN, "output": "json"},
            timeout=20)
        if r.status_code != 200:
            return "UNVERIFIED"
        j = r.json() or {}
        nat = str(j.get("national_dnc", "")).strip().upper()
        st  = str(j.get("state_dnc", "")).strip().upper()
        lit = str(j.get("litigator", "")).strip().upper()
        if lit in ("Y", "TRUE", "1"):           return "litigator"
        if nat == "Y" or st == "Y":              return "dnc"
        return "clear"
    except Exception:
        return "UNVERIFIED"

def gate(lead, suppression):
    """Run the full compliance gate. Mutates lead; returns send_ready bool."""
    e = lead["e164"]
    if not e:
        lead.update(line_type="INVALID_PHONE", textable="no",
                    dnc="-", suppressed="-", send_ready=False); return False
    if e in suppression:
        lead.update(line_type="-", textable="-", dnc="-",
                    suppressed="yes", send_ready=False); return False
    lt, textable = check_textable(e)
    dnc = check_dnc(e)
    lead.update(
        line_type=lt,
        textable=("yes" if textable else ("no" if textable is False else "UNVERIFIED")),
        dnc=dnc, suppressed="no")
    lead["send_ready"] = (textable is True) and (dnc == "clear")
    return lead["send_ready"]

# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE A — SCRAPE  (Apify Google Maps Scraper: compass/crawler-google-places)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_places(search_terms, location, max_per):
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("Run: pip3 install apify-client"); sys.exit(1)
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print("  ERROR: set APIFY_TOKEN to scrape.  export APIFY_TOKEN=\"...\"")
        sys.exit(1)

    client = ApifyClient(token)
    run_input = {
        "searchStringsArray": search_terms,
        "locationQuery": location,
        "maxCrawledPlacesPerSearch": max_per,
        "language": "en",
        "scrapePlaceDetailPage": True,
        "skipClosedPlaces": True,
    }
    print(f"  Scraping Google Maps: {search_terms} in {location} "
          f"(<= {max_per}/term)...")
    run = client.actor("compass/crawler-google-places").call(run_input=run_input)
    leads = []
    for it in client.dataset(run["defaultDatasetId"]).iterate_items():
        biz = clean(it.get("title", "") or "")
        phone = it.get("phoneUnformatted") or it.get("phone") or ""
        e = to_e164(phone)
        if not biz or not e:
            continue
        web = it.get("website") or ""
        leads.append({
            "full_name": "", "first_name": "",
            "business": biz, "phone_raw": clean(str(phone)), "e164": e,
            "reviews": str(it.get("reviewsCount", "") or ""),
            "website": "strong" if web else "none",
            "category": clean(it.get("categoryName", "") or ""),
            "city": clean(it.get("city", "") or ""),
            "source": "google_maps",
            "id": f"{biz}|{e}",
        })
    print(f"  Scraped {len(leads)} businesses with a valid phone.\n")
    return leads

# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE B — ENRICH  (TSV/CSV you already have: name <TAB> business <TAB> phone)
# ─────────────────────────────────────────────────────────────────────────────
def parse_contacts(fp):
    leads = []
    with open(fp, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            sep = "\t" if "\t" in line else ","
            parts = [p.strip() for p in line.split(sep) if p.strip()]
            if len(parts) < 2: continue
            fn = clean(parts[0]); first = fn.split()[0] if fn else ""
            biz = clean(parts[1])
            phone = clean(parts[2]) if len(parts) >= 3 else ""
            e = to_e164(phone)
            leads.append({
                "full_name": fn, "first_name": first,
                "business": biz, "phone_raw": phone, "e164": e,
                "reviews": "", "website": "", "category": "", "city": "",
                "source": "enrich", "id": f"{fn}|{biz}|{e}",
            })
    return leads

# ─────────────────────────────────────────────────────────────────────────────
#  PERSONALIZATION  (Gemini + live Google Search) — send-ready leads only
# ─────────────────────────────────────────────────────────────────────────────
def make_prompt(first, biz, phone):
    who = f"Contact first name: {first}. " if first else ""
    return (
        f"Search Google for this business and write a personalized cold TEXT message. "
        f"Business: {biz}. {who}Phone: {phone}. "
        f"Find its review count, whether it runs ads, website quality, and ONE specific detail. "
        f"Write a cold text that OPENS with that specific detail, pitches {OFFER}, "
        f"ends with 'Worth 10 min this week? - {SIGNOFF}', stays under 320 characters, "
        f"and includes a clear opt-out hint (it should read like a real person texting). "
        f'Return ONLY plain ASCII JSON, no unicode: '
        f'{{"reviews":"...","ads_running":"yes/no/unknown","website":"strong/basic/none",'
        f'"key_detail":"...","text":"..."}}')

def research(client, lead):
    r = client.models.generate_content(
        model=MODEL,
        contents=make_prompt(lead["first_name"], lead["business"], lead["phone_raw"]),
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.3))
    raw = clean(normalize(r.text.strip())).replace("```json", "").replace("```", "").strip()
    s = raw.find("{"); e = raw.rfind("}")
    if s == -1 or e == -1:
        raise ValueError(f"No JSON in response: {raw[:100]}")
    return clean_dict(json.loads(raw[s:e+1]))

# ─────────────────────────────────────────────────────────────────────────────
#  PROGRESS  (resume support — carried over)
# ─────────────────────────────────────────────────────────────────────────────
_plock = threading.RLock()   # reentrant: process() holds it while calling save_progress()
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f: return json.load(f)
    return {}
def save_progress(p):
    with _plock:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(p, f, ensure_ascii=True)

# ─────────────────────────────────────────────────────────────────────────────
#  WORKER
# ─────────────────────────────────────────────────────────────────────────────
def process(lead, pool, progress, counters, clock, plock, suppression, skip_personalize):
    cid = lead["id"]
    # 1) compliance gate (cheap APIs / local) — always runs
    send_ready = gate(lead, suppression)
    record = {"status": "screened", "lead": lead, "data": {}}

    if not send_ready:
        reason = (lead.get("suppressed") == "yes" and "SUPPRESSED") or \
                 (lead.get("line_type") == "INVALID_PHONE" and "INVALID_PHONE") or \
                 (lead.get("textable") == "no" and "NOT_TEXTABLE") or \
                 (lead.get("textable") == "UNVERIFIED" and "TEXTABLE_UNVERIFIED") or \
                 (lead.get("dnc") in ("dnc", "litigator") and f"DNC_{lead['dnc'].upper()}") or \
                 (lead.get("dnc") == "UNVERIFIED" and "DNC_UNVERIFIED") or "BLOCKED"
        record["status"] = reason
        with _plock: progress[cid] = record; save_progress(progress)
        with clock: counters["blocked"] += 1
        with plock: print(f"  [--] {lead['business'][:42]:42} BLOCKED: {reason}")
        return

    # 2) personalize (Gemini) — only for send-ready leads
    if skip_personalize:
        record["status"] = "send_ready"
        with _plock: progress[cid] = record; save_progress(progress)
        with clock: counters["ready"] += 1
        with plock: print(f"  [OK] {lead['business'][:42]:42} SEND-READY (no copy)")
        return

    for attempt in range(MAX_RETRIES + 1):
        client, idx = pool.get_client()
        try:
            data = research(client, lead)
            record["status"] = "send_ready"; record["data"] = data
            with _plock: progress[cid] = record; save_progress(progress)
            with clock: counters["ready"] += 1; n = counters["ready"]
            with plock:
                print(f"  [OK] [{n}] {lead['business'][:40]}")
                print(f"       {data.get('text','')[:110]}")
            return
        except Exception as ex:
            err = clean(str(ex))
            if "day" in err.lower() and ("quota" in err.lower() or "exhaust" in err.lower()):
                pool.mark_dead(idx); time.sleep(2)
            elif "429" in err or "quota" in err.lower() or "rate" in err.lower():
                pool.mark_rl(idx, 65)
                with plock: print(f"  [RL] Gemini key {idx+1} cooling 65s...")
                time.sleep(2)
            elif attempt < MAX_RETRIES:
                time.sleep(3)
            else:
                # passed compliance but copy failed — still send-ready, blank text
                record["status"] = "send_ready_nocopy"; record["error"] = err[:200]
                with _plock: progress[cid] = record; save_progress(progress)
                with clock: counters["ready"] += 1
                with plock: print(f"  [~] {lead['business'][:40]} ready, copy failed: {err[:50]}")
                return

# ─────────────────────────────────────────────────────────────────────────────
#  OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
def write_outputs(leads, progress):
    audit_cols = ["Business", "First Name", "Full Name", "Phone (E164)", "Phone (10)",
                  "Send Ready", "Textable", "Line Type", "DNC", "Suppressed",
                  "Message", "Key Detail", "Reviews", "Ads", "Website",
                  "Category", "City", "Source", "Status"]
    send_cols  = ["First Name", "Last Name", "Phone", "Business", "Message",
                  "Key Detail", "Line Type", "DNC Status", "Source"]

    with open(AUDIT_CSV, "w", newline="", encoding="utf-8") as fa, \
         open(SENDREADY_CSV, "w", newline="", encoding="utf-8") as fs:
        wa = csv.writer(fa); wa.writerow(audit_cols)
        ws = csv.writer(fs); ws.writerow(send_cols)
        for c in leads:
            r = progress.get(c["id"], {}); lead = r.get("lead", c); d = r.get("data", {})
            status = r.get("status", "pending")
            ready = status in ("send_ready", "send_ready_nocopy")
            wa.writerow([
                clean(c["business"]), clean(c.get("first_name", "")), clean(c.get("full_name", "")),
                lead.get("e164", c.get("e164", "")), ten_digit(lead.get("e164", c.get("e164", ""))),
                "YES" if ready else "no",
                lead.get("textable", ""), lead.get("line_type", ""),
                lead.get("dnc", ""), lead.get("suppressed", ""),
                clean(d.get("text", "")), clean(d.get("key_detail", "")),
                clean(d.get("reviews", c.get("reviews", ""))), clean(d.get("ads_running", "")),
                clean(d.get("website", c.get("website", ""))),
                clean(c.get("category", "")), clean(c.get("city", "")),
                c.get("source", ""), status])
            if ready:
                first = clean(c.get("first_name", ""))
                full = clean(c.get("full_name", ""))
                last = full[len(first):].strip() if (full and first and full.startswith(first)) else ""
                ws.writerow([
                    first, last, ten_digit(lead.get("e164", "")),
                    clean(c["business"]), clean(d.get("text", "")),
                    clean(d.get("key_detail", "")), lead.get("line_type", ""),
                    lead.get("dnc", ""), c.get("source", "")])

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="The Sweep — Compliant Lead Engine")
    p.add_argument("input", nargs="?", help="TSV/CSV for --enrich mode")
    p.add_argument("--scrape", action="store_true", help="Scrape fresh from Google Maps")
    p.add_argument("--enrich", metavar="FILE", help="Enrich an existing TSV/CSV list")
    p.add_argument("--search", default="HVAC,plumber,electrician,roofing",
                   help="Comma-separated trade terms (scrape mode)")
    p.add_argument("--location", default="Fort Collins, Colorado",
                   help="Location query (scrape mode)")
    p.add_argument("--max", type=int, default=100, help="Max places per search term")
    p.add_argument("--resume", action="store_true", help="Resume from saved progress")
    p.add_argument("--workers", type=int, default=None, help="Parallel workers (default=# Gemini keys)")
    p.add_argument("--skip-personalize", action="store_true",
                   help="Run compliance gate only, skip Gemini copywriting")
    args = p.parse_args()

    keys = load_keys()
    if not keys and not args.skip_personalize:
        print("\n  ERROR: No Gemini keys found (needed for copywriting).")
        print("  Add keys, or run with --skip-personalize to do compliance only.")
        print_key_help(); sys.exit(1)

    # ── gather leads from chosen source(s) ──
    leads = []
    if args.scrape:
        leads += scrape_places([s.strip() for s in args.search.split(",") if s.strip()],
                               args.location, args.max)
    enrich_file = args.enrich or (args.input if not args.scrape else None)
    if enrich_file:
        leads += parse_contacts(enrich_file)
    if not leads:
        print("  Nothing to do. Use --scrape ... and/or --enrich FILE."); sys.exit(1)

    # de-dupe by E.164
    seen = set(); deduped = []
    for l in leads:
        k = l["e164"] or l["id"]
        if k in seen: continue
        seen.add(k); deduped.append(l)
    leads = deduped

    suppression = load_suppression()
    n = args.workers or max(1, len(keys))

    # compliance-key status banner (so it's obvious what's verified vs not)
    tw  = "ON " if (_TW_SID and _TW_TOKEN) else "OFF (textable=UNVERIFIED, fail-closed)"
    rv  = "ON " if _RV_TOKEN else "OFF (DNC=UNVERIFIED, fail-closed)"
    print(f"\n{'='*62}")
    print(f"  THE SWEEP — Compliant Lead Engine")
    print(f"  Leads in        : {len(leads):,}")
    print(f"  Suppression list: {len(suppression):,} numbers")
    print(f"  Textable check  : Twilio Lookup  [{tw}]")
    print(f"  DNC scrub       : RealValidation [{rv}]")
    print(f"  Gemini keys     : {len(keys)}  |  Workers: {n}")
    print(f"  Outputs         : {SENDREADY_CSV}  +  {AUDIT_CSV}")
    print(f"{'='*62}\n")

    progress = load_progress() if args.resume else {}
    if args.resume:
        for k in list(progress.keys()):
            st = progress[k].get("status", "")
            if st.endswith("UNVERIFIED") or st == "error":
                del progress[k]   # retry unverified/errored on resume

    pending = [l for l in leads
               if progress.get(l["id"], {}).get("status") not in
                  ("send_ready", "send_ready_nocopy")]
    print(f"  To process: {len(pending):,}  (done: {len(leads)-len(pending):,})\n")

    pool = KeyPool(keys) if keys else None
    counters = {"ready": 0, "blocked": 0}
    clock = threading.Lock(); plock = threading.Lock()

    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = {ex.submit(process, l, pool, progress, counters, clock, plock,
                          suppression, args.skip_personalize or not keys): l
                for l in pending}
        try:
            for f in as_completed(futs): f.result()
        except KeyboardInterrupt:
            print("\n  Stopped. Progress saved."); ex.shutdown(wait=False, cancel_futures=True)

    write_outputs(leads, progress)
    print(f"\n{'='*62}")
    print(f"  SEND-READY : {counters['ready']:,}   ->  {SENDREADY_CSV}")
    print(f"  Blocked    : {counters['blocked']:,}   (see {AUDIT_CSV} for reasons)")
    print(f"  Run on     : {datetime.date.today().isoformat()}")
    print(f"{'='*62}")
    print("  Import sweep_send_ready.csv into Smarter Contact.")
    print("  Re-scrub before every send — DNC status changes daily.\n")

if __name__ == "__main__":
    main()
