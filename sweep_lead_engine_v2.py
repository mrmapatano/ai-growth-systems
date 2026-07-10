#!/usr/bin/env python3
"""
================================================================================
  THE SWEEP — LEAD ENGINE v2   (M&C x PSI)
  One file in (Apify scrape CSV)  ->  two trustworthy lists out.
================================================================================
  Built off sweep_leadgen.py (v1). v2 adds the ICP / Fit-Score "brain", ingests
  the Apify CSV directly, dedupes, and writes TWO lists instead of one.

  PIPELINE (every lead passes this gate, in order):
     Apify CSV  (title, phone, city, state, categoryName [, website, reviewsCount, rating])
        -> normalize phone to E.164  (+1XXXXXXXXXX)
        -> drop invalid phones
        -> dedupe (one row per phone)
        -> internal SUPPRESSION list           (our opt-outs — always applied, free)
        -> FIT-SCORE / ICP brain:
             * hard-drop national franchises (name list)
             * hard-drop commercial-only / non-trade categories
             * score the rest 0-100 (NoCo geo + residential trade + size + owner signal)
             * Fit_Status: Pursue 75+ / Hold 50-74 / Disqualify <50
        -> textable check   (Twilio Lookup line-type: keep mobile/VoIP)   [key-gated]
        -> DNC scrub        (RealValidation: Federal + CO + litigator)     [key-gated]
        -> write callable.csv  + textable.csv  + audit.csv

  TWO OUTPUTS:
     callable.csv  — valid, deduped, ICP-scored, sorted BEST-FIRST. Dialers load today.
                     (call-first phase: B2B calls are largely DNC-exempt; DNC flagged.)
     textable.csv  — mobiles/VoIP only AND DNC-clear. SEND-READY. Held until 10DLC.
     audit.csv     — every lead + the reason it passed or was dropped (proof of scrub).

  FAIL-CLOSED (non-negotiable):
     A number reaches textable.csv ONLY if a real line-type check says mobile/VoIP
     AND a real DNC check says clear. No key = UNVERIFIED = never textable. When in
     doubt, it does NOT go out.

  RUN
     # Cloud (recommended): open Sweep_Lead_Engine_v2.ipynb in Google Colab, upload CSV, Run all.
     # Local:
     python3 sweep_lead_engine_v2.py --in "Apify Leads.csv" --outdir ./out
     # With compliance keys set (textable.csv then fills):
     TWILIO_SID=.. TWILIO_TOKEN=.. REALVALIDATION_TOKEN=.. python3 sweep_lead_engine_v2.py --in leads.csv
================================================================================
"""
import os, sys, csv, re, argparse, datetime

try:
    import requests
except ImportError:
    requests = None

# ──────────────────────────────────────────────────────────────────────────────
#  CONFIG  —  edit these (this is the whole targeting brain; change here, not below)
# ──────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # --- compliance ---
    "SUPPRESSION_FILE": "suppression.txt",        # one phone/line = never contact (CO law)
    "TEXTABLE_LINE_TYPES": {"mobile", "nonFixedVoip", "voip"},

    # --- ICP geography (NoCo = our market). Lowercase, no state suffix. ---
    "NOCO_CORE": {                                # full geo points
        "fort collins","loveland","greeley","windsor","timnath","wellington",
        "berthoud","johnstown","evans","severance","laporte","eaton","ault",
        "milliken","mead","pierce","platteville","fort lupton","nunn","gilcrest",
        "kersey","la salle","dacono","frederick","firestone","fort collins/loveland",
    },
    "NOCO_EDGE": {                                # most geo points (NoCo-adjacent)
        "longmont","erie","estes park","fort morgan","sterling","brighton",
        "lyons","niwot","hudson","keenesburg","wiggins",
    },
    "DENVER_METRO": {                             # low geo points (out-of-NoCo, Front Range)
        "denver","aurora","lakewood","arvada","westminster","thornton","centennial",
        "broomfield","littleton","parker","castle rock","commerce city","englewood",
        "wheat ridge","northglenn","boulder","golden","henderson","superior","louisville",
    },
    # everything else in CO (Colorado Springs, Pueblo, Grand Junction, Durango...) = lowest geo

    # --- ICP residential trade categories (Apify categoryName, lowercased substring match) ---
    "CORE_TRADES": {                              # full category points
        "hvac","heating","air conditioning","furnace","plumber","plumbing",
        "electrician","electrical","roofer","roofing","landscaper","landscaping",
        "lawn","gutter","drain","heat pump","duct",
    },
    "EDGE_TRADES": {                              # partial points (adjacent residential)
        "landscape designer","handyman","general contractor","contractor",
        "irrigation","tree service","fence","concrete","painter","painting",
        "garage door","insulation","solar","window","siding","remodel",
    },
    # categories that are NOT our buyer -> hard drop
    "COMMERCIAL_OR_NONTRADE_DROP": {
        "supply store","wholesaler","manufacturer","distributor","store",
        "association","government","school","hospital","apartment","property management",
        "real estate","insurance","bank","restaurant","hotel","auto","car ",
        "equipment rental","hardware store","home improvement store",
    },

    # --- national chains / franchises -> hard drop (lowercase substring match) ---
    "FRANCHISE_DROP": {
        "one hour","ars","service experts","roto-rooter","roto rooter","mr. rooter",
        "mr rooter","benjamin franklin","mister sparky","aire serv","horizon services",
        "bell brothers","precision plumbing","precision air","plumbline","brothers plumbing",
        "done plumbing","fix-it 24/7","fix it 24/7","tipping hat","blue sky","applewood",
        "cooper green","time plus","go direct","sears","home depot","lowe's","lowes",
        "costco","best buy","grainger","ferguson","gulfeagle","abc supply","beacon",
        "lennox","carrier","trane","goodman","rheem","american residential","authority brands",
        "neighborly","wind river environmental","len the plumber","hero","high 5",
        "g&c","one hour heating","swan ",  # Swan = large regional, treat as chain
    },

    # --- Fit-Score weights (max 100) ---
    "W_GEO": 35, "W_TRADE": 30, "W_SIZE": 25, "W_OWNER": 10,

    # --- size band by review count (owner-operated sweet spot). None = column absent. ---
    "REVIEW_SWEET_LOW": 5, "REVIEW_SWEET_HIGH": 150, "REVIEW_CHAIN": 400,

    # --- thresholds ---
    "PURSUE_AT": 75, "HOLD_AT": 50,
    # If set to a number, leads scoring below it are EXCLUDED from callable.csv
    # (still in audit). None = keep everything in callable, just sorted best-first.
    "DROP_CALLABLE_BELOW": None,
}

# ──────────────────────────────────────────────────────────────────────────────
#  PHONE NORMALIZATION  (US -> E.164)   [carried from v1]
# ──────────────────────────────────────────────────────────────────────────────
def to_e164(raw):
    if not raw: return ""
    d = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(d) == 11 and d.startswith("1"): d = d[1:]
    if len(d) != 10: return ""
    if d[0] in "01" or d[3] in "01": return ""     # invalid NANP area/exchange
    return "+1" + d

def ten_digit(e164):
    return e164[2:] if e164.startswith("+1") else e164

# ──────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 1 — internal suppression list  (always on, free)   [from v1]
# ──────────────────────────────────────────────────────────────────────────────
def load_suppression(path):
    s = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                e = to_e164(line)
                if e: s.add(e)
    return s

# ──────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 2 — textable / line-type  (Twilio Lookup v2)   [key-gated, from v1]
#    Returns True (textable), False (landline/etc.), or None (UNVERIFIED).
# ──────────────────────────────────────────────────────────────────────────────
_TW_SID   = os.environ.get("TWILIO_SID", "")
_TW_TOKEN = os.environ.get("TWILIO_TOKEN", "")

def check_textable(e164):
    if not (_TW_SID and _TW_TOKEN and requests):
        return None                                # no key -> UNVERIFIED (fail-closed)
    try:
        r = requests.get(
            f"https://lookups.twilio.com/v2/PhoneNumbers/{e164}",
            params={"Fields": "line_type_intelligence"},
            auth=(_TW_SID, _TW_TOKEN), timeout=20)
        if r.status_code != 200:
            return None
        lti = (r.json() or {}).get("line_type_intelligence") or {}
        lt  = (lti.get("type") or "").strip()
        return (lt in CONFIG["TEXTABLE_LINE_TYPES"]), lt
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
#  COMPLIANCE LAYER 3 — DNC + litigator scrub  (RealValidation)   [key-gated, from v1]
#    Returns "clear" / "dnc" / "litigator" / "UNVERIFIED".
# ──────────────────────────────────────────────────────────────────────────────
_RV_TOKEN = os.environ.get("REALVALIDATION_TOKEN", "")

def check_dnc(e164):
    if not (_RV_TOKEN and requests):
        return "UNVERIFIED"                        # no key -> fail-closed
    try:
        r = requests.get(
            "https://api.realvalidation.com/rpvWebService/DNCLookup.php",
            params={"phone": ten_digit(e164), "token": _RV_TOKEN, "output": "json"},
            timeout=20)
        if r.status_code != 200:
            return "UNVERIFIED"
        j = r.json() or {}
        if str(j.get("litigator", "")).strip().upper() == "Y": return "litigator"
        nat = str(j.get("national_dnc", "")).strip().upper()
        st  = str(j.get("state_dnc", "")).strip().upper()
        if nat == "Y" or st == "Y": return "dnc"
        return "clear"
    except Exception:
        return "UNVERIFIED"

# ──────────────────────────────────────────────────────────────────────────────
#  THE FIT-SCORE / ICP BRAIN  (the new piece in v2)
#  Aligned to TS_M03 rubric: residential trades, ~2-8 trucks, NoCo geo, owner-run.
#  Returns (score 0-100, status, drop_reason or "")
# ──────────────────────────────────────────────────────────────────────────────
def _lc(x): return (x or "").strip().lower()

def fit_score(name, city, state, category, website="", reviews=None, rating=None):
    n, c, cat = _lc(name), _lc(city), _lc(category)

    # ---- HARD DROPS (not scored) ----
    for fr in CONFIG["FRANCHISE_DROP"]:
        if fr in n:
            return 0, "Disqualify", "DROP_FRANCHISE"
    if cat and any(bad in cat for bad in CONFIG["COMMERCIAL_OR_NONTRADE_DROP"]):
        # but never drop if it's clearly a core trade word too
        if not any(t in cat for t in CONFIG["CORE_TRADES"]):
            return 0, "Disqualify", "DROP_COMMERCIAL"
    st0 = _lc(state)
    if st0 and st0 not in ("colorado", "co"):       # ICP is Northern Colorado only
        return 0, "Disqualify", "DROP_OUT_OF_STATE"

    score = 0.0

    # ---- GEO (max W_GEO) ----
    st = _lc(state)
    if c in CONFIG["NOCO_CORE"]:
        score += CONFIG["W_GEO"]
    elif c in CONFIG["NOCO_EDGE"]:
        score += CONFIG["W_GEO"] * 0.80
    elif c in CONFIG["DENVER_METRO"]:
        score += CONFIG["W_GEO"] * 0.35
    elif st in ("", "colorado", "co"):
        score += CONFIG["W_GEO"] * 0.20            # CO but outside our zones / blank city
    else:
        score += 0                                  # out of state

    # ---- TRADE (max W_TRADE) ----
    if any(t in cat for t in CONFIG["CORE_TRADES"]):
        score += CONFIG["W_TRADE"]
    elif any(t in cat for t in CONFIG["EDGE_TRADES"]):
        score += CONFIG["W_TRADE"] * 0.55
    else:
        score += CONFIG["W_TRADE"] * 0.20

    # ---- SIZE / owner-operated sweet spot (max W_SIZE) ----
    if reviews is None or reviews == "":
        score += CONFIG["W_SIZE"] * 0.55           # unknown -> neutral
    else:
        try: rv = int(float(reviews))
        except Exception: rv = -1
        if rv < 0:
            score += CONFIG["W_SIZE"] * 0.55
        elif rv >= CONFIG["REVIEW_CHAIN"]:
            score += CONFIG["W_SIZE"] * 0.20       # too big -> likely chain/commercial
        elif CONFIG["REVIEW_SWEET_LOW"] <= rv <= CONFIG["REVIEW_SWEET_HIGH"]:
            score += CONFIG["W_SIZE"]              # owner-operated sweet spot
        elif rv < CONFIG["REVIEW_SWEET_LOW"]:
            score += CONFIG["W_SIZE"] * 0.50       # very new / thin
        else:
            score += CONFIG["W_SIZE"] * 0.65       # 150-400 reviews: established mid

    # ---- OWNER SIGNAL (max W_OWNER) ----
    owner_pts = 0
    if website: owner_pts += CONFIG["W_OWNER"] * 0.4   # has a real site = legit small biz
    # personal-name / small-shop markers
    if re.search(r"\b(llc|inc|& son|and son|& sons|bros|brothers)\b", n) or \
       re.search(r"^[a-z]+['’]s\b", n) or re.search(r"^[a-z]+ [a-z]+ (heating|plumbing|electric|roofing)", n):
        owner_pts += CONFIG["W_OWNER"] * 0.6
    score += min(owner_pts, CONFIG["W_OWNER"])

    score = round(min(score, 100.0), 1)
    status = ("Pursue" if score >= CONFIG["PURSUE_AT"]
              else "Hold" if score >= CONFIG["HOLD_AT"]
              else "Disqualify")
    return score, status, ""

# ──────────────────────────────────────────────────────────────────────────────
#  CSV INGEST  (flexible Apify column mapping)
# ──────────────────────────────────────────────────────────────────────────────
COLMAP = {
    "name":     ["title","name","business name","company"],
    "phone":    ["phone","phoneunformatted","phone number","number"],
    "city":     ["city"],
    "state":    ["state"],
    "category": ["categoryname","category"],
    "website":  ["website","url","site"],
    "reviews":  ["reviewscount","review_count","reviews","reviewcount"],
    "rating":   ["totalscore","google_rating","rating","stars"],
}

def _pick(headers, keys):
    low = {h.lower().strip(): h for h in headers}
    for k in keys:
        if k in low: return low[k]
    return None

def read_leads(path):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        cols = {k: _pick(r.fieldnames, v) for k, v in COLMAP.items()}
        for row in r:
            rows.append({k: (row.get(col, "") if col else "") for k, col in cols.items()})
    return rows

# ──────────────────────────────────────────────────────────────────────────────
#  MAIN PIPELINE
# ──────────────────────────────────────────────────────────────────────────────
def run(in_path, outdir=".", suppression_file=None, dnc=True, textable=True):
    os.makedirs(outdir, exist_ok=True)
    sup_path = suppression_file or CONFIG["SUPPRESSION_FILE"]
    suppression = load_suppression(sup_path)

    raw = read_leads(in_path)
    stats = dict(total=len(raw), invalid_phone=0, dupe=0, suppressed=0,
                 drop_franchise=0, drop_commercial=0, drop_out_of_state=0, kept=0,
                 pursue=0, hold=0, disqualify=0, textable=0)

    seen = set()
    audit, callable_rows, textable_rows = [], [], []

    for lead in raw:
        name, city, state = lead["name"], lead["city"], lead["state"]
        cat, web, rev = lead["category"], lead["website"], lead["reviews"]
        e = to_e164(lead["phone"])

        rec = {"name": name, "phone_e164": e, "city": city, "state": state,
               "category": cat, "website": web, "reviews": rev,
               "fit_score": "", "fit_status": "", "line_type": "", "textable": "",
               "dnc": "", "suppressed": "", "callable": "no", "send_ready": "no",
               "reason": ""}

        if not e:
            rec["reason"] = "INVALID_PHONE"; stats["invalid_phone"] += 1
            audit.append(rec); continue
        if e in seen:
            rec["reason"] = "DUPLICATE"; stats["dupe"] += 1
            audit.append(rec); continue
        seen.add(e)
        if e in suppression:
            rec["suppressed"] = "yes"; rec["reason"] = "SUPPRESSED"; stats["suppressed"] += 1
            audit.append(rec); continue
        rec["suppressed"] = "no"

        score, status, drop = fit_score(name, city, state, cat, web, rev)
        rec["fit_score"], rec["fit_status"] = score, status
        if drop:
            rec["reason"] = drop
            key = {"DROP_FRANCHISE": "drop_franchise", "DROP_COMMERCIAL": "drop_commercial",
                   "DROP_OUT_OF_STATE": "drop_out_of_state"}.get(drop, "drop_commercial")
            stats[key] += 1
            audit.append(rec); continue

        # ---- compliance scrub (key-gated; fail-closed) ----
        rec["dnc"] = check_dnc(e) if dnc else "SKIPPED"
        tx = check_textable(e) if textable else None
        if isinstance(tx, tuple):
            is_tx, lt = tx
        else:
            is_tx, lt = None, ""
        rec["line_type"] = lt or ("UNVERIFIED" if is_tx is None else "")
        rec["textable"] = ("yes" if is_tx is True else "no" if is_tx is False else "UNVERIFIED")

        # CALLABLE: valid, deduped, not suppressed, ICP-survivor. (Call-first phase.)
        below = CONFIG["DROP_CALLABLE_BELOW"]
        if below is not None and score < below:
            rec["reason"] = f"BELOW_CALLABLE_CUTOFF_{below}"
            audit.append(rec); continue
        rec["callable"] = "yes"
        stats["kept"] += 1
        stats[status.lower()] += 1
        callable_rows.append(rec)

        # SEND-READY (textable.csv): mobile/VoIP AND DNC-clear. Fail-closed.
        if rec["textable"] == "yes" and rec["dnc"] == "clear":
            rec["send_ready"] = "yes"; stats["textable"] += 1
            textable_rows.append(rec)
        else:
            rec["reason"] = (rec["reason"] or
                             f"NOT_SENDREADY(textable={rec['textable']},dnc={rec['dnc']})")
        audit.append(rec)

    # sort callable BEST-FIRST
    callable_rows.sort(key=lambda r: (r["fit_score"] if isinstance(r["fit_score"], (int,float)) else 0),
                       reverse=True)
    textable_rows.sort(key=lambda r: (r["fit_score"] if isinstance(r["fit_score"], (int,float)) else 0),
                       reverse=True)

    _write(os.path.join(outdir, "callable.csv"), callable_rows,
           ["name","phone_e164","city","state","category","fit_score","fit_status",
            "website","reviews","dnc"])
    _write(os.path.join(outdir, "textable.csv"), textable_rows,
           ["name","phone_e164","city","state","category","fit_score","fit_status",
            "line_type","dnc"])
    _write(os.path.join(outdir, "audit.csv"), audit,
           ["name","phone_e164","city","state","category","fit_score","fit_status",
            "callable","textable","line_type","dnc","suppressed","send_ready","reason"])

    _runlog(outdir, in_path, stats, sup_path, len(suppression))
    return stats

def _write(path, rows, cols):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(cols)
        for r in rows: w.writerow([r.get(c, "") for c in cols])

def _runlog(outdir, in_path, s, sup_path, sup_n):
    tw = "ON" if (_TW_SID and _TW_TOKEN) else "OFF (textable=UNVERIFIED, fail-closed)"
    rv = "ON" if _RV_TOKEN else "OFF (DNC=UNVERIFIED, fail-closed)"
    lines = [
        "THE SWEEP — LEAD ENGINE v2 — RUN LOG",
        f"  When            : {datetime.datetime.now():%Y-%m-%d %H:%M}",
        f"  Input           : {in_path}",
        f"  Suppression     : {sup_n} numbers ({sup_path})",
        f"  Twilio textable : {tw}",
        f"  RealValidation  : {rv}",
        "  " + "-"*52,
        f"  {s['total']:>6} leads in",
        f"  {s['invalid_phone']:>6} dropped — invalid phone",
        f"  {s['dupe']:>6} dropped — duplicate",
        f"  {s['suppressed']:>6} dropped — suppression list",
        f"  {s['drop_franchise']:>6} dropped — national franchise/chain",
        f"  {s['drop_commercial']:>6} dropped — commercial/non-trade category",
        f"  {s['drop_out_of_state']:>6} dropped — out of state (non-CO)",
        f"  {s['kept']:>6} KEPT (callable, ICP-scored)",
        f"           ├─ Pursue (75+) : {s['pursue']}",
        f"           ├─ Hold (50-74) : {s['hold']}",
        f"           └─ Disqualify   : {s['disqualify']}",
        f"  {s['textable']:>6} SEND-READY mobiles (textable.csv)  [0 until 10DLC + keys]",
        "  " + "-"*52,
        "  callable.csv  -> dialers load TODAY (sorted best-first)",
        "  textable.csv  -> held for 10DLC SMS launch (fail-closed)",
        "  audit.csv     -> every lead + why it passed/dropped",
        "  Re-scrub DNC before every SMS send — status changes daily.",
    ]
    out = "\n".join(lines)
    print(out)
    with open(os.path.join(outdir, "run_log.txt"), "w", encoding="utf-8") as f:
        f.write(out + "\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="The Sweep Lead Engine v2")
    ap.add_argument("--in", dest="inp", required=True, help="Apify scrape CSV")
    ap.add_argument("--outdir", default="out", help="output folder")
    ap.add_argument("--suppression", default=None, help="suppression.txt path")
    args = ap.parse_args()
    run(args.inp, args.outdir, args.suppression)
