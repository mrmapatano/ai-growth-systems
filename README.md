# AI Growth Systems

Production AI systems I built to run [Mapatano & Company](https://mapatanoandcompany.com), a B2B growth agency serving owner-operated trades businesses — and to power the fulfillment layer of the [Pre-Skilling Institute](https://www.linkedin.com/in/abraham-mapatano), a nonprofit workforce institute I founded at Colorado State University.

These aren't demos. They run the business: sourcing leads, scrubbing them for compliance, AI-researching every prospect, and feeding a live SDR team.

**Results to date:** 7,781-contact verified owner database built and maintained · 4.07% SMS reply rate validated in live testing · ~12,000 AI research requests/day throughput · 85% of agency fulfillment automated.

---

## The pipeline

```
Google Maps scrape (Apify)
        │
        ▼
sweep_leadgen.py / sweep_lead_engine_v2.py     ← lead engine
        │  normalize → dedupe → suppression → ICP fit-score (0–100)
        │  → line-type check (Twilio Lookup) → DNC scrub (Fed + CO + litigator)
        ▼
  callable.csv / textable.csv / audit.csv      ← compliance-proven lists
        │
        ▼
smart_drop_turbo.py                            ← AI personalization engine
        │  per-lead web research + message generation (Gemini)
        ▼
  SMS outreach (Smarter Contact) → GoHighLevel CRM → SDR team
```

## The systems

### `smart_drop_turbo.py` — AI personalization engine
Multi-threaded Python engine that researches every prospect and writes a personalized opener at scale.

- **Parallel API key rotation** across up to 8 Gemini accounts (~12,000 requests/day on free tier — $0 marginal cost)
- Thread-safe client pool with dead-key detection and automatic failover
- Crash-safe **resume system** (JSON progress file) — restartable mid-run at any point
- Unicode normalization layer at all data entry/exit points (curly quotes were silently corrupting JSON payloads; fixed at the boundary, not case-by-case)

### `sweep_lead_engine_v2.py` — ICP scoring + compliance gate
Turns a raw scrape into two trustworthy lists: numbers we can **call today** and numbers we can **text** once carrier registration clears. Every lead passes a gate:

- E.164 normalization → dedupe → internal suppression list
- **Fit-Score "brain"**: hard-drops national franchises and non-trade categories, then scores 0–100 on geography, residential-trade signal, size, and owner signal (Pursue 75+ / Hold 50–74 / Disqualify <50)
- Twilio Lookup line-type check (mobile/VoIP only for SMS)
- DNC scrub — Federal + Colorado + litigator lists
- Writes an `audit.csv` recording why every single lead passed or was dropped — proof of scrub, by design

Compliance isn't a bolt-on here: the system was architected so a non-compliant number can never reach the team.

### `sweep_leadgen.py` — v1 end-to-end pipeline
The original single-file pipeline: Apify scrape → clean → scrub → AI research → personalized cold text. v2 split scoring into its own brain; v1 is kept for the integrated scrape-to-message path.

## Also built (private)

- **LeadOps MVP** — Flask CRM-lite (SQLite, importers, test suite) for lead operations; private because it holds live pipeline data
- **N8N + GoHighLevel automation stack** — 7 workflows handling client onboarding, review generation, missed-call text-back, and reporting
- **Claude agent workflows** — custom skill library orchestrating both ventures' operations

## Stack

Python (threading, concurrent.futures) · Gemini API · Apify · Twilio Lookup · RealValidation · GoHighLevel · N8N · Smarter Contact · Apollo.io

---

**Abraham Mapatano** · Economics (Honors), Colorado State University '27 · [LinkedIn](https://www.linkedin.com/in/abraham-mapatano) · mr.mapatano16@gmail.com
