# Filling the coverage gap: alternative sources research (June 2026)

Follow-up to `coverage-gap-audit.md`, which established that several High Courts
(Madhya Pradesh, Kerala, Gujarat, Bombay's Mumbai seat) stopped publishing recent
judgments to `judgments.ecourts.gov.in` — the gap is **source-side**, so the only
fix is a new source. This doc evaluates the two candidates for an **all-25-HC**
solution.

## The two candidates

1. **`hcservices.ecourts.gov.in`** — the eCourts High Court "case status / services"
   portal. One central system covering all HCs.
2. **Individual High Court websites** — each court's own portal
   (e.g. `bombayhighcourt.nic.in`), where the missing judgments demonstrably live.

## Key correction to earlier assumptions

The audit doc had de-prioritised hcservices as "case-oriented". That was wrong on
one decisive point: **hcservices supports date-range enumeration** via *Court
Orders → Search by Order Date* (`cases/s_orderdate.php`, backing query
`cases/s_orderdate_qry.php`, params `from_date`/`to_date`). No case numbers
needed — the same date-window model `download.py` already uses on the judgments
portal. It also exposes a first-class **neutral-citation search**
(`cases/s_citation_no.php`) — which the judgments portal does *not* carry.

## Comparison

| Dimension | hcservices.ecourts.gov.in | Individual HC websites |
|---|---|---|
| **Scrapers needed for all 25 HCs** | **1** (parameterised by `state_cd`/`court_code`) | **~12–16** distinct (no shared judgment-search template, even within NIC) |
| **Date-range enumeration** | Yes — `s_orderdate.php` (all benches, all courts) | Most majors yes (Bombay, Delhi, Madras, MP, Kerala, Calcutta, P&H, Karnataka); **Gujarat = no** (case-no/citation only); **Allahabad eLegalix = per-document security code**, resists bulk |
| **Has the missing data?** | **Very likely** — reads the same case-management DB the courts' own NIC sites serve from; **but not yet proven live** (CAPTCHA-gated) | **Confirmed** — the test PDF (BHC-AS:827) is served straight from `bombayhighcourt.nic.in` |
| **Neutral citation** | Yes (`s_citation_no.php`, indexed field) | Yes on most courts (Bombay `BHC-AS`, Delhi `DHC`, etc.) |
| **CAPTCHA** | Image, 6-char alphanumeric, **per search**; ~75% solve with `ddddocr`/ONNX. Needs a **new** solver (not the arithmetic one we have) | Varies: **~6 courts none** (MP, Himachal, P&H, Madras, Delhi, partly Calcutta); rest image CAPTCHAs of differing types |
| **Speed / politeness** | Serial norm, 1s+/req, 30–60s tail latencies; full-corpus sweep is very long | No-CAPTCHA courts are fast; others comparable |
| **PDF + metadata** | PDF via `display_pdf.php`; CNR, parties, dates, judge | Direct PDFs; rich metadata + neutral citations |
| **Maintenance** | **Low** — one codebase, one CAPTCHA model | **High** — N codebases that drift independently |
| **Prior art** | `openjustice-in/ecourts`, `bharat-courts` (single-threaded, OCR CAPTCHA) — usable references | Rare, shallow, mostly case-lookup; only `bharat-courts` does one site directly (Calcutta, "better PDF coverage than eCourts") |
| **Main risk** | Live completeness for the *dark* courts unproven; CAPTCHA + slow | Per-site fragility; Gujarat/Allahabad hard; N-scraper sprawl |

## Verdict

**For the all-HC goal, `hcservices` is the better backbone.** It is the only
option that is (a) one uniform scraper for all 25 courts, (b) date-range
enumerable like our existing pipeline, (c) neutral-citation searchable, and
(d) fed from the same case DB the court sites use, so it should contain the
recent judgments the judgments portal dropped. The cost is a new image-CAPTCHA
solver and slow serial scraping — bounded, well-trodden work (two reference
implementations exist).

**Individual court sites are the ground truth and win on recency, but cost
~12–16 scrapers** with no template reuse, several hard cases (Gujarat, Allahabad,
Karnataka), and almost no reusable prior art. They are best used *selectively*,
not as the primary all-HC mechanism.

### Recommended architecture (hybrid, hcservices-first)

1. **Primary new source: hcservices order-date sweep** for the dark courts
   (MP, Kerala, Gujarat, Bombay-Mumbai-seat), then extend to all 25 as a
   freshness/backstop layer. One scraper, parameterised per court.
2. **Selective court-site scrapers** only where they add clear value:
   the CAPTCHA-free easy wins (MP `/judgements`, Calcutta, P&H, Himachal) and
   for **neutral-citation enrichment** of every record.
3. **Keep** the existing `judgments.ecourts.gov.in` pipeline as the baseline for
   the courts that still publish there (Madras, Delhi, P&H, Karnataka, …).

## The one make-or-break test before building anything

Everything above hinges on an **unproven** assumption: that hcservices actually
*has* the judgments the judgments portal is missing for the dark courts. Both are
eCourts properties; if Bombay stopped feeding *all* central eCourts systems, the
order-date sweep won't help and we'd be forced onto court sites.

**Proof-of-concept (small, decisive):** build a minimal hcservices client (order-
date query + image-CAPTCHA solver) and run it for:
- Bombay **Appellate Side**, decision dates Jan 2026 — does it return ~thousands
  of orders incl. WP 8390/2009 / `2026:BHC-AS:827`? (judgments portal had ~78 for
  all of 2026)
- Madhya Pradesh, 2025 — does it return ≫177?

If hcservices returns the data the portal lacks → it is the all-HC answer, build
it out. If it is *also* sparse → pivot to court-site scrapers (starting with the
CAPTCHA-free courts). Either way this PoC settles the direction cheaply.

## Reference endpoints

- Order-date enumeration: `…/ecourtindiaHC/cases/s_orderdate.php` → `s_orderdate_qry.php` (`from_date`,`to_date`,`showRecords`)
- Neutral-citation search: `…/ecourtindiaHC/cases/s_citation_no.php`
- PDF: `…/ecourtindiaHC/cases/display_pdf.php?filename=…`
- Uniform court selector: `…/ecourtindiaHC/index_highcourt.php?state_cd=<N>&court_code=<M>` (note: hcservices `state_cd`/`court_code` differ from this repo's `27~1` scheme — a mapping table is required)
- Prior art: `github.com/openjustice-in/ecourts`, `github.com/iamshouvikmitra/bharat-courts`
