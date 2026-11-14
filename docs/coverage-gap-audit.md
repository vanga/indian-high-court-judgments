# Coverage-gap audit (June 2026)

## Why this exists

Users reported judgments that are absent from this dataset but present on a
High Court's own website (e.g. *Deepak Bahry vs Heart And Soul Entertainment*,
Bombay HC, WP 8390/2009, decided 7 Jan 2026, neutral citation `2026:BHC-AS:827`
— on the Bombay HC site and Indian Kanoon, not on the dataset).

This pipeline scrapes a **single source**: `judgments.ecourts.gov.in`
(`download.py`). The dataset is therefore exactly as complete as that one portal.
This audit quantifies where that source has gaps.

Reproduce with `python audit_coverage.py` (reads parquet footers from the public
S3 bucket; no credentials, a few hundred range requests).

## Headline finding

**The aggregate is healthy; specific courts have gone dark, and the gaps are
masked by redistribution.** Total volume per decision-year is flat (STATS.md "By
Year", from the tar index): 2023 ≈ 1.79M → 2024 ≈ 1.38M → 2025 ≈ 1.43M. So at
the all-courts level nothing looks wrong. But underneath, several courts/benches
collapsed (Madhya Pradesh, Kerala, Gujarat, Bombay's Mumbai seat) while others
grew (Madras, Delhi, Chhattisgarh), netting out to ~flat. The healthy headline
is exactly why these holes went unnoticed.

The gap is therefore **not uniform** — it is concentrated in specific
courts/benches that stopped (or sharply reduced) publishing to the central
portal, mostly at clean calendar-year boundaries. Other large courts (Madras,
Delhi, Punjab & Haryana, Karnataka, Calcutta, Patna) remain healthy through 2025.
A single scraper bug could not produce that court-by-court pattern, so the likely
dominant cause is **source-side coverage loss** (courts moving to their own
portals / neutral citations), not the scraper missing data the portal still
serves — but see the confound below; this is not yet proven.

### Aggregate by decision-year (STATS.md, from the tar index)

| Year | Metadata records |
|---|--:|
| 2021 | 1,209,520 |
| 2022 | 1,724,305 |
| 2023 | 1,794,281 (peak) |
| 2024 | 1,379,829 |
| 2025 | 1,427,623 |
| 2026* | 408,988 (partial) |

Roughly offsetting moves, 2023 → 2025: **down** — Allahabad −110k (JSON),
MP −121k, Kerala −77k, Rajasthan −70k, Bombay-Mumbai-seat/Orissa/Andhra;
**up** — Delhi +42k, Madras +41k, Chhattisgarh +19k, Himachal +15k.

### A note on counting (parquet vs JSON)

Counts below are parquet `num_rows`. For most courts this matches the raw JSON
object count exactly (MP, Kerala, Bombay verified identical). **Exception:**
parquet can *overcount* where re-scrapes produced duplicate rows — Allahabad 2023
reads 532k in parquet but only 313k as JSON objects. JSON object counts are
authoritative; the collapse findings (MP, Kerala, Bombay-AS) hold under both.

## Judgments per court by decision-year (all benches)

`2026` is partial (data to ~June). The flag compares the last complete year
(2025) against each court's peak year.

| Court | Name | 2022 | 2023 | 2024 | 2025 | 2026* | 2025/peak |
|---|---|--:|--:|--:|--:|--:|--:|
| 23_23 | Madhya Pradesh | 100,143 | 121,072 | 12,293 | **184** | 71 | **0.2%** ⛔ |
| 32_4 | Kerala | 76,737 | 83,859 | 45,796 | **6,818** | 52 | **8.1%** ⛔ |
| 24_17 | Gujarat | 68,450 | 4,513 | 1,760 | **7,473** | 3,874 | **10.9%** ⛔ |
| 9_13 | Allahabad | 524,553 | 532,089 | 90,426 | 202,345 | 8,250 | 38.0% ⚠️ |
| 8_9 | Rajasthan | 93,383 | 119,187 | 91,384 | 48,884 | 15,929 | 41.0% ⚠️ |
| 28_2 | Andhra Pradesh | 46,038 | 99,768 | 56,751 | 59,201 | 20,408 | 59.3% |
| 27_1 | Bombay | 101,103 | 111,187 | 84,740 | 70,486 | **369** | 63.4% † |
| 21_11 | Orissa | 116,446 | 111,641 | 90,078 | 75,365 | 31,454 | 64.7% |
| 16_20 | Tripura | 2,433 | 2,242 | 2,149 | 1,601 | 893 | 65.8% |
| 36_29 | Telangana | 71,594 | 66,857 | 38,931 | 50,795 | 8,876 | 70.9% |
| 19_16 | Calcutta | 82,498 | 85,323 | 68,643 | 69,210 | 26,618 | 81.1% |
| 10_8 | Patna | 117,116 | 135,913 | 123,106 | 112,625 | 44,176 | 82.9% |
| 20_7 | Jharkhand | 44,410 | 46,010 | 46,520 | 40,974 | 16,504 | 88.1% |
| 1_12 | Jammu & Kashmir | 14,442 | 12,700 | 10,960 | 13,532 | 4,506 | 93.7% |
| 3_22 | Punjab & Haryana | 133,062 | 149,388 | 154,801 | 145,495 | 65,170 | 94.0% |
| 17_21 | Meghalaya | 1,497 | 1,476 | 1,446 | 1,414 | 705 | 94.5% |
| 18_6 | Gauhati | 26,000 | 27,799 | 27,102 | 26,470 | 11,237 | 95.2% |
| 11_24 | Sikkim | 156 | 148 | 131 | 150 | 56 | 96.2% |
| 29_3 | Karnataka | 74,702 | 80,102 | 91,855 | 91,473 | 31,601 | 99.6% |
| 7_26 | Delhi | 5,024 | 8,465 | 50,462 | 50,414 | 22,746 | 99.9% |
| 33_10 | Madras | 174,573 | 181,272 | 177,560 | 221,936 | 77,461 | 100% |
| 22_18 | Chhattisgarh | 32,216 | 42,179 | 50,880 | 61,311 | 22,496 | 100% |
| 2_5 | Himachal Pradesh | 16,501 | 20,635 | 27,911 | 36,132 | 14,881 | 100% |
| 5_15 | Uttarakhand | 14,922 | 14,570 | 14,710 | 16,794 | 6,899 | 100% |
| 14_25 | Manipur | 2,970 | 1,885 | 4,334 | 2,678 | 1,032 | 61.8% |

⛔ near-total collapse  ⚠️ severely degraded  † court total hides a bench-level collapse (see below)

## Bench-level detail for the worst cases

**Bombay (27_1)** — the collapse is the *Mumbai principal seat only*; the
mofussil benches stayed healthy through 2025, which is why the court total looks
merely "degraded". This is the bench that issues `2026:BHC-AS:*` citations.

| bench | 2022 | 2023 | 2024 | 2025 | 2026* |
|---|--:|--:|--:|--:|--:|
| newas (Mumbai Appellate Side) | 34,284 | 41,217 | 20,744 | **265** | 78 |
| newos (Mumbai Original Side) | 6,747 | 4,156 | 1,841 | **34** | 10 |
| hcaurdb (Aurangabad) | 37,060 | 39,468 | 35,507 | 40,561 | 86 |
| kolhcdb (Kolhapur) | 7,232 | 7,777 | 10,090 | 10,603 | 7 |
| hcbgoa (Goa) | 1,696 | 2,432 | 1,805 | 1,399 | 0 |

**Madhya Pradesh (23_23)** — all three benches fall off a cliff at the start of
2024 and are essentially gone by 2025.

| bench | 2022 | 2023 | 2024 | 2025 | 2026* |
|---|--:|--:|--:|--:|--:|
| mphc_db_jbp (Jabalpur) | 53,408 | 64,497 | 6,119 | 111 | 52 |
| mphc_db_ind (Indore) | 25,398 | 31,334 | 4,039 | 38 | 12 |
| mphc_db_gwl (Gwalior) | 21,337 | 25,241 | 2,135 | 35 | 7 |

**Allahabad (9_13)** — wildly irregular: the main bench lost almost all of 2024
(444k → 3.4k), partly recovered in 2025, and the Lucknow bench fell off in 2026.
(These are parquet figures and overcount via duplicates — JSON object counts are
313k/90k/202k for 2023/24/25. Still a real dip in 2024, but not a "collapse".)

| bench | 2022 | 2023 | 2024 | 2025 | 2026* |
|---|--:|--:|--:|--:|--:|
| cisdb_16012018 (main) | 441,735 | 443,845 | 3,355 | 118,717 | 8,021 |
| cishclko (Lucknow) | 82,818 | 88,244 | 87,071 | 83,628 | 229 |

## The 2026 cliff

Many courts that were healthy through 2025 show a near-zero 2026 — including
benches with no prior gap (Bombay Aurangabad 40,561 → 86; Allahabad Lucknow
83,628 → 229; Kolhapur 10,603 → 7). Yet other courts have substantial 2026 data
(Madras 77k, P&H 65k, Karnataka 32k). So the 2026 cliff is **court-specific**,
not a global sync outage — and the portal-vs-index reconciliation (below) confirms
it is the portal that lacks the data, not our scrape that missed it.

## Cause: confirmed source-side (Gap A), not a scraper miss

The two candidate causes were:

1. **Source absence (Gap A):** the portal genuinely no longer has these
   judgments → the only fix is adding the courts' own websites as a source.
2. **Scraper miss (Gap B):** the portal still serves them but our scrape
   stalled/errored → fix would be a backfill/re-scrape pass.

**It is Gap A.** This is settled by the maintainer's own portal-vs-index
reconciliation, `docs/portal_vs_s3_index_after_remaining_gap_retry_*.csv`
(2026-06-01), whose `portal_total` column is the count the eCourts portal *itself*
reports per court/year. Our index matches it everywhere (we usually hold slightly
more, from dedup):

| Court (2025) | portal_total | our index | gap |
|---|--:|--:|--:|
| Madhya Pradesh (23_23) | 177 | 184 | ~0 |
| Kerala (32_4) | 6,793 | 6,818 | ~0 |
| Bombay (27_1, all benches) | 68,854 | 71,172 | ~0 |
| Madras (33_10) | 224,416 | 223,637 | ~0 |

The portal's own MP-2025 total is **177**. We are not under-scraping — the data
is absent at the source. Three independent measurements agree on the MP-2025
figure: this audit (184), the GitHub issue #28 CSV from @rahulraj80 (177), and
`portal_total` (177). Backfill/re-scrape cannot help; the "remaining gap retry"
run already drove the residual to ~zero.

> Caveat on the aggregate aside: counts above are JSON/index-based. The parquet
> `num_rows` used in the tables higher up overcounts where re-scrapes left
> duplicate rows (Allahabad 2023 reads 532k in parquet vs ~314k by index, and
> portal_total 319,947). Trust index/JSON counts over parquet row counts.

## Recommended next steps

The only lever left is external sources — the central portal does not have these
judgments.

1. **Fix Gap A** (the real coverage fix): per-court website scrapers, prioritised
   by gap size — Madhya Pradesh, Kerala, Bombay (Mumbai seat), Gujarat first.
   Many NIC court sites share a template, so one scraper can cover several courts.
2. **De-prioritise `hcservices.ecourts.gov.in`** as a bulk source: case-oriented,
   captcha-heavy, and independently reported as error-prone/incomplete.
3. **Document the source limitation** for users (README / dataset docs): the
   dataset mirrors `judgments.ecourts.gov.in`, which several High Courts have
   stopped feeding for recent years — so recent coverage for those courts is
   inherently partial until court-site ingestion is added. This directly answers
   issue #28.
4. **No backfill/Gap-B work needed** — reconciliation shows the scrape is already
   complete against the portal.
