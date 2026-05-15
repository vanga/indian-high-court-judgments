# eCourts/Dataset Count Discrepancy Notes

Investigation date: 2026-05-07.

## Current working hypothesis

The gap is not one single issue.

1. Some windows have raw metadata JSON in S3 that was never added to metadata
   tar/index/parquet. In at least one PHHC 2025 sample, most of those loose
   metadata records also lack corresponding PDF objects/data index entries.
2. Some windows are genuinely missing from raw metadata too, so re-scraping can
   still find new rows.
3. Existing aggregate S3 drift confirms parquet/index lag is real but too small
   to explain the whole portal-vs-dataset gap.

## Portal date filter semantics

Probe script: `scripts/probe_portal_date_semantics.py`

PHHC one-day check:

```text
court=3~22
date=2025-02-20
portal_total=692
rows_fetched=692
parsed_decision_dates=692
inside_decision_date_window=692
outside_decision_date_window=0
decision_date_counts={2025-02-20: 692}
```

PHHC weekly first-page check:

```text
court=3~22
date=2025-02-20..2025-02-26
portal_total=3069
rows_fetched=1000
inside_decision_date_window=1000
outside_decision_date_window=0
decision_date_counts_on_first_page={2025-02-25: 1000}
```

Interpretation: for these sampled rows, the portal date filter matches the
`Decision Date` parsed from result-row HTML, not filename/upload date. This is
evidence, not a universal proof for every court/query.

## Aggregate 2025/2026 S3 layer counts

From EC2 S3 layer audit across all courts for years 2025 and 2026:

```text
portal_2025=1411878
portal_2026=330192
portal_combined=1742070

metadata_json_objects=1338346
data_pdf_objects=1324128
metadata_index_file_count=1314433
data_index_file_count=1324059
parquet_rows=1311609

raw_json_minus_parquet=26737
raw_pdf_minus_data_index=69
raw_json_minus_metadata_index=23913
portal_minus_raw_json=403724
portal_minus_parquet=430461
```

Interpretation: internal raw JSON to parquet drift explains only about 26.7k
rows of a roughly 430k recent-year portal-vs-parquet gap. A large part of the
gap must be missing before raw JSON, or distributed in layers not captured by
simple year totals.

## Controlled PHHC window

Window: PHHC `3_22`, bench `phhc`, `2025-02-20..2025-02-26`.

Portal daily counts:

```text
2025-02-20  692
2025-02-21  472
2025-02-24  705
2025-02-25  1200
2025-02-26  0
portal_week_total=3069
```

S3 parquet:

```text
window_rows=1293
2025-02-20  33
2025-02-21  15
2025-02-24  90
2025-02-25  1155
```

Metadata tar/index:

```text
tar_members_seen=117762
window_metadata_tar_json=1317
2025-02-20  45
2025-02-21  18
2025-02-24  95
2025-02-25  1159
```

Loose raw metadata not in metadata index:

```text
raw_json_objects=126938
metadata_index_distinct=117511
loose_raw_json_not_in_metadata_index=9427
window_loose_raw_json=1770
window_loose_downloaded_counts={'True': 1770}
window_loose_missing_pdf_object=1766
window_loose_missing_data_index=1766
2025-02-20  679
2025-02-21  463
2025-02-24  624
2025-02-25  4
```

Interpretation: portal total `3069` is nearly explained by
`metadata_tar_json 1317 + loose_raw_json 1770 = 3087`. The controlled PHHC gap
is mostly loose raw metadata not packed into tar/index/parquet, and almost all
of those loose records have missing PDFs in S3 despite `downloaded: true` in the
metadata JSON.

Sample loose metadata object:

```text
metadata/json/year=2025/court=3_22/bench=phhc/PHHC010009602025_1_2025-02-20.json
LastModified=2025-10-26T02:44:59Z
pdf_link=court/cnrorders/phhc/orders/PHHC010009602025_1_2025-02-20.pdf
downloaded=true
corresponding data/pdf object missing in years 2023..2026
```

## Controlled Madras window

Window: Madras `33_10`, `2025-09-23`.

Portal:

```text
portal_total=1799
first_page_rows=1000
```

S3 parquet:

```text
window_rows=1408
```

Metadata tar/index:

```text
bench=hc_cis_mas window_metadata_tar_json=1054
bench=mdubench   window_metadata_tar_json=354
combined_metadata_tar_json=1408
```

Loose raw metadata not in metadata index:

```text
raw_json_objects=168001
metadata_index_distinct=167952
loose_raw_json_not_in_metadata_index=49
window_loose_raw_json=49
window_loose_downloaded_counts={'True': 49}
window_loose_missing_pdf_object=0
window_loose_missing_data_index=0
```

Interpretation: Madras `2025-09-23` has about `1799 - (1408 + 49) = 342`
rows missing even from raw metadata. This is a true scrape coverage gap, not
just parquet/index drift.

## Scraper behavior observed

Old behavior silently treated terminal `session_expire` / API search failures
as empty result pages. Patch in `download.py` now raises/retries task-level
failures so these windows become visible instead of logging `results=0`.

Example from earlier:

```text
Madras 2025-01-25..2025-01-31 old summary: pages=0 results=0 session_expire=4
Live portal for same window: 4966
Targeted retry after patch: pages=5 results=4966 downloaded=1 skip_s3=4965
```

## Active remediation test

Started on EC2:

```bash
./.venv/bin/python download.py \
  --court_code 33_10 \
  --start_date 2025-09-23 \
  --end_date 2025-09-23 \
  --day_step 1 \
  --max_workers 1 \
  --sync-s3 \
  --compress-pdfs
```

Observed so far:

```text
attempts 1 and 2 failed with terminal session_expire
attempt 3 succeeded search
task summary: pages=2 results=1799 downloaded=25 skip_s3=913 skip_local=150
session_refreshes=1 session_expire=2
```

Upload/sync completed.

Post-run observed output:

```text
hc_cis_mas new PDF files=656
mdubench   new PDF files=139
total new PDF files=795

hc_cis_mas parquet: 109980 existing + 77702 new - 77046 duplicates = 110636
mdubench parquet:   57972 existing + 42595 new - 42456 duplicates = 58111
```

Post-run re-counts:

```text
parquet window_rows=1583
metadata tar hc_cis_mas=1214
metadata tar mdubench=369
metadata tar combined=1583
loose raw JSON still=49
portal_total=1799
residual_missing_from_raw=1799 - (1583 + 49) = 167
residual_missing_from_tar/parquet=1799 - 1583 = 216
```

Note: the earlier rough residual of 216 was relative to tar/parquet only. When
loose raw metadata is included, residual missing from raw metadata for this day
is 167.

Updated at: 2026-05-07T08:12:55Z.

## Scraper session-refresh pagination bug

Observed during repeated Madras `2025-09-23` passes:

Before the patch, after the scraper downloaded 25 fresh PDFs on page 2, it
created a new portal session but kept `iDisplayStart=1000`. The portal appears
to tie pagination state to the session, so page 2 under a new session can return
empty/no useful results. This caused one run to stop after only one 25-download
batch.

Patch in `download.py`:

```text
After a 25-download session refresh:
  sEcho = 1
  iDisplayStart = 0
  app_token = refreshed token
```

This preserves the intended design: restart from the first 1000-row search page
after a session refresh, skip rows already present in S3/local cheaply, and then
continue to the missing rows.

Tests:

```text
local: ./.venv/bin/python -m unittest tests.test_download_s3_sync
result: Ran 8 tests, OK

EC2: ./.venv/bin/python -m unittest tests.test_download_s3_sync
result: Ran 8 tests, OK
```

Patched Madras validation run:

```text
court=33_10 date=2025-09-23
task summary: pages=4 results=3598 downloaded=28 skip_s3=3541 skip_local=25
session_refreshes=1 session_expire=2 api_errors=1
```

The duplicated `results=3598` is expected after restarting pagination: it
processed the same 1,799 live rows twice, skipping already-present rows on the
second pass.

## Madras `2025-09-23` final comparison

After patched run and S3 sync:

```text
live portal total=1799
portal rows fetched for comparison=1799
parquet rows for decision_date=2025-09-23: 1809
parquet unique pdf_link=1809
parquet unique cnr=1801
portal unique pdf=1799
portal unique cnr=1799
missing parquet pdf count=0
extra parquet pdf count=10
duplicate parquet CNR groups=8
```

Interpretation: the live portal rows are fully covered in parquet for this
window. The remaining +10 parquet overcount is mostly historical duplicate CNRs
with multiple PDF filenames for the same parsed decision date, e.g. one current
`2025-09-23` file and another later portal file-date for the same CNR.

Updated at: 2026-05-07T08:24:32Z.

## Madras 2025 full-year proof run

Baseline before the full-year proof run:

```text
court=33_10
window=2025-01-01..2025-12-31
live portal total=224396
S3 parquet rows before run=168973
gap before run=55423
```

EC2 proof run:

```text
cd /opt/ihcj/repo
./.venv/bin/python download.py \
  --court_code 33_10 \
  --start_date 2025-01-01 \
  --end_date 2025-12-31 \
  --day_step 1 \
  --max_workers 2 \
  --sync-s3 \
  --compress-pdfs
```

The first pass is intentionally allowed to complete even when individual
date-tasks fail after retries, because successful tasks must still be synced to
S3. Failed dates are collected and reported after S3 sync, then rerun.

During the first pass, several dates still hit terminal `session_expire` after
3 retries. The retry path was further hardened so `session_expire` now creates a
fresh portal session before solving captcha and refreshing `app_token`; this is
expected to apply to the failed-date rerun, not to the already-running process.

Tests after the patch:

```text
local: ./.venv/bin/python -m unittest tests/test_download_s3_sync.py
result: Ran 9 tests, OK

EC2: ./.venv/bin/python -m unittest tests/test_download_s3_sync.py
result: Ran 9 tests, OK
```

Interim observation while the first pass was active: local Madras files were
still increasing and the run was processing October 2025, so it was not stuck.
The final proof needs these follow-ups:

```text
1. Let the first pass sync S3.
2. Rerun only collected failed dates with the fresh-session retry patch.
3. Recount S3 parquet for 2025.
4. Compare against live portal total=224396.
5. If counts are close, run row-level portal-vs-parquet comparison for at least
   selected monthly windows, and optionally the full year.
```

Updated at: 2026-05-07T09:15:00Z.

## Madras 2025 full-year result

Final state after the EC2 primary run, failed-date follow-up, final S3 sync, and
parquet recount:

```text
court=33_10
window=2025-01-01..2025-12-31

portal total before run=224396
portal total checked after run=224406

S3 parquet before run=168973
S3 parquet after run=212448

closed gap=212448 - 168973 = 43475
remaining gap vs latest portal=224406 - 212448 = 11958
```

S3 sync effects:

```text
primary pass:
  hc_cis_mas new PDF files=8332
  mdubench new PDF files=4720
  total=13052

follow-up/final sync:
  hc_cis_mas new PDF files=18848
  mdubench new PDF files=11575
  total=30423

total parquet increase=43475
```

Failure behavior:

```text
primary pass failed date tasks=75
follow-up reruns still returned non-zero for 36 dates
```

Interpretation: hidden/ignored search/session failures are a major contributor
to the discrepancy. The full-year run recovered roughly 43.5k rows for Madras
2025 alone. It did not completely close the gap; remaining difference is still
about 12k rows, concentrated in dates that still hit terminal session expiry or
otherwise only partially completed.

Updated at: 2026-05-08T00:00:00Z.

## All-court 2025/2026 remediation run

Started on EC2:

```bash
cd /opt/ihcj/repo
./.venv/bin/python download.py \
  --start_date 2025-01-01 \
  --end_date 2026-05-08 \
  --day_step 1 \
  --max_workers 4 \
  --sync-s3 \
  --compress-pdfs
```

Primary log:

```text
/opt/ihcj/repo/logs/all_courts_2025_2026_retry_20260508T010741Z.log
primary_pid=163407
```

Failed-date watcher:

```bash
./.venv/bin/python scripts/retry_failed_dates_from_log.py \
  --primary-log /opt/ihcj/repo/logs/all_courts_2025_2026_retry_20260508T010741Z.log \
  --primary-pid 163407 \
  --rounds 3 \
  --max-workers 1 \
  --poll-seconds 300
```

Watcher log:

```text
/opt/ihcj/repo/logs/all_courts_failed_retry_watcher_20260508T011115Z.log
```

Checkpoint at `2026-05-08T05:11:19Z`:

```text
started courts: 9~13, 27~1, 19~16, 18~6, 36~29
completed/synced courts: 9~13, 27~1, 19~16, 18~6
current court: 36~29 High Court for State of Telangana
visible Telangana progress: about 229/493 date windows

successful task summaries:
  9~13: 477
  27~1: 478
  19~16: 476
  18~6: 493
  36~29: 221
  total: 2145

failed date tasks captured for watcher:
  9~13: 16
  27~1: 15
  19~16: 17
  36~29: 8
  total: 56

downloaded_by_task_total=17218
new_pdf_sync_total_for_completed_courts=33138
```

The watcher was still waiting for the primary PID. Telangana had not synced yet,
so its local downloaded count was not reflected in `New PDF files` S3 sync
counters. Process health was normal: primary PID active, Ghostscript active,
root disk about 7% used, and local data files continuing to change.

Checkpoint at `2026-05-08T06:22:23Z`:

```text
completed/synced courts: 9~13, 27~1, 19~16, 18~6
current court: 36~29 High Court for State of Telangana
visible Telangana progress: about 265/493 date windows

successful task summaries:
  9~13: 477
  27~1: 478
  19~16: 476
  18~6: 493
  36~29: 248
  total: 2172

failed date tasks captured for watcher:
  9~13: 16
  27~1: 15
  19~16: 17
  36~29: 17
  total: 65

result_rows_total=669520
downloaded_by_task_total=19176
downloaded_by_task_for_36_29=3763
new_pdf_sync_total_for_completed_courts=33138
```

Telangana local data had grown to about 3.7 GB. The watcher was still waiting
for the primary PID. No Telangana S3 sync had happened yet because the court
was still in progress.

Checkpoint at `2026-05-08T08:24:04Z`:

```text
completed/synced courts from daily run: 9~13, 27~1, 19~16, 18~6, 36~29
Telangana new_pdf_sync_total=11907

old daily all-court pid 163407 stopped after Telangana sync
old watcher pid 164796 stopped
handoff monitor pid 226075 stopped

filtered retry pid=229874
filtered retry log=logs/failed_through_telangana_retry_20260508T081814Z.log
filtered failed-task log=logs/failed_through_telangana_20260508T081814Z.log
filtered failed tasks=73
retry mode: day_step=1, max_workers=2

weekly remaining-courts pid=229876
weekly remaining-courts log=logs/remaining_courts_weekly_20260508T081814Z.log
remaining courts=28~2,22~18,7~26,24~17,2~5,1~12,20~7,29~3,32~4,23~23,
  14~25,17~21,21~11,3~22,8~9,11~24,16~20,5~15,33~10,10~8
weekly mode: day_step=7, max_workers=8
```

Initial weekly Andhra checkpoint:

```text
court=28~2
weekly summaries=37
weekly failures=1
```

Rationale for the mode split: weekly windows reduce search/session overhead for
refreshing mostly-existing data, while failed windows are retried through
`scripts/retry_failed_dates_from_log.py` with `day_step=1`, so failed weekly
windows are automatically split into daily tasks during follow-up.

Checkpoint at `2026-05-08T09:57:12Z`:

```text
portal count capture completed:
  file=portal_s3_year_compare_all_courts_2025_2026_20260508T093248Z.tsv
  rows=50 court/year rows + header
  note=S3 columns in this file came from s3_layer_audit_2025_2026_current.tsv,
       which predates some current backfill syncs; use it as portal baseline
       and rerun S3 audit after the backfill finishes.

weekly remaining-courts run:
  pid=237045
  log=logs/remaining_courts_weekly_w4_20260508T084009Z.log
  mode=day_step=7 max_workers=4
  completed/synced court=28~2
  28~2 new_pdf_sync_total=13097
  current court=22~18
  22~18 summaries=41
  22~18 failures=0 so far

filtered failed-date retry:
  pid=229874
  log=logs/failed_through_telangana_retry_20260508T081814Z.log
  round_1_finished_tasks=34/73
  recent successes include 36~29 2025-01-29 and 2025-02-05
```

Important Telangana numbers:

```text
portal 2025=50749
portal 2026=7983
portal 2025+2026=58732

fresh post-sync S3 audit:
  2025 metadata_json=49229 data_pdf=38694 parquet_rows=49365
  2026 metadata_json=6471  data_pdf=6468  parquet_rows=6471
  parquet total=55836
  portal_minus_parquet=2896

Telangana new PDFs synced from daily run:
  2025=10407
  2026=1500
  total=11907

Telangana failed daily windows before retry=25
```

Checkpoint at `2026-05-08T10:31:09Z`:

```text
weekly remaining-courts run:
  pid=237045
  log=logs/remaining_courts_weekly_w4_20260508T084009Z.log
  mode=day_step=7 max_workers=4 sync-s3 compress-pdfs
  completed/synced court=28~2
  current court=22~18
  started courts=28~2, 22~18
  successful task summaries:
    28~2: 55
    22~18: 52
    total: 107
  failed weekly tasks:
    28~2: 16
    22~18: 16
    total: 32
  downloaded_by_task_total=3680
  synced_new_pdf_total=13097, all from 28~2 so far

filtered failed-date retry:
  pid=229874
  log=logs/failed_through_telangana_retry_20260508T081814Z.log
  mode=day_step=1 max_workers=2 sync-s3 compress-pdfs
  round_1_finished_tasks=35/73
  latest visible child=36~29 2025-04-30
```

Interpretation: the broad weekly run is still productive but weekly windows for
larger courts continue to hit terminal `session_expire`; those failures are
being surfaced instead of silently counted as zero and are intended to be fed
through the daily failed-window retry path after each broad run.

Checkpoint at `2026-05-08T10:35:26Z`:

```text
weekly remaining-courts run:
  completed/synced courts=28~2, 22~18
  current court=7~26
  22~18 new PDF files:
    2025=2450
    2026=4886
    total=7336
  cumulative synced new PDFs visible in weekly log:
    28~2=13097
    22~18=7336
    total=20433

filtered failed-date retry:
  round_1_finished_tasks=38/73
  recent successes:
    36~29 2025-04-30
    36~29 2025-07-24
    36~29 2025-08-01
  current visible child=36~29 2025-08-07
```

Checkpoint at `2026-05-08T10:42:36Z`:

```text
weekly remaining-courts run:
  started courts=28~2, 22~18, 7~26
  current court=7~26
  7~26 successful weekly tasks=37
  7~26 failed weekly tasks=0
  7~26 downloaded_by_task=744

filtered failed-date retry:
  round_1_finished_tasks=38/73
  current visible child=36~29 2025-08-07
  status=active; processed past first 25-download refresh and continued
```

Weekly failed-window watcher started at `2026-05-08T10:48:24Z`:

```text
watcher_pid=265325
watcher_log=logs/weekly_failed_retry_watcher_20260508T104805Z.log
watched_pid=237045
watched_log=logs/remaining_courts_weekly_w4_20260508T084009Z.log
post-completion action:
  ./.venv/bin/python scripts/retry_failed_dates_from_log.py \
    --primary-log logs/remaining_courts_weekly_w4_20260508T084009Z.log \
    --rounds 3 \
    --max-workers 2 \
    --poll-seconds 120
```

Checkpoint at `2026-05-08T10:58:57Z`:

```text
weekly remaining-courts run:
  current court=7~26
  7~26 successful weekly tasks=41
  7~26 failed weekly tasks=4
  7~26 downloaded_by_task=1081
  synced_new_pdf_total_still=20433

filtered failed-date retry:
  round_1_finished_tasks=40/73
  recent successful Telangana retries:
    36~29 2025-08-07
    36~29 2025-08-14
  current visible child=36~29 2025-08-18
```

Post-backfill audit watcher started at `2026-05-08T11:05:37Z`:

```text
watcher_pid=271226
watcher_log=logs/post_backfill_audit_watcher_20260508T110600Z.log
waiting_for_pids:
  229874  # filtered retry for already-completed daily courts
  265325  # watcher that waits for weekly run and retries weekly failures
post-completion action:
  ./.venv/bin/python scripts/audit_s3_layers.py \
    --courts 10_8,11_24,14_25,16_20,17_21,18_6,19_16,1_12,20_7,21_11,22_18,23_23,24_17,27_1,28_2,29_3,2_5,32_4,33_10,36_29,3_22,5_15,7_26,8_9,9_13 \
    --years 2025,2026 \
    > s3_layer_audit_2025_2026_post_backfill_<timestamp>.tsv
```

Checkpoint at `2026-05-08T11:11:14Z`:

```text
weekly remaining-courts run:
  current court=7~26
  7~26 successful weekly tasks=42
  7~26 failed weekly tasks=5
  7~26 downloaded_by_task=1170
  synced_new_pdf_total_still=20433

filtered failed-date retry:
  round_1_finished_tasks=45/73
  recent Telangana retries:
    36~29 2025-08-20 returncode=0
    36~29 2025-08-28 returncode=0
    36~29 2025-09-01 returncode=1
    36~29 2025-09-02 returncode=0
  current visible child=36~29 2025-09-03
```

Checkpoint at `2026-05-08T11:28:22Z`:

```text
weekly remaining-courts run:
  current court=7~26
  7~26 successful weekly tasks=43
  7~26 failed weekly tasks=20
  7~26 downloaded_by_task=1178
  synced_new_pdf_total_still=20433
  note=failures are concentrated in high-volume Delhi weekly windows; the
       process remains active and failed windows are queued for daily retry.

filtered failed-date retry:
  round_1_finished_tasks=45/73
  current visible child=36~29 2025-09-03
  note=this date is slow because it is repeatedly finding fresh PDFs and
       refreshing after 25 downloads; process is alive.
```

Checkpoint at `2026-05-08T11:33:48Z`:

```text
weekly remaining-courts run:
  current court=7~26
  7~26 successful weekly tasks=48
  7~26 failed weekly tasks=22
  7~26 downloaded_by_task=1712
  synced_new_pdf_total_still=20433

filtered failed-date retry:
  round_1_finished_tasks=46/73
  36~29 2025-09-03 finished returncode=0
  current visible child=36~29 2025-09-15
```

Checkpoint at `2026-05-08T11:39:19Z`:

```text
weekly remaining-courts run:
  completed/synced courts=28~2, 22~18, 7~26
  current court=24~17
  7~26 new PDF files:
    2025=7320
    2026=3681
    total=11001
  cumulative synced new PDFs visible in weekly log:
    28~2=13097
    22~18=7336
    7~26=11001
    total=31434

filtered failed-date retry:
  round_1_finished_tasks=48/73
  recent Telangana retries:
    36~29 2025-09-15 returncode=0
    36~29 2025-09-18 returncode=0
  current visible child=36~29 2025-09-19
```

Checkpoint at `2026-05-08T11:48:25Z`:

```text
weekly remaining-courts run:
  completed/synced courts=28~2, 22~18, 7~26, 24~17
  current court=2~5
  24~17 new PDF files:
    2025=50
    2026=388
    total=438
  cumulative synced new PDFs visible in weekly log:
    28~2=13097
    22~18=7336
    7~26=11001
    24~17=438
    total=31872

filtered failed-date retry:
  round_1_finished_tasks=50/73 at 2026-05-08T11:44:49Z
  current visible child=36~29 2025-10-07
```

Checkpoint at `2026-05-08T11:53:53Z`:

```text
weekly remaining-courts run:
  current court=2~5
  2~5 successful weekly tasks=43
  2~5 failed weekly tasks=2
  2~5 downloaded_by_task=18
  cumulative synced new PDFs visible in weekly log=31872

filtered failed-date retry:
  round_1_finished_tasks=50/73
  current visible child=36~29 2025-10-07
```

Checkpoint at `2026-05-08T12:04:51Z`:

```text
weekly remaining-courts run:
  current court=2~5
  2~5 successful weekly tasks=50
  2~5 failed weekly tasks=7
  2~5 downloaded_by_task=79
  cumulative synced new PDFs visible in weekly log=31872

filtered failed-date retry:
  round_1_finished_tasks=54/73
  recent Telangana retries:
    36~29 2025-10-09 returncode=0
    36~29 2025-10-12 returncode=0
    36~29 2025-12-29 returncode=0
  current visible child=36~29 2026-02-06
```

Checkpoint at `2026-05-08T12:21:26Z`:

```text
weekly remaining-courts run:
  completed/synced courts=28~2, 22~18, 7~26, 24~17, 2~5
  current court=1~12
  2~5 new PDF files:
    2025=2406
    2026=2841
    total=5247
  cumulative synced new PDFs visible in weekly log:
    28~2=13097
    22~18=7336
    7~26=11001
    24~17=438
    2~5=5247
    total=37119

filtered failed-date retry:
  round_1_finished_tasks=60/73
  recent retry state:
    9~13 2025-10-08 returncode=0
    9~13 2025-10-12 returncode=1
  current visible child=9~13 2025-10-13
```

Checkpoint at `2026-05-08T12:28:05Z`:

```text
weekly remaining-courts run:
  current court=1~12
  1~12 successful weekly tasks=55
  1~12 failed weekly tasks=0
  1~12 downloaded_by_task=726
  cumulative synced new PDFs visible in weekly log=37119

filtered failed-date retry:
  round_1_finished_tasks=60/73
  current visible child=9~13 2025-10-13
  note=child is active and downloading many PDFs with 25-download refreshes.
```

Checkpoint at `2026-05-08T12:33:40Z`:

```text
weekly remaining-courts run:
  completed/synced courts=28~2, 22~18, 7~26, 24~17, 2~5, 1~12
  current court=20~7
  1~12 new PDF files:
    jammuhc 2025=802
    jammuhc 2026=117
    kashmirhc 2025=687
    kashmirhc 2026=95
    total=1701
  cumulative synced new PDFs visible in weekly log=38820

filtered failed-date retry:
  round_1_finished_tasks=61/73
  9~13 2025-10-13 finished returncode=1
  current visible child=9~13 2025-10-14
```

Checkpoint at `2026-05-08T12:39:14Z`:

```text
weekly remaining-courts run:
  current court=20~7
  20~7 successful weekly tasks=43
  20~7 failed weekly tasks=0
  20~7 downloaded_by_task=274
  cumulative synced new PDFs visible in weekly log=38820

filtered failed-date retry:
  round_1_finished_tasks=61/73
  current visible child=9~13 2025-10-14
```

Checkpoint at `2026-05-08T12:51:03Z`:

```text
weekly remaining-courts run:
  current court=20~7
  20~7 successful weekly tasks=47
  20~7 failed weekly tasks=3
  20~7 downloaded_by_task=971
  cumulative synced new PDFs visible in weekly log=38820

filtered failed-date retry:
  round_1_finished_tasks=61/73
  current visible child=9~13 2025-10-14
  note=child is active and repeatedly hitting 25-download refreshes.
```

Checkpoint at `2026-05-08T13:07:33Z`:

```text
weekly remaining-courts run:
  current court=20~7
  20~7 successful weekly tasks=50
  20~7 failed weekly tasks=9
  20~7 downloaded_by_task=1671
  cumulative synced new PDFs visible in weekly log=38820

filtered failed-date retry:
  round_1_finished_tasks=63/73
  recent retry state:
    9~13 2025-10-14 returncode=0
    9~13 2025-10-15 returncode=0
  current visible child=9~13 2025-10-16
```

Checkpoint at `2026-05-08T13:12:41Z`:

```text
weekly remaining-courts run:
  pid=237045
  command includes day_step=7, max_workers=4, sync-s3, compress-pdfs
  current court=20~7
  successful weekly tasks total=407
  failed weekly tasks total=80
  20~7 successful weekly tasks=51
  20~7 failed weekly tasks=10
  20~7 downloaded_by_task=1913
  cumulative synced new PDFs visible in weekly log=38820
  note=20~7 2026-03-18..2026-03-24 hit session_expired on attempt 1 and was immediately retried by patched retry logic.

filtered failed-date retry:
  pid=229874
  current child pid=297177
  child command includes court=9~13, date=2025-10-16, day_step=1, max_workers=2, sync-s3, compress-pdfs
  round_1_finished_tasks=63/73
  current child is active and downloading results for Allahabad 2025-10-16.

post-backfill audit watcher:
  waiting for retry pid=229874 and weekly retry watcher pid=265325 before running fresh S3 layer audit.
```

Checkpoint at `2026-05-08T13:17:04Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=20~7
  successful weekly tasks total=409
  failed weekly tasks total=82
  20~7 successful weekly tasks=53
  20~7 failed weekly tasks=12
  20~7 downloaded_by_task=1951
  cumulative synced new PDFs visible in weekly log=38820
  latest successful windows include:
    2026-04-01..2026-04-07 results=1736 downloaded=25 skip_s3=1672 skip_local=25
    2026-04-08..2026-04-14 results=677 downloaded=13 skip_s3=664 skip_local=0

filtered failed-date retry:
  pid=229874 still active
  current child still=9~13 2025-10-16
  round_1_finished_tasks=63/73
  note=child had a session_expired attempt but restarted and continued processing.
```

Checkpoint at `2026-05-08T13:20:41Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=20~7
  successful weekly tasks total=411
  failed weekly tasks total=82
  20~7 successful weekly tasks=55
  20~7 failed weekly tasks=12
  20~7 downloaded_by_task=2119
  latest successful windows include:
    2026-04-22..2026-04-28 results=5103 downloaded=153 skip_s3=1811 skip_local=525
    2026-04-15..2026-04-21 results=1162 downloaded=15 skip_s3=972 skip_local=175

filtered failed-date retry:
  round_1_finished_tasks=65/73
  9~13 2025-10-16 finished returncode=1
  9~13 2025-10-17 finished returncode=0
  current visible child=9~13 2025-10-28

post-backfill audit:
  not started yet; watcher still waiting on active retry and weekly-retry watcher.
```

Checkpoint at `2026-05-08T13:24:49Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=20~7
  successful weekly tasks total=412
  failed weekly tasks total=83
  20~7 successful weekly tasks=56
  20~7 failed weekly tasks=13
  20~7 downloaded_by_task=2192
  latest successful window:
    2026-05-06..2026-05-08 results=369 downloaded=73 skip_s3=150 skip_local=75
  note=tail shows active progress and session refreshes after 25 downloads.

filtered failed-date retry:
  round_1_finished_tasks=65/73
  current visible child=9~13 2025-10-28
  note=tail shows active processing and repeated 25-download refreshes.

post-backfill audit:
  not started yet; watcher still waiting.
```

Checkpoint at `2026-05-08T13:52:52Z`:

```text
filtered failed-date retry:
  round_1 complete
  round_1 successes=59
  round_1 failures=14
  round_2 started with 14 failed tasks
  round_2 first task recovered:
    19~16 2025-11-19 returncode=0
  current/active around checkpoint:
    19~16 2025-12-04

consistency note:
  during 19~16 sync, logs showed local PDFs whose matching local JSON metadata files were missing, so decision date could not be extracted.
  example warning pattern:
    Error extracting decision date from data/court/cnrorders/calcutta_appellate_side/orders/<cnr>_2026-04-28.json: No such file or directory
  impact:
    these PDFs are preserved locally but skipped for upload/parquet because the scraper cannot assign decision-date partition without metadata JSON.
  follow-up:
    after active runs finish, include these preserved/skipped files in the consistency audit and decide whether to rehydrate missing JSON from S3 or refetch metadata.

weekly remaining-courts run:
  still active on 20~7 with per-PDF downloads; no new task summary yet.
```

Checkpoint at `2026-05-08T13:57:38Z`:

```text
filtered failed-date retry:
  round_1 complete:
    successes=59
    failures=14
  round_2 progress:
    finished=3/14
    19~16 2025-11-19 returncode=0
    19~16 2025-12-04 returncode=0
    19~16 2025-12-11 returncode=1
    current visible child=27~1 2025-11-13

weekly remaining-courts run:
  still active on 20~7
  completed summary counters unchanged:
    20~7 successful weekly tasks=57
    20~7 failed weekly tasks=13
    20~7 downloaded_by_task=2505
  note=tail still shows active per-PDF downloads.

post-backfill audit:
  not started yet; waiting on retry and weekly retry watcher.
```

Checkpoint at `2026-05-08T14:26:14Z`:

```text
filtered failed-date retry:
  top-level pid=229874 exited
  all filtered failed-date tasks recovered by round 3:
    round_1 successes=59 failures=14
    round_2 successes=13 failures=1
    round_3 successes=1 failures=0

post-backfill audit watcher:
  observed pid_exited=229874 at 2026-05-08T14:25:37Z
  now waiting only for weekly retry watcher pid=265325

weekly remaining-courts run:
  current court=29~3
  29~3 successful weekly tasks=27
  29~3 failed weekly tasks=13
  cumulative synced new PDFs visible in weekly log=47250
```

Checkpoint at `2026-05-08T14:31:22Z`:

```text
filtered failed-date retry:
  complete; top-level pid exited and audit watcher observed it.

post-backfill audit watcher:
  waiting only for weekly retry watcher pid=265325

weekly remaining-courts run:
  current court=29~3
  29~3 successful weekly tasks=27
  29~3 failed weekly tasks=17
  29~3 downloaded_by_task=636
  note=tail shows active parallel result processing and downloads for 29~3; not stalled.
```

Checkpoint at `2026-05-08T14:36:25Z`:

```text
weekly remaining-courts run:
  moved past 29~3 to 32~4
  29~3 synced new PDFs visible in log=1563
  current court=32~4
  32~4 successful weekly tasks=18
  32~4 failed weekly tasks=53
  32~4 downloaded_by_task=0
  cumulative synced new PDFs visible in weekly log=48813

post-backfill audit watcher:
  still waiting for weekly retry watcher pid=265325
```

Checkpoint at `2026-05-08T14:46:12Z`:

```text
weekly remaining-courts run:
  moved past 32~4 and 23~23
  current court=14~25
  23~23 successful weekly tasks=66
  23~23 failed weekly tasks=5
  23~23 downloaded_by_task=0
  14~25 successful weekly tasks=59
  14~25 failed weekly tasks=6
  14~25 downloaded_by_task=29
  cumulative synced new PDFs visible in weekly log still=53811
  note=14~25 sync summary has not appeared yet.

post-backfill audit watcher:
  still waiting for weekly retry watcher pid=265325
```

Checkpoint at `2026-05-08T14:40:09Z`:

```text
weekly remaining-courts run:
  current court=32~4
  29~3 synced new PDFs visible in log increased to 5821
  32~4 successful weekly tasks=18
  32~4 failed weekly tasks=53
  32~4 downloaded_by_task=0
  cumulative synced new PDFs visible in weekly log=53071

post-backfill audit watcher:
  still waiting for weekly retry watcher pid=265325
```

Checkpoint at `2026-05-08T14:17:44Z`:

```text
weekly remaining-courts run:
  current court=29~3
  29~3 successful weekly tasks=26
  29~3 failed weekly tasks=6
  29~3 downloaded_by_task=636
  cumulative synced new PDFs visible in weekly log=47250

filtered failed-date retry:
  round_2 progress=12/14
  newly recovered since prior checkpoint:
    36~29 2025-09-25 returncode=0
    36~29 2025-10-07 returncode=0
    9~13 2025-10-12 returncode=0
    9~13 2025-10-13 returncode=0
    9~13 2025-10-16 returncode=0
  current visible child=9~13 2025-10-28

post-backfill audit:
  not started yet; waiting on retry and weekly retry watcher.
```

Checkpoint at `2026-05-08T14:02:39Z`:

```text
weekly remaining-courts run:
  broad run moved past 20~7 to 29~3
  20~7 synced new PDFs visible in log=8430
  started courts count=8
  current court=29~3
  29~3 successful weekly tasks=14
  29~3 downloaded_by_task=51
  cumulative synced new PDFs visible in weekly log=47250

filtered failed-date retry:
  round_2 progress=7/14
  recovered in round_2:
    19~16 2025-11-19 returncode=0
    19~16 2025-12-04 returncode=0
    27~1 2025-11-13 returncode=0
    27~1 2025-11-27 returncode=0
    36~29 2025-02-12 returncode=0
    36~29 2025-09-01 returncode=0
  still failed in round_2 so far:
    19~16 2025-12-11 returncode=1
  current visible child=36~29 2025-09-25
```

Checkpoint at `2026-05-08T13:46:01Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=20~7
  successful weekly tasks total=413
  failed weekly tasks total=83
  20~7 successful weekly tasks=57
  20~7 failed weekly tasks=13
  20~7 downloaded_by_task=2505
  note=completed counters remain flat, but CPU/tail show active per-PDF downloads and 25-download refreshes.

filtered failed-date retry:
  round_1_finished_tasks=71/73
  recent successes:
    9~13 2025-12-10 returncode=0
    9~13 2025-12-11 returncode=0
  current visible child=9~13 2025-12-12

post-backfill audit:
  not started yet; watcher still waiting.
```

Checkpoint at `2026-05-08T13:36:29Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=20~7
  successful weekly tasks total=413
  failed weekly tasks total=83
  20~7 successful weekly tasks=57
  20~7 failed weekly tasks=13
  20~7 downloaded_by_task=2505
  note=completed counters flat since 13:28, but process CPU and tails show active per-PDF downloads inside large result windows.

filtered failed-date retry:
  round_1_finished_tasks=66/73
  9~13 2025-10-28 finished returncode=1
  current visible child=9~13 2025-11-12

post-backfill audit:
  not started yet; watcher still waiting.
```
Checkpoint at `2026-05-08T14:50:37Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  command includes --day_step 7 --max_workers 4 --sync-s3 --compress-pdfs
  started courts=13/20
  current court=21~11
  successful weekly tasks total=686
  failed weekly tasks total=196
  downloaded_by_task total=12333
  visible S3 new PDF sync total=53850
  note=advanced from 665 to 686 successful weekly windows between 14:48 and 14:50 UTC, so the primary run is not stuck.

failed-window retry watcher:
  pid=265325 waiting for primary pid=237045
  configured command retries failed weekly windows from logs/remaining_courts_weekly_w4_20260508T084009Z.log with --rounds 3 --max-workers 2.

post-backfill audit watcher:
  pid=271226 waiting for failed-window retry watcher pid=265325
  no post-backfill S3 audit TSV exists yet.
```

Checkpoint at `2026-05-08T14:59:14Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=21~11
  started courts=13/20
  successful weekly tasks total=696
  failed weekly tasks total=206
  downloaded_by_task total=12393
  recent failure type=Search API session expired after retries
  note=completed summaries are temporarily flat because active tasks are large result windows with ongoing per-result downloads; this is not a local/system failure.

failed-window retry behavior:
  scripts/retry_failed_dates_from_log.py parses final "Task failed after retries" entries only.
  each failed weekly window is rerun with --day_step 1 --max-workers 2 --sync-s3 --compress-pdfs.
  this matches the current hypothesis that failed weekly windows need smaller retry slices.
```

Checkpoint at `2026-05-08T15:21:20Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  current court=21~11
  started courts=13/20
  successful weekly tasks total=699
  failed weekly tasks total=223
  downloaded_by_task total=12639
  21~11 successful weekly tasks=43
  21~11 failed weekly tasks=27
  21~11 downloaded_by_task=325
  latest completed 21~11 window=2026-05-06..2026-05-08 results=1512 downloaded=139 skip_s3=447 skip_local=375
  note=the broad pass is progressing slowly but is active; failed 21~11 windows continue to be captured for daily retry.

downstream watchers:
  weekly failed-window retry watcher pid=265325 still waiting for primary pid=237045.
  post-backfill audit watcher pid=271226 still waiting for retry watcher pid=265325.
  no post-backfill S3 audit TSV exists yet.
```

Checkpoint at `2026-05-08T15:23:51Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  21~11 primary weekly pass completed and broad run moved to 3~22
  started courts=14/20
  successful weekly tasks total=702
  failed weekly tasks total=225
  downloaded_by_task total=12671
  21~11 successful weekly tasks=43
  21~11 failed weekly tasks=28
  21~11 downloaded_by_task=325
  21_11 visible S3 new PDF sync total=4725
  current court=3~22
  3~22 successful weekly tasks=3
  3~22 failed weekly tasks=1
  3~22 downloaded_by_task=32

downstream watchers:
  weekly failed-window retry watcher pid=265325 still waiting for primary pid=237045.
  post-backfill audit watcher pid=271226 still waiting for retry watcher pid=265325.
```

Checkpoint at `2026-05-08T15:46:08Z`:

```text
weekly remaining-courts run:
  pid=237045 still active
  3~22 primary weekly pass completed and broad run moved to 8~9
  started courts=15/20
  successful weekly tasks total=736
  failed weekly tasks total=266
  downloaded_by_task total=13110
  3~22 successful weekly tasks=29
  3~22 failed weekly tasks=42
  3~22 downloaded_by_task=445
  3_22 visible S3 new PDF sync total=2420
  current court=8~9
  8~9 successful weekly tasks=8
  8~9 downloaded_by_task=26

downstream watchers:
  weekly failed-window retry watcher pid=265325 still waiting for primary pid=237045.
  post-backfill audit watcher pid=271226 still waiting for retry watcher pid=265325.
```

Checkpoint at `2026-05-08T16:53:55Z`:

```text
weekly remaining-courts run:
  pid=237045 exited at 2026-05-08T16:48:24Z
  all 20 remaining courts started
  successful weekly tasks total=969
  failed weekly tasks total=451
  downloaded_by_task total=13392
  visible S3 new PDF sync total=68103

failed-window retry watcher:
  pid=265325 active
  retry script pid=334893 active
  round=1 failed_tasks=451
  retry mode=day_step=1, max_workers=2, sync-s3, compress-pdfs
  progress at checkpoint: started=14, finished=13, failed=8
  retry round 1 downloaded_by_task=2 so far; visible new PDF sync=2 so far

important consistency observation:
  retry log has missing-local-JSON / unparseable-decision-date warnings during S3 sync.
  counts at checkpoint:
    remaining_courts_weekly_w4 log: unparseable_decision_date_warnings=7, missing_json_warnings=38
    failed_task_rerun_round_1 log: unparseable_decision_date_warnings=26, missing_json_warnings=339
  these skipped local PDFs cannot be partitioned by decision date and are not uploaded by sync.
  post-backfill audit must verify whether they are already represented in S3/parquet or are true orphan PDFs.

  downstream audit:
  post-backfill audit watcher pid=271226 is waiting for retry watcher pid=265325.
  no post-backfill S3 audit TSV exists yet.
```

Checkpoint at `2026-05-08T16:59:36Z`:

```text
progress monitor:
  monitor_pid=337126
  monitor_log=logs/progress_monitor_20260508T170000Z.log
  cadence=15 minutes
  purpose=durable retry/audit progress snapshots if local session disconnects

failed-window retry watcher:
  round=1 still active
  progress at checkpoint: started=24, finished=23, failed=11
  current court=10~8
  downloaded_by_task for retry round 1 so far: {'10~8': 73}
  visible S3 new PDF sync for retry round 1 so far: {'10_8': 70}

downstream audit:
  no post-backfill S3 audit TSV exists yet.

audit comparison helper:
  script=scripts/compare_portal_baseline_to_s3_audit.py
  copied_to_ec2=yes
  verified_with_stale_inputs:
    ./.venv/bin/python scripts/compare_portal_baseline_to_s3_audit.py \
      --portal-baseline-tsv portal_s3_year_compare_all_courts_2025_2026_20260508T093248Z.tsv \
      --s3-audit-tsv s3_layer_audit_2025_2026_current.tsv
  purpose=compare stored portal_total baseline to fresh post-backfill S3 audit without refetching portal counts.
```

Checkpoint at `2026-05-08T17:13:26Z`:

```text
failed-window retry watcher:
  round=1 still active
  progress at checkpoint: started=35, finished=34, failed=15
  current court=10~8
  current window=2025-10-29..2025-11-04
  visible S3 new PDF sync for retry round 1 so far: {'10_8': 757}

downstream audit:
  no post-backfill S3 audit TSV exists yet.
```

Checkpoint at `2026-05-08T17:22:15Z`:

```text
failed-window retry watcher:
  pid=265325 active
  retry script pid=334893 active
  round=1 still active
  progress from durable monitor: started=35, finished=34, failed=15
  active court/window observed in process list: 10~8, 2025-11-12..2025-11-18
  visible S3 new PDF sync for retry round 1 so far: {'10_8': 1007}

downstream audit:
  post-backfill audit watcher pid=271226 still waiting for retry watcher pid=265325.
  no post-backfill S3 audit TSV exists yet.

operator note:
  user asked to reduce polling frequency; rely on progress_monitor_20260508T170000Z.log and check roughly hourly unless explicitly asked.
```

## Completion audit gate

Objective restated as concrete deliverables:

```text
1. Scraper behavior:
   terminal eCourts search/session failures must be surfaced and retried, not
   silently treated as zero-result success.

2. Backfill execution:
   2025/2026 refreshes for all courts must run with S3 sync and PDF
   compression enabled, with failed windows rerun at smaller day_step.

3. Raw/S3 consistency:
   for 2025/2026, count raw metadata JSON, raw PDFs, metadata tar/index,
   data tar/index, and parquet rows by court/year.

4. Portal comparison:
   compare fresh S3 counts to the stored eCourts portal baseline
   portal_s3_year_compare_all_courts_2025_2026_20260508T093248Z.tsv.

5. Residual gap explanation:
   identify whether remaining gaps are primarily failed windows still pending,
   raw JSON/PDF/index/parquet drift, missing/corrupt PDFs, or portal-only rows.

6. Verification evidence:
   keep concrete commands/log paths/TSVs/tests in this note before marking the
   overall goal complete.
```

Current evidence status:

```text
scraper failure surfacing:
  evidence=download.py patch + tests/test_download_s3_sync.py
  local_test=./.venv/bin/python -m unittest tests/test_download_s3_sync.py
  latest_result=Ran 9 tests, OK at 2026-05-08T17:23Z local time
  status=covered for unit behavior, plus EC2 logs show terminal failures now reported.

helper script syntax:
  evidence=./.venv/bin/python -m py_compile scripts/audit_s3_decision_window.py scripts/audit_s3_layers.py scripts/audit_s3_window.py scripts/compare_portal_baseline_to_s3_audit.py scripts/compare_portal_parquet_window.py scripts/compare_portal_s3_year_counts.py scripts/count_s3_loose_metadata_window.py scripts/count_s3_metadata_tar_window.py scripts/count_s3_parquet_window.py scripts/count_s3_raw_decision_window.py scripts/ecourts_count_window.py scripts/inspect_s3_partition_gap.py scripts/probe_portal_date_semantics.py scripts/retry_failed_dates_from_log.py scripts/sync_local_backfill_minimal.py
  latest_result=passed with no output at 2026-05-08T17:27Z local time

backfill execution:
  evidence=EC2 weekly remaining-courts run plus failed-window retry watcher.
  weekly_primary_log=logs/remaining_courts_weekly_w4_20260508T084009Z.log
  failed_retry_watcher=logs/weekly_failed_retry_watcher_20260508T104805Z.log
  status=in progress; retry watcher pid=265325 has not finished.

raw/S3 consistency:
  evidence_target=s3_layer_audit_2025_2026_post_backfill_<timestamp>.tsv
  status=blocked on retry watcher; post-backfill audit watcher pid=271226 waiting.

portal comparison:
  baseline=portal_s3_year_compare_all_courts_2025_2026_20260508T093248Z.tsv
  comparison_script=scripts/compare_portal_baseline_to_s3_audit.py
  status=script ready and tested against stale audit, but final comparison blocked on fresh audit.

residual gap explanation:
  status=incomplete until fresh audit and comparison are available.
```

Checkpoint at `2026-05-09T02:24:50Z`:

```text
failed-window retry watcher:
  round=1 still active
  latest rerun log=logs/failed_task_rerun_round_1_20260508T164824Z.log
  progress at checkpoint: started=153, finished=152, failed=90
  current active window observed in process list: court=28~2, 2025-12-24..2025-12-30
  last completed windows include:
    28~2 2025-10-29..2025-11-04 rc=0
    28~2 2025-11-05..2025-11-11 rc=0
    28~2 2025-11-12..2025-11-18 rc=0
    28~2 2025-11-19..2025-11-25 rc=0
    28~2 2025-11-26..2025-12-02 rc=0

retry recovery totals so far:
  downloaded_by_task:
    10~8=4122
    16~20=2
    20~7=50
    21~11=5621
    22~18=4925
    24~17=76
    28~2=1318
  visible new PDF sync:
    10_8=18272
    16_20=2
    20_7=150
    21_11=14396
    22_18=8050
    24_17=76
    28_2=1459

downstream audit:
  post-backfill audit watcher pid=271226 still waiting for retry watcher pid=265325.
  no post-backfill S3 audit TSV exists yet.
  no post-backfill portal-vs-S3 comparison TSV exists yet.
```

Checkpoint at `2026-05-09T02:30:39Z`:

```text
EC2 storage headroom:
  host=ubuntu@13.232.212.167
  root_volume=242G total, 15G used, 227G free (6% used)

repo-local footprint:
  data=123M
  logs=60M
  captcha-tmp=165M
  .venv=4.9G

interpretation:
  local-first retry passes without --sync-s3 are operationally safe on current
  disk headroom. The machine can accumulate multiple recovery passes before a
  final S3/parquet reconciliation run.
```

Checkpoint at `2026-05-09T02:37:18Z`:

```text
strategy switch:
  user requested stopping the sync-heavy failed-window retry and switching to
  a local-first recovery pass.

old processes stopped:
  retry watcher/script pids terminated: 265324, 265325, 334893
  waiting audit watcher pids terminated: 271225, 271226
  active sync-heavy download pid terminated before switch.

remaining work carried forward:
  source file=logs/failed_tasks_round_1_20260508T164824Z.tsv
  already finished windows parsed from old round-1 log
  remaining weekly windows after subtracting finished ones=298
  remaining file=logs/failed_tasks_remaining_localonly_20260509T023455Z.tsv

new active run:
  runner=/opt/ihcj/repo/logs/run_localonly_failed_retry_w5.py
  log=/opt/ihcj/repo/logs/localonly_failed_retry_w5_20260509T023455Z.log
  mode=local_only (no --sync-s3)
  per-window scraper args:
    --day_step 1
    --max_workers 5
    --compress-pdfs
  first verified active command:
    /opt/ihcj/repo/.venv/bin/python download.py --court_code 28~2 --start_date 2026-01-28 --end_date 2026-02-03 --day_step 1 --max_workers 5 --compress-pdfs

verification:
  log header confirms:
    watcher_started_at=2026-05-09T02:37:18.078824+00:00
    mode=local_only day_step=1 max_workers=5
  first task line confirms correct TSV parsing:
    rerun_task court=28~2 from=2026-01-28 to=2026-02-03 started_at=...
  download.py output confirms:
    S3 integration disabled
    Generated 7 tasks for court 28~2
```

Checkpoint at `2026-05-09T02:38:19Z`:

```text
new local-only retry health:
  runner process active:
    /opt/ihcj/repo/.venv/bin/python /opt/ihcj/repo/logs/run_localonly_failed_retry_w5.py
  first active scrape process:
    /opt/ihcj/repo/.venv/bin/python download.py --court_code 28~2 --start_date 2026-01-28 --end_date 2026-02-03 --day_step 1 --max_workers 5 --compress-pdfs

verification from log:
  started weekly windows in new mode=1
  finished weekly windows in new mode=0
  active window:
    28~2 2026-01-28..2026-02-03
  log continues to show:
    S3 integration disabled
    PDF compression enabled
    Generated 7 tasks for court 28~2

initial throughput signal:
  multiple daily result processors are active concurrently inside the first
  weekly retry window and the log is already showing fresh downloads on those
  daily slices. This confirms the local-first, max_workers=5 path is live.
```

Checkpoint at `2026-05-09T05:39:09Z`:

```text
local-only retry progress:
  started weekly windows=30
  finished weekly windows=29
  non-zero finished windows=15
  current active window:
    29~3 2025-12-03..2025-12-09

recovered task-level downloads so far:
  28~2=2973
  29~3=11735

local EC2 footprint after several hours of local-first recovery:
  data=2.6G
  logs=65M
  captcha-tmp=174M
  root volume still has 224G free

interpretation:
  the switched strategy is working and storage remains comfortable. Final
  completion is still deferred until we do the later reconciliation sync and
  regenerate the S3/parquet/portal audit artifacts.
```

Checkpoint at `2026-05-09T11:44:52Z`:

```text
strategy switch 2:
  user requested replacing the weekly-serialized local-only runner with a
  global daily-task queue across the remaining backlog.

backlog reduction before switch:
  previous weekly backlog file=logs/failed_tasks_remaining_localonly_20260509T023455Z.tsv
  weekly windows already completed by the serialized local-only runner were
  subtracted from that file.
  remaining weekly windows after subtraction=148
  pruned weekly backlog file=logs/failed_tasks_remaining_global_queue_20260509T114204Z.tsv

new runner:
  script=scripts/run_failed_daily_queue.py
  ec2_runner_pid=477089
  log=logs/global_daily_queue_20260509T114204Z.log
  mode=global_daily_queue
  workers=5
  compression=enabled
  sync_s3=disabled

queue expansion:
  daily_tasks=1024

first verified active day-level scraper processes:
  33~10 2026-04-22
  33~10 2026-04-23
  33~10 2026-04-24
  33~10 2026-04-25
  33~10 2026-04-26

interpretation:
  concurrency now spans the whole remaining backlog rather than one weekly
  window at a time. This removes the weekly serialization bottleneck while
  keeping local-first recovery semantics.
```

Checkpoint at `2026-05-10T03:19:35Z`:

```text
global daily queue result:
  queue runner log=logs/global_daily_queue_20260509T114204Z.log
  daily_tasks=1024
  finished=1024
  non_zero_failures=155
  watcher_finished_at=2026-05-09T16:06:37.363606+00:00

recovered task-level downloads by phase:
  old sync-heavy retry=16921
  local-only weekly retry=27694
  global daily queue=21611
  combined recovered downloads=66226

global daily queue recovered downloads by court:
  33~10=320
  3~22=10981
  5~15=1452
  7~26=4122
  8~9=4736

local EC2 footprint after queue completion:
  data=17G
  logs=88M
  captcha-tmp=229M
  root volume free=210G

next gate:
  scraping is no longer the active blocker. The next decision is whether to
  retry the 155 failed daily tasks once more locally or proceed to final
  reconciliation sync and then run fresh S3/parquet/portal audits.
```

Checkpoint at `2026-05-10T03:21:15Z`:

```text
final local retry pass started:
  source failed-daily file=logs/failed_daily_retry_round2_20260510T032106Z.tsv
  extracted failed daily tasks from first global queue=155

retry runner:
  log=logs/global_daily_queue_retry2_20260510T032106Z.log
  pid=549716
  mode=global_daily_queue
  workers=5
  daily_tasks=155
  sync_s3=disabled
  compression=enabled

first verified active retry tasks:
  33~10 2026-04-28
  33~10 2026-04-29
  33~10 2026-04-30
  33~10 2026-05-07
  3~22 2025-02-19
```

Checkpoint at `2026-05-10T07:43:03Z`:

```text
dedicated Madras 2026 refresh started:
  host=ubuntu@13.232.212.167
  log=logs/madras_2026_refresh_20260510T074303Z.log
  active scraper pid observed=574700
  command:
    ./.venv/bin/python download.py --court_code 33~10 --start_date 2026-01-01 --end_date 2026-05-10 --day_step 1 --max_workers 5 --compress-pdfs

run mode:
  local-first
  sync_s3=disabled
  compression=enabled

verification:
  log shows multiple concurrent result processors active for 33~10 and daily
  tasks already starting, for example from_date=2026-01-09.
```
## 2026-05-10: Madras failed-date serial retry

- Extracted the `46` terminal failed Madras `2026` dates from
  `logs/madras_2026_refresh_20260510T074303Z.log` on EC2 into:
  `logs/madras_2026_failed_dates_round1_noheader.tsv`
- Launched a dedicated Madras-only rerun on EC2 with one-day tasks and
  `workers=1` via:
  `scripts/run_failed_daily_queue.py --weekly-tsv logs/madras_2026_failed_dates_round1_noheader.tsv --log-path logs/madras_2026_failed_dates_retry_20260510T131554Z.log --workers 1 --compress-pdfs`
- This is intentionally the gentlest setup to separate transient/session-pressure
  failures from persistent court-specific failures.
- First result:
  - `2026-01-02` had failed earlier with terminal `Search API session expired after retries`
  - It succeeded on the serial one-day rerun with `returncode=0`
  - The runner then advanced to `2026-01-06`
- Interpretation:
  - At least some of the Madras failed dates are transient/load-related rather
    than permanently unscriptable.
  - Serial one-day reruns are a valid recovery strategy for the Madras `2026`
    discrepancy.

## 2026-05-11: Madras bench-filter probe

- Targeted the repeated Madras failed-date set from the Jan-Mar rerun:
  `2026-01-08`, `2026-01-11`, `2026-01-14`, `2026-01-17`, `2026-01-30`,
  `2026-03-09`, `2026-03-10`, `2026-03-19`.
- Used `2026-03-09` as the first count-only probe because the latest Jan-Mar
  run reported it as a sticky failure.
- Unfiltered portal API request for `33~10` on `2026-03-09` succeeded with
  `716` rows. The returned PDF paths include both Madras benches:
  `hc_cis_mas=552`, `mdubench=164`.
- Request-side bench filter guesses tested:
  `state_code_li`, `dist_code`, `int_fin_court_val`, `sel_establishment`,
  `selEstablishment`, `est_code`, `establishment_code`, `establishment`,
  `bench`, `bench_code`, `int_fin_bench_val`, `int_fin_est_val`, with values
  based on `hc_cis_mas`, `mdubench`, and `33~10~<bench>`.
- Result: no confirmed working request-side bench filter. Variants either
  returned zero rows, were ignored and returned mixed-bench rows, or triggered
  terminal `session_expire`.
- Interpretation: bench identity is reliably visible after search from PDF
  paths, but the accepted portal search payload field for pre-filtering by
  bench has not been identified. The next reliable path is to capture the
  browser network request after a human-completed advanced search with a bench
  selected.

Follow-up correction:

- Browser network capture showed the actual bench filter is the existing
  `dist_code` request field:
  `dist_code=1` for `hc_cis_mas`, `dist_code=2` for `mdubench`, empty for no
  bench filter.
- Focused API probe validated the mapping:
  `2026-03-09`: unfiltered `716` = `552` (`dist_code=1`) + `164`
  (`dist_code=2`).
  `2026-01-30`: unfiltered `1068` = `594` (`dist_code=1`) + `474`
  (`dist_code=2`).
- Added a scraper CLI hook `--dist-code` so failed/high-volume windows can be
  retried per bench without changing normal court-level scraping.
- Reran the 8 Madras failed dates for both `dist_code=1` and `dist_code=2`.
  No new downloads were needed, but `166` existing local PDFs were detected as
  local-only skips.
- Synced local Madras files to S3:
  `hc_cis_mas`: `138` metadata + `138` PDFs
  `mdubench`: `28` metadata + `28` PDFs
  4 orphan PDFs remain locally because their paired JSON files are missing, so
  they cannot be safely partitioned by decision date.
- Post-sync Madras 2026 S3 internal audit:
  raw JSON `67299`, raw PDF `67299`, metadata index `67299`, data index
  `67299`, parquet `67299`.
- Post-sync weekly portal recount for Madras 2026:
  portal weekly sum `67241`, S3/index/parquet `67299`, delta `-58`
  (`portal - S3`). This is within small portal/count drift and the earlier
  large Madras 2026 gap is gone.

## Current retry priority

Source: committed `docs/portal_vs_s3_all_years_complete_20260514.csv`.

Top courts by remaining positive `portal - data_index` gap:

```text
9_13  Allahabad High Court              110335
29_3  High Court of Karnataka            60553
3_22  High Court of Punjab and Haryana   14663
22_18 High Court of Chhattisgarh         10151
21_11 High Court of Orissa                9076
10_8  Patna High Court                    4695
28_2  High Court of Andhra Pradesh        4277
33_10 Madras High Court                   3909
```

Current EC2 state on 2026-05-14:

```text
primary run: download.py --start_date 2024-01-01 --end_date 2024-12-31 --day_step 7 --max_workers 3 --compress-pdfs
primary pid: 754007
current log: logs/all_courts_2024_weekly_20260514.log
retry watcher: scripts/retry_failed_dates_from_log.py
retry watcher pid: 763522
retry watcher log: logs/all_courts_2024_weekly_failed_retry_20260514.nohup
```

Interpretation:

- Let the broad 2024 weekly run finish first.
- Then rerun only the failed windows collected from that log.
- Use daily `day_step=1` on failed windows before trying any wider
bench-specific narrowing.

## Live portal drift check

After the committed 2026-05-14 reference CSV was generated, some portal counts
changed again. EC2 live compares show that a few previously "over portal"
courts are now either exact matches or very close:

```text
3_22  2026  portal=40757  raw_json=40757  raw_pdf=40757  metadata_index=40757  data_index=40757  parquet_pdf_link=40757
7_26  2026  portal=18510  raw_json=18510  raw_pdf=18510  metadata_index=18510  data_index=18510  parquet_pdf_link=18510
27_1  2025  portal=71172  raw_json=71172  raw_pdf=71172  metadata_index=71172  data_index=71172  parquet_pdf_link=71173
```

Bombay 2025 still shows internal duplication noise:

```text
json_not_metadata_index=1
json_without_pdf=1
json_without_data_index=1
parquet_without_json=1
metadata_index_duplicate_entries=34
data_index_duplicate_entries=34
sample_json_not_metadata_index=HCBM060000492024_1_2025-04-04.json
sample_parquet_without_json=HCBM030044932023_1_2025-03-27.pdf
```

Interpretation:

- Some apparent "our counts are larger than portal" cases were stale-baseline
  artifacts. The portal itself moved after the reference CSV snapshot.
- For future debugging, always re-check live portal counts before treating an
  overage as a real dataset defect.
- When live portal still trails our counts, the next inspection target should
  be duplicate index/parquet entries in the exact court-year partition, not a
  blanket global fix.

## PHHC 2025 sample orphan check

Sample records from `3_22 / 2025` that are present in `metadata/json` but not
in `data/pdf` are not empty metadata stubs. The JSON objects contain:

```text
downloaded=true
pdf_link=court/cnrorders/phhc/orders/<basename>.pdf
raw_html present
```

Example:

```text
metadata/json/year=2025/court=3_22/bench=phhc/PHHC010000682025_1_2025-02-24.json
downloaded=true
pdf_link=court/cnrorders/phhc/orders/PHHC010000682025_1_2025-02-24.pdf
data/pdf/year=2025/court=3_22/bench=phhc/PHHC010000682025_1_2025-02-24.pdf -> 404
```

Interpretation:

- These are valid downloaded rows whose PDF object is missing from S3.
- That is a post-download persistence problem, not a benign metadata-only row.
- The next inspection should trace whether the PDF was never uploaded, was lost
  in `sync-s3`, or was removed during cleanup after a partial failure.

## Duplicate CNR inspection

The larger-than-portal cases are not caused by year partition boundaries.
Repeated CNRs are present inside the same yearly parquet partitions:

```text
PHHC010474462002 -> 3 rows in 2025 parquet, all decision_date=2025-04-09, different pdf_link values
HCBM030083832022 -> 5 rows in 2025 parquet, all decision_date=2025-04-07, different pdf_link values
HCBM060000492024 -> duplicated across two Bombay bench parquet files in 2025, same decision_date=2025-03-07
```

Interpretation:

- This is a logical-duplicate problem inside the yearly parquet set.
- The duplicates are coming from repeated ingestion of the same CNR, not from
  an off-by-one year partitioning issue.
- The likely sources are overlapping reruns or multiple bench-specific rows
  being materialized without CNR-level dedupe before parquet write.

## Later court-year probes

Additional S3-only scans show the overage class is not universal:

```text
7_26 / 2025
raw_json=53579
raw_pdf=53051
metadata_index=52562
data_index=53051
parquet_pdf_link=52565
```

Interpretation: Delhi 2025 is primarily a missing-raw / missing-parquet case,
not a duplicate-parquet case.

```text
9_13 / 2025
raw_json=202488
raw_pdf=202488
metadata_index=202484
data_index=202488
parquet_pdf_link=202480
```

Interpretation: Allahabad 2025 is not one of the overage partitions; it is
slightly behind in parquet/metadata index coverage instead.
