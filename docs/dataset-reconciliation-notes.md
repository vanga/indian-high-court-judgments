# Dataset Reconciliation Notes

This note captures durable lessons from the eCourts count reconciliation work.
It is intentionally short; detailed scrape logs and generated comparison CSVs
should stay out of long-term documentation unless they are baselines.

## What Counts Mean

Portal court/year counts are useful for finding missing coverage, but they are
the current portal view, not immutable ground truth. Some judgments captured in
S3 are no longer returned by the same portal date query.

For quick dataset reconciliation, use S3 data index counts first. They are the
cheapest proxy for PDFs available in the public dataset.

When a discrepancy matters, compare layers separately:

- Portal count by court/year.
- S3 data.index.json count and distinct file count.
- S3 metadata.index.json count and distinct file count.
- Raw JSON count.
- Raw PDF count.
- Parquet count after cnr + decision_date dedupe.

Interpretation:

- portal > data index: likely missing scrape windows, failed searches, or data
  not synced into indexes yet.
- metadata index > data index: metadata exists without matching indexed PDF.
- raw JSON > metadata index/parquet: raw metadata was captured but not fully
  materialized.
- data index > portal: investigate, but do not delete automatically. It may be
  valid historical data no longer returned by the portal.

## Scraper Lessons

The portal date filter appears to match the Decision Date shown in result-row
HTML, not the PDF filename or upload date. This was verified on sampled PHHC
windows, but should still be treated as sampled evidence rather than a formal
portal contract.

Silent failed searches were the main historical gap source. eCourts
session-expiry or API-error responses must raise terminal errors; they must not
be treated as successful zero-result windows.

For historical refreshes, weekly windows are usually efficient. If a weekly
window fails, retry as daily windows. If daily windows fail, retry with smaller
page size and, for large courts, split by bench/dist code.

For parquet/data-row dedupe, use cnr + decision_date. CNR alone is too
aggressive because the same CNR can appear later with another decision date.
Filename alone is too weak because repeated or overlapping runs can materialize
logical duplicates.

## Representative Portal Undercount

Punjab and Haryana, court 3_22, is the clearest example where S3 has more data
than the latest portal baseline without an index-duplicate explanation.

Snapshot for 3_22 all years:

~~~text
portal_minus_data_index=-102,597
data_index_duplicates=0
~~~

Snapshot for 3_22 / 2024:

~~~text
portal_baseline=139,495
data_index=155,078
data_index_distinct=155,078
parquet_rows_after_cnr_date_dedupe=154,801
parquet_distinct_cnr_decision_date=154,801
~~~

For 2024-09-30, the current portal result set was a subset of local parquet:

~~~text
local_parquet_cnrs=876
current_portal_cnrs=762
local_not_portal=114
portal_not_local=0
~~~

Example local-only rows looked valid and contained Decision Date metadata for
2024-09-30:

~~~text
PHHC011496812019
PHHC010080002019
~~~

Conclusion: portal undercounts are investigation signals, not deletion signals.

## Useful Baselines And Scripts

Reference portal baseline:

- docs/portal_vs_s3_all_years_complete_20260514.csv

Useful scripts:

- scripts/audit_s3_index_counts.py
- scripts/compare_portal_baseline_to_s3_indexes.py
- scripts/retry_failed_dates_from_log.py
- scripts/dedupe_s3_parquet_by_cnr_date.py
- scripts/probe_portal_date_semantics.py
