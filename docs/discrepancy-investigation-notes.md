# eCourts/Dataset Count Discrepancy Notes

This file is intended as long-term project memory, not a full scrape run log.
Keep durable findings, assumptions, and representative examples here. Put bulky
run logs and one-off CSV outputs under logs/ or generated artifacts instead.

## Durable Findings

The portal date filter appears to match the Decision Date shown in result-row
HTML, not the PDF filename date or upload date. A PHHC probe for
2025-02-20..2025-02-26 returned rows whose parsed decision dates all fell
inside that window. This is evidence from sampled courts/windows, not a formal
guarantee for every eCourts query.

S3 internal layer drift exists, but it is usually not large enough to explain
the full portal-vs-dataset gap by itself. In the 2025/2026 audit, raw metadata
JSON was ahead of parquet by about 26.7k rows, while the portal-vs-parquet gap
was about 430k rows. That means missing scrape windows and silent request
failures matter more than parquet/index lag for the large gaps.

Failed eCourts searches can look like successful zero-result windows unless the
response is explicitly checked for terminal errors. This was the main cause of
large discrepancies: failed date windows were effectively treated as done and
were skipped by later resume runs.

For logical dataset rows, use cnr + decision_date as the parquet dedupe key.
The same CNR can appear multiple times because judgments/orders can be
re-uploaded or materialized through repeated/overlapping runs. The same CNR can
also later appear on another decision date, so CNR alone is too aggressive.

Some current portal counts are lower than historical data already captured in
S3. Do not delete S3 raw/index/parquet rows solely because the current portal
date query returns fewer rows. Inspect representative records first.

## Backfill Strategy

Use portal court-year counts as a directional baseline for missing data, but
treat them as current portal state rather than immutable truth.

For normal historical refreshes, weekly windows are efficient because most PDFs
already exist and search is relatively cheap. When a weekly window fails, retry
with daily windows. If daily windows still fail, retry with smaller page size
and, for large courts, split by bench/dist code.

Bench/dist-code splitting is useful for high-volume courts because it reduces
payload size and backend pressure. Dist codes observed from portal requests can
be passed as dist_code; for example, Madras uses dist_code=1 and dist_code=2,
and Allahabad has been backfilled with dist_code=1 and dist_code=2.

For stubborn failed dates, a durable unresolved-date queue would be useful:
store only unresolved (court, year/date window, dist_code, page_size) tasks;
remove them on success. A full resolved-history table is unnecessary.

## Count Comparison Rules

For quick reconciliation, prefer S3 data.index.json counts. They are the best
cheap proxy for PDFs known to the public dataset.

When investigating a discrepancy, compare these layers separately:

- Portal count by court/year.
- S3 data.index.json count and distinct file count.
- S3 metadata.index.json count and distinct file count.
- Raw loose/tar JSON count.
- Raw loose/tar PDF count.
- Parquet row count after cnr + decision_date dedupe.

Interpretation guide:

- portal > data.index: likely missing scrape coverage, failed windows, or data
  not yet synced to indexes.
- metadata.index > data.index: metadata exists without matching indexed PDF, or
  PDF upload/indexing failed.
- raw_json > metadata.index/parquet: raw metadata was captured but not fully
  materialized.
- data.index > portal: may be valid historical rows no longer returned by the
  current portal, duplicate ingestion, or portal baseline drift. Do not
  auto-delete.

## Known Issue Classes

Silent search failure: eCourts error/session-expiry responses must raise rather
than be treated as empty result sets.

PDF corruption: before persisting a downloaded PDF, validate that the response
looks like a PDF and not an HTML/session-error page.

Pagination/session refresh: refreshing the eCourts session every 25 PDF
downloads is intentional. Avoid resetting pagination to the first page unless
that behavior is explicitly required; otherwise dense refreshes can re-scan the
same page repeatedly.

Sync cleanup: cleanup after S3 upload must not delete unsynced local files from
partially failed years/windows. Successful years can be cleaned; failed years
must remain available for retry/sync.

Parquet dedupe: parquet generation should drop duplicates using cnr +
decision_date, not filename alone and not CNR alone.

## Representative Portal-Undercount Example

Punjab and Haryana (3_22) is the clearest case where our dataset has more rows
than the latest portal baseline without an obvious duplicate-index explanation.

All-years comparison snapshot:

~~~text
net portal_minus_data_index=-102,597
negative_only=-103,524
positive_only=927
data_index_duplicates=0
~~~

For 3_22 / 2024:

~~~text
portal_baseline=139,495
data_index=155,078
data_index_distinct=155,078
parquet_rows_after_cnr_date_dedupe=154,801
parquet_distinct_cnr_decision_date=154,801
~~~

Filename/upload-date partition drift did not explain this. In the 2024 parquet
partition, 154,355 rows had a 2024 year in the PDF filename and only 446 rows
had non-2024 filename years.

For date 2024-09-30, the current portal result set was a subset of local parquet
rows:

~~~text
local_parquet_cnrs=876
current_portal_cnrs=762
local_not_portal=114
portal_not_local=0
~~~

Searching those 114 local-only CNRs across PHHC parquets from 2008-2026 found
only a small amount of later date movement:

~~~text
extra_hit_cnrs=114
extra_hits_rows=119
extra_with_multiple_rows=5
extra_with_other_decision_date_rows=5
~~~

Examples where the same CNR also appears on another decision date:

~~~text
PHHC010080292022 -> 2024-09-30 and 2026-04-20
PHHC010223642024 -> 2024-09-30 and 2026-01-29
PHHC010226682024 -> 2024-09-30 and 2026-01-29
PHHC010274511999 -> 2024-09-30 and 2026-04-29
PHHC011345562023 -> 2024-09-30 and 2025-08-06
~~~

Sample local-only rows contained valid judgment metadata/content:

~~~text
PHHC011496812019
decision_date=2024-09-30
pdf_link=court/cnrorders/phhc/orders/PHHC011496812019_1_2024-09-30.pdf
title=CRR(F)/3/2020 of SURYA KANT Vs KANIKA AND ORS
judge=MS. JUSTICE NIDHI GUPTA
disposal_nature=DISMISSED
raw text includes "Date of Decision: 30.09.2024"
portal metadata line includes "Decision Date : 30-09-2024"

PHHC010080002019
decision_date=2024-09-30
pdf_link=court/cnrorders/phhc/orders/PHHC010080002019_1_2024-09-30.pdf
title=CRM-M/3748/2019 of RANJIT KAUR AND ANR Vs SANDEEP SINGH
disposal_nature=DISPOSED OF
raw text includes "Date of Decision: 30.09.2024"
portal metadata line includes "Decision Date : 30-09-2024"
~~~

Conclusion: these rows look like valid historical judgments captured by the
scraper but no longer returned by the current portal date query. Use portal
overcounts to guide backfills, but treat portal undercounts as an investigation
signal, not a deletion signal.

## Useful Artifacts And Scripts

Portal reference counts:

- docs/portal_vs_s3_all_years_complete_20260514.csv

Comparison/audit scripts:

- scripts/audit_s3_index_counts.py
- scripts/compare_portal_baseline_to_s3_indexes.py
- scripts/retry_failed_dates_from_log.py
- scripts/dedupe_s3_parquet_by_cnr_date.py
- scripts/probe_portal_date_semantics.py

Operational convention: generated comparisons should include a timestamp in the
filename and should not be treated as permanent documentation unless they
capture a baseline needed for future reconciliation.
