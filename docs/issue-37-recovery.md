# Issue #37: parser recovery and safe replay

This fix accepts bare `open_pdf(...)` handlers as well as the old
`javascript:open_pdf(...)` form. It makes parse failures, unexpected row errors,
and failed PDF downloads fail their date task after its bounded retries. These
failures are fatal even below the normal transient-failure tolerance. Already
present PDFs and genuinely empty searches remain valid zero-download results.
The existing contiguous coverage calculation prevents a failed date task from
advancing its court cursor. Successful local files can still be checkpointed.

## Verification limits

The regression inputs are synthetic examples of the markup reported in #37,
not a newly captured portal fixture. Offline tests do not establish portal
availability, successful current CAPTCHA handling, or that missing production
judgments have been recovered. No live crawl, S3 write, deployment, cursor reset,
or backfill is part of this change.

## Recovery procedure (requires an explicitly authorized production run)

1. Deploy/review this fix before replaying any affected range. Do not combine it
   with a change that advances cursors over failed date tasks.
2. Preserve the current indexes and aggregate per-court/bench coverage evidence.
   `updated_at` and `scraped_through_date` can reflect the failed parser runs;
   neither proves successful judgment capture. Likewise, the newest decision
   date in a filename is not proof that all earlier judgments are present.
3. Determine each affected court's earliest unverified date from run logs,
   portal results and existing object/index coverage. Issue #37 reports some
   courts behind since approximately August 26; this is an audit lead, not a
   universal verified cutoff. Replaying only the automatic 14-day overlap will
   not recover older gaps.
4. Start with one court and one explicit day, one worker, without `--sync-s3`.
   Inspect actual response outcomes, PDF signatures, metadata and the task
   summary. Obtain any CAPTCHA approval required by the execution environment.
5. For the approved missing-date range, use explicit `--court_code`,
   `--start_date`, `--end_date`, `--max_workers 1`, and a bounded runtime. Enable
   `--sync-s3` only for the separately authorized production repair. Explicit
   dates bypass the possibly over-advanced automatic starting cursor, so a
   blind destructive cursor rewind is unnecessary. Stop on incomplete rows;
   do not reinterpret a failed task as covered to move the cursor forward.
6. Reconcile successful downloaded PDFs, raw metadata, tar membership and
   parquet coverage against the requested court/date ranges before considering
   recovery complete. Keep failed/unrun ranges explicitly outstanding.

PR #36 concerns existing non-PDF placeholder objects skipped by filename. That
is a separate repair: this parser fix does not automatically replace those
objects. Review its upload error reporting and apply the parser fix there
before using its refetch path. Do not declare `not_found_in_search` evidence of
portal absence when parsing itself has failed.
