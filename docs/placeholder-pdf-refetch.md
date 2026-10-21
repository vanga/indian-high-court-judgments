# Placeholder PDFs and how to re-fetch them

## The problem

Before `download.py` checked that a downloaded body starts with `%PDF`, some
portal error responses were saved under the judgment's filename and uploaded
as if they were the judgment:

- HTML "Welcome User Search Page not Found" (124 bytes) and HTTP 404 pages for
  `/cnrorders/...` (258-266 bytes), mostly Bombay.
- A 228-byte HTML error page (Andhra Pradesh).
- JSON or text errors such as
  `{"msg":"error","error":"FPDF error: Unexpected end of stream"}` or
  `Failed to create image from the QR code PNG file` (43-75 bytes, Delhi).

Every skip check in the pipeline is by filename (the `data.index.json` lookup
during scraping and `filter_new_files_by_name` at upload), so these files are
never fetched or uploaded again by a normal run.

A listing of every `data/pdf/` key on 2026-10-19 found 41,032 objects under
1 KiB that are not PDFs:

| Court | Count |
|-------|------:|
| Bombay 27~1 | 38,977 |
| Andhra Pradesh 28~2 | 910 |
| Delhi 7~26 | 757 |
| Punjab and Haryana 3~22 | 314 |
| Karnataka 29~3 | 69 |
| Gauhati 18~6, Madras 33~10 | 1 each |
| Supreme Court bucket | 3 (not handled by this scraper) |

## 1. Find them

```bash
python scripts/find_placeholder_pdfs.py --verify-magic --output placeholder-pdfs.csv
# narrower: --prefix data/pdf/year=2008/court=27_1/
```

The bucket is read anonymously (pass `--signed` to use credentials). Without
`--verify-magic` the list is size-only (`< --max-bytes`, default 1024); with
it, each candidate's first 8 bytes are read and real PDFs are dropped.
`--resolve-dates` adds a `search_date` column (see "Which dates are searched").

## 2. Re-fetch them

```bash
# Local only: PDFs land under data/court/cnrorders/..., nothing is uploaded
python download.py --refetch-list placeholder-pdfs.csv --court_code 28_2 \
    --refetch-report refetch-report-28_2.csv

# Maintainers: also replace the objects in S3
python download.py --refetch-list placeholder-pdfs.csv --court_code 28_2 --sync-s3
```

The list can be the CSV above (a `key` column, optional `search_date`) or a
plain file with one key per line. A re-fetch run:

- searches only the court x date ranges the listed keys need
  (`--day_step N` merges consecutive dates into ranges of up to N days; gaps
  are never bridged);
- downloads only listed files. Every other search row is skipped without a
  download, an S3 lookup or a metadata write;
- fetches listed files even though the S3 index contains their names;
- still applies the normal checks (`%PDF` header, 315-byte not-found page,
  HTTP status), so a portal that still serves an error page does not
  overwrite anything;
- never moves resume cursors, and never rewrites metadata JSON, metadata tars
  or parquet;
- writes a report (default `refetch-report.csv`): `key, outcome, detail,
  search_date, local_path, uploaded`.

| outcome | meaning |
|---------|---------|
| `refetched` | a real PDF was downloaded (and uploaded if `uploaded=true`) |
| `still_not_pdf` | the portal listed the file but still served an error (non-PDF body, empty body, 315-byte page, HTTP 404) |
| `not_found_in_search` | no search row for this bench + filename in the searched dates |
| `error` | the search or download failed (session, network, HTTP 5xx); re-run to retry |
| `unsupported` | not a high-court `data/pdf/year=/court=/bench=/` key, or no usable date |

Re-running with the same list is safe: a real PDF already on local disk is
reused, and a stale placeholder on local disk is deleted before downloading.

## Which dates are searched

A PDF can only be downloaded from a live search result, so each key needs a
search date. By default it is the `YYYY-MM-DD` at the end of the filename. The
portal's date filter matches the result row's Decision Date
(see `dataset-reconciliation-notes.md`), and partitions are by Decision Date
year, so the filename date is right when it equals the Decision Date.

In the 2026-10-19 list, 940 of 41,029 high-court keys have a filename year
different from the partition year (e.g.
`year=1993/.../HCBM060000601993_2_2014-06-15.pdf`). For those the filename date
is probably not the Decision Date. Either:

- run the finder with `--resolve-dates`, which reads the key's
  `metadata/json/` file and fills `search_date` from its Decision Date, or
- pass `--refetch-pad-days N` to search N days either side.

Keys that still are not found are reported as `not_found_in_search`.

## How S3 is updated (`--sync-s3`)

For each re-fetched file, in the partition of the listed key:

1. The `data/pdf/` object is overwritten with the real PDF.
2. The corrected PDFs are put in a new tar part
   `part-<UTC timestamp>-refetch.tar` next to the existing parts. Existing tar
   objects are not rewritten. In `data.index.json`, each corrected name is
   moved from the older part's `files` to the new part's `files`, and recorded
   in the older part's `superseded_files` (the stale member is still physically
   in that older tar). `file_count` is unchanged; `tar_size` grows by the new
   part's size.

### For tar consumers

A filename can now appear in more than one tar of a partition. To resolve it:

- **With the index:** the authoritative copy is in the part whose `files` lists
  the name. Ignore members listed in a part's `superseded_files`.
- **Without the index:** extract the parts in ascending name order (names are
  UTC timestamps) and let later members overwrite earlier ones. **Last part
  wins.**

Parts that have never had a member replaced are serialized exactly as before
(no `superseded_files` key). `scripts/full_s3_integrity_audit.py
--verify-tar-members` does not report superseded members as extra tar members.
