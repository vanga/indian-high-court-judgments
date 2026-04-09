# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

@README.md

## Commands

```bash
# Setup
python3.13 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Download judgments for a specific court
python download.py --court_code "33~10" --start_date 2023-01-01 --end_date 2023-12-31 --max_workers 4

# Download and sync to S3
python download.py --sync-s3 --max_workers 4

# Process metadata into Parquet
python process_metadata.py

# Generate statistics
python stats.py --year 2024
```

No test framework is configured. Python 3.13 is the target runtime.

## Gotchas

### Court code formats

Two formats coexist — always use `to_s3_format()` / `from_s3_format()` from `src/utils/court_utils.py`:
- **eCourts/CLI format**: tilde separator (`27~1`)
- **S3 path format**: underscore separator (`27_1`)

Mixing these up will silently produce wrong S3 paths or fail to match eCourts API expectations.

### Data partitioning

Files are partitioned by **decision date year** (extracted from case HTML), not download date. This means a download run in 2025 may write files into `year=2018/` partitions.

### Session refresh

`ECourtSession` in `download.py` auto-refreshes every 25 downloads. If you change download logic, preserve this cadence or the eCourts server will reject requests.

### TAR archive limits

S3 tar archives split at 1 GB per part. `s3_utils.py` handles this automatically — don't create tars manually.

### Ghostscript dependency

PDF compression requires `gs` (Ghostscript) on PATH. The `--compress` flag silently skips compression if `gs` is not found.

## Architecture

The pipeline flows: **download.py** (orchestrator) → **src/captcha_solver/** (ONNX model for eCourts CAPTCHA) → **src/utils/s3_utils.py** (tar + upload) → S3 bucket `indian-high-court-judgments`.

`download.py` generates `CourtDateTask` objects (court × date chunk), runs them in parallel via `ThreadPoolExecutor`, each using `ECourtSession` for HTTP + CAPTCHA handling.

Index files (`metadata.index.json`, `data.index.json`) track tar contents and `updated_at` per bench, enabling incremental/resumable downloads.

S3 path convention: `{metadata|data}/{json|pdf|tar}/year=YYYY/court=XX_YY/bench=name/`

## CI/CD

GitHub Actions (`.github/workflows/court-data-pipeline.yml`): manual dispatch only (cron commented out), Python 3.13, AWS OIDC auth (no stored credentials).
