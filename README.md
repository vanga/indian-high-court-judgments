# Indian High Court Judgments

[![Daily Sync](https://github.com/vanga/indian-high-court-judgments/actions/workflows/court-data-pipeline.yml/badge.svg)](https://github.com/vanga/indian-high-court-judgments/actions/workflows/court-data-pipeline.yml)

An open dataset of judgments from 25 Indian High Courts, scraped from the [eCourts judgments portal](https://judgments.ecourts.gov.in). The dataset is synced daily via GitHub Actions.

- **Download the dataset**: [AWS Open Data Registry](https://registry.opendata.aws/indian-high-court-judgments/)
- **License**: [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/) — free to use, share, and adapt with attribution
- **Storage & transfer**: sponsored by AWS
- **Size**: ~1.1 TB across all years. See [STATS.md](STATS.md) for a breakdown by court and year.
- **Collaborate / ask questions**: [Discord](https://discord.gg/mQhghxCRJU)

> **Please scrape responsibly.** The eCourts portal is a public service — avoid high-concurrency scraping and be considerate of the maintainers. If you just want the data, use the AWS Open Data link above instead of running the scraper.

## Court codes

Court codes use the format `STATE~COURT` (e.g. `27~1` for Bombay). The canonical list lives in [`court-codes.json`](court-codes.json); this table is a human-readable mirror.

| Code  | Court |
|-------|-------|
| 9~13  | Allahabad High Court |
| 27~1  | Bombay High Court |
| 19~16 | Calcutta High Court |
| 18~6  | Gauhati High Court |
| 36~29 | High Court for State of Telangana |
| 28~2  | High Court of Andhra Pradesh |
| 22~18 | High Court of Chhattisgarh |
| 7~26  | High Court of Delhi |
| 24~17 | High Court of Gujarat |
| 2~5   | High Court of Himachal Pradesh |
| 1~12  | High Court of Jammu and Kashmir |
| 20~7  | High Court of Jharkhand |
| 29~3  | High Court of Karnataka |
| 32~4  | High Court of Kerala |
| 23~23 | High Court of Madhya Pradesh |
| 14~25 | High Court of Manipur |
| 17~21 | High Court of Meghalaya |
| 21~11 | High Court of Orissa |
| 3~22  | High Court of Punjab and Haryana |
| 8~9   | High Court of Rajasthan |
| 11~24 | High Court of Sikkim |
| 16~20 | High Court of Tripura |
| 5~15  | High Court of Uttarakhand |
| 33~10 | Madras High Court |
| 10~8  | Patna High Court |

## Running the scraper

The scraper is intended for contributors who maintain the dataset. End users should download from the AWS Open Data Registry link above.

### Install

```bash
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

PDF compression (optional) requires [Ghostscript](https://www.ghostscript.com/) on `PATH`; on macOS: `brew install ghostscript`.

### Usage

```bash
# Resume all courts from the last downloaded date per court
python download.py

# Specific court
python download.py --court_code "9~13"

# Specific date range across all courts
python download.py --start_date 2023-01-01 --end_date 2023-01-31

# Specific court + date range, chunked for parallelism
python download.py --court_code "9~13" --start_date 2023-01-01 --end_date 2023-12-31 --day_step 7
```

Features:
- Resumes from the last downloaded date per court when no range is given.
- Solves the eCourts arithmetic CAPTCHA automatically with an ONNX model.
- Compresses PDFs with Ghostscript (~50% average reduction) when `gs` is available.
- Parallelises across court × date chunks via a thread pool.

## Processing metadata

After downloading, `process_metadata.py` parses the raw HTML embedded in each JSON metadata file and writes a consolidated Parquet file (snappy-compressed):

```bash
python process_metadata.py
```

Output: `processed_metadata.parquet`, plus per-court intermediate files under `processed_data/`.

## Troubleshooting

- **CAPTCHA failures**: failed CAPTCHA images are saved to `./captcha-failures/` for debugging.
- **Session expiry**: handled automatically; the scraper refreshes its eCourts session every 25 downloads.
- **Download errors**: failed tasks are tracked and retried on subsequent runs.

## For contributors

See [AGENTS.md](AGENTS.md) for architecture, data partitioning conventions, known gotchas, and CI notes.
