# Indian Court Judgments Downloader

This script automates the downloading of court judgments from the Indian eCourts website (https://judgments.ecourts.gov.in). It supports parallel downloading of judgments from multiple courts and date ranges.

## Features

- Download judgments from specific courts or all available courts
- Specify date ranges for downloading or continue from last downloaded date
- Process date ranges in parallel for faster downloading
- Automatic CAPTCHA solving using OCR
- Robust error handling and session management
- Tracking of download progress

## Installation

1. Create and activate a virtual environment:
   ```bash
   python3.13 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the downloader:
   ```bash
   python download.py
   ```

## Usage

### Basic Usage

```bash
python download.py
```

This will download judgments from all courts, starting from the last downloaded date for each court (or from 2008-01-01 if no previous downloads).

### Downloading from a Specific Court

```bash
python download.py --court_code "9~13"
```

This will download judgments from the specified court code, starting from the last downloaded date.

### Downloading for a Specific Date Range

```bash
python download.py --start_date 2023-01-01 --end_date 2023-01-31
```

This will download judgments from all courts for the specified date range.

### Downloading from a Specific Court for a Specific Date Range

```bash
python download.py --court_code "9~13" --start_date 2023-01-01 --end_date 2023-01-31
```

### Controlling the Date Range Chunk Size

```bash
python download.py --court_code "9~13" --start_date 2023-01-01 --end_date 2023-12-31 --day_step 7
```

This will process the date range in 7-day chunks, which can be useful for parallel processing.

## How It Works

1. **Task Generation**: The script generates tasks based on court codes and date ranges. Each task represents a specific court and date range to process.

2. **Parallel Processing**: Tasks are processed in parallel using a thread pool, with a configurable number of worker threads.

3. **Session Management**: For each court and date range, the script:
   - Initializes a session with the eCourts website
   - Searches for judgments within the specified date range
   - Processes search results page by page
   - Downloads PDFs and associated metadata

4. **CAPTCHA Handling**: The script automatically solves CAPTCHAs using OCR:
   - Downloads CAPTCHA images
   - Uses EasyOCR to read the mathematical expressions
   - Solves the expressions and submits the answers
   - After 25 downloads, the script refreshes the session to avoid CAPTCHA challenges

5. **Progress Tracking**: The script maintains a tracking file (`track.json`) to record the last downloaded date for each court, allowing it to resume from where it left off.

## Output Structure

Downloaded judgments are stored in the `./data` directory, organized by their original path structure from the eCourts website. For each judgment, the script saves:

- The PDF file
- A JSON metadata file containing details about the judgment

Example directory structure:

data/
├── cnrorders/
│   ├── taphc/
│   │   ├── orders/
│   │   │   ├── 2017/
│   │   │   │   ├── HBHC010262202017_1_2047-06-29.pdf
│   │   │   │   └── HBHC010262202017_1_2047-06-29.json

## Processing Downloaded Metadata

After downloading judgments, you can process the metadata to extract structured information and generate a consolidated CSV file:

```bash
python process_metadata.py
```

This script:
1. Processes all JSON metadata files in the `./data` directory
2. Extracts structured information from the raw HTML
3. Outputs information about any metadata files without raw HTML

## Troubleshooting

- **CAPTCHA Failures**: If the script encounters issues with CAPTCHA solving, failed CAPTCHAs are saved in the `./captcha-failures` directory for debugging.
- **Session Expiry**: The script automatically handles session expiry by refreshing tokens and cookies.
- **Download Errors**: Failed downloads are tracked and can be retried in subsequent runs.
