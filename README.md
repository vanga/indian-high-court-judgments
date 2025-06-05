* All the data is available for download [here](https://registry.opendata.aws/indian-high-court-judgments/)
* The data is licensed under [Creative Commons Attribution 4.0 (CC-BY-4.0)](https://creativecommons.org/licenses/by/4.0/), which means you are free to use, share, and adapt the data as long as you provide appropriate attribution.
* AWS sponsors the storage and data transfer costs of the data.
* Join [discord server](https://discord.gg/mQhghxCRJU) if you want to collaborate on this repo or have any questions.
* **Be responsible, considerate, and think about the maintainers of the ecourts website. Avoid scraping with high concurrency.**
* **track.json** is broken and not working as expected.


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

## Court Codes

The following table lists the court codes used in the system:

| Code | Court Name |
|------|------------|
| 9~13 | Allahabad High Court |
| 27~1 | Bombay High Court |
| 19~16 | Calcutta High Court |
| 18~6 | Gauhati High Court |
| 36~29 | High Court for State of Telangana |
| 28~2 | High Court of Andhra Pradesh |
| 22~18 | High Court of Chhattisgarh |
| 7~26 | High Court of Delhi |
| 24~17 | High Court of Gujarat |
| 2~5 | High Court of Himachal Pradesh |
| 1~12 | High Court of Jammu and Kashmir |
| 20~7 | High Court of Jharkhand |
| 29~3 | High Court of Karnataka |
| 32~4 | High Court of Kerala |
| 23~23 | High Court of Madhya Pradesh |
| 14~25 | High Court of Manipur |
| 17~21 | High Court of Meghalaya |
| 21~11 | High Court of Orissa |
| 3~22 | High Court of Punjab and Haryana |
| 8~9 | High Court of Rajasthan |
| 11~24 | High Court of Sikkim |
| 16~20 | High Court of Tripura |
| 5~15 | High Court of Uttarakhand |
| 33~10 | Madras High Court |
| 10~8 | Patna High Court |


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