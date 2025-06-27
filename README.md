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


the amount of storage required to download the data 1) by courts 2)by years is mentioned below 

# Summary of Data
1. **Total Size by Court**
2. **Total Size by Year**
3. **Total Size**
---

## 1. Total Size by Court


| Code   | Court                             | Total Size (GB) |
| ------ | --------------------------------- | --------------: |
| 19~16  | Calcutta High Court               |           18.95 |
| 1~12   | High Court of Jammu and Kashmir   |           13.06 |
| 27~1   | Bombay High Court                 |           49.46 |
| 32~4   | High Court of Kerala              |           77.75 |
| 3~22   | High Court of Punjab and Haryana  |          118.65 |
| 5~15   | High Court of Uttarakhand         |            7.04 |
| 7~26   | High Court of Delhi               |            33.1 |
| 22~18  | High Court of Chhattisgarh        |           55.06 |
| 36~29  | High Court for State of Telangana |           93.66 |
| 10~8   | Patna High Court                  |           79.49 |
| 21~11  | High Court of Orissa              |           55.22 |
| 2~5    | High Court of Himachal Pradesh    |           17.75 |
| 24~17  | High Court of Gujarat             |           20.96 |
| 9~13   | Allahabad High Court              |           49.98 |
| 18~6   | Gauhati High Court                |           14.68 |
| 20~7   | High Court of Jharkhand           |           26.86 |
| 8~9    | High Court of Rajasthan           |           66.15 |
| 28~2   | High Court of Andhra Pradesh      |            45.8 |
| 33~10  | Madras High Court                 |          105.45 |
| 23~23  | High Court of Madhya Pradesh      |           41.83 |
| 29~3   | High Court of Karnataka           |           49.69 |
| 11~24  | High Court of Sikkim              |            0.63 |
| 16~20  | High Court of Tripura             |            3.47 |
| 14~25  | High Court of Manipur             |            1.64 |
| 17~21  | High Court of Meghalaya           |            1.28 |

---

## 2. Total Size by Year

| Year | Total Size (GB) | Total Size (MB) |
| :--- | --------------: | --------------: |
| 1950 |           0.00  |            3.90 |
| 1951 |           0.00  |            0.66 |
| 1952 |           0.00  |            0.74 |
| 1953 |           0.00  |            0.89 |
| 1954 |           0.00  |            2.16 |
| 1955 |           0.00  |            4.55 |
| 1956 |           0.00  |            2.34 |
| 1957 |           0.00  |            0.11 |
| 1958 |           0.00  |            2.70 |
| 1959 |           0.00  |            0.01 |
| 1960 |           0.00  |            0.22 |
| 1961 |           0.00  |            0.08 |
| 1962 |           0.00  |            0.04 |
| 1963 |           0.00  |            0.27 |
| 1964 |           0.00  |            0.07 |
| 1965 |           0.00  |            0.00 |
| 1966 |           0.00  |            0.06 |
| 1967 |           0.00  |            0.17 |
| 1968 |           0.00  |            0.11 |
| 1969 |           0.00  |            0.08 |
| 1970 |           0.00  |            0.42 |
| 1971 |           0.00  |            0.05 |
| 1972 |           0.00  |            0.06 |
| 1973 |           0.00  |            0.14 |
| 1974 |           0.00  |            0.09 |
| 1975 |           0.00  |            0.07 |
| 1976 |           0.00  |            0.35 |
| 1977 |           0.00  |            0.35 |
| 1978 |           0.01  |            9.76 |
| 1979 |           0.00  |            0.44 |
| 1980 |           0.00  |            0.98 |
| 1981 |           0.00  |            0.05 |
| 1982 |           0.00  |            1.26 |
| 1983 |           0.00  |            0.34 |
| 1984 |           0.00  |            0.24 |
| 1985 |           0.00  |            3.03 |
| 1986 |           0.00  |            0.59 |
| 1987 |           0.00  |            1.11 |
| 1988 |           0.00  |            0.89 |
| 1989 |           0.00  |            1.94 |
| 1990 |           0.00  |            2.17 |
| 1991 |           0.00  |            1.67 |
| 1992 |           0.00  |            2.93 |
| 1993 |           0.01  |            8.18 |
| 1994 |           0.00  |            3.69 |
| 1995 |           0.01  |            8.17 |
| 1996 |           0.03  |           33.33 |
| 1997 |           0.08  |           82.58 |
| 1998 |           0.21  |          214.13 |
| 1999 |           0.06  |           63.73 |
| 2000 |           0.11  |          109.75 |
| 2001 |           0.28  |          282.09 |
| 2002 |           0.26  |          262.11 |
| 2003 |           0.95  |          973.24 |
| 2004 |           1.96  |        2,002.42 |
| 2005 |           3.12  |        3,193.34 |
| 2006 |           5.17  |        5,299.09 |
| 2007 |          10.27  |       10,520.56 |
| 2008 |          12.58  |       12,883.90 |
| 2009 |          16.70  |       17,104.83 |
| 2010 |          22.42  |       22,961.82 |
| 2011 |          29.15  |       29,848.08 |
| 2012 |          29.70  |       30,413.75 |
| 2013 |          29.57  |       30,279.16 |
| 2014 |          33.52  |       34,323.10 |
| 2015 |          38.48  |       39,401.08 |
| 2016 |          40.82  |       41,802.38 |
| 2017 |          45.66  |       46,750.77 |
| 2018 |          66.72  |       68,319.69 |
| 2019 |          76.93  |       78,775.25 |
| 2020 |          56.91  |       58,278.01 |
| 2021 |          87.28  |       89,377.28 |
| 2022 |         134.37  |      137,597.49 |
| 2023 |         145.52  |      149,008.66 |
| 2024 |         134.55  |      137,782.18 |
| 2025 |          24.13  |       24,758.98 |

---

## 3. Total Across All Years

* **Total Size:** 1,072,716.02 MB
* **Converted to GB:** 1,047.66 GB  <BR> (using 1 GB = 1,024 MB)
* **Converted to TB:** 1.023 TB     <BR>   (using 1 TB = 1,024*1,024 MB)

## Key Insights

| Insight                          | Value                                                       |
| :------------------------------  | :---------------------------------------------------------  |
| Year with highest data volume     | **2023** (145.516 GB)                                      |
| Court with highest data volume    | **3~22** (High Court of Punjab and Haryana) (118.646 GB)   |
| Total data volume across all years| **1,047.66 GB**                                            |
| Total data volume across all years (TB) | **1.023 TB**                                         |


