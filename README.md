* All the data is available for download [here](https://registry.opendata.aws/indian-high-court-judgments/)
* The data is licensed under [Creative Commons Attribution 4.0 (CC-BY-4.0)](https://creativecommons.org/licenses/by/4.0/), which means you are free to use, share, and adapt the data as long as you provide appropriate attribution.
* AWS sponsors the storage and data transfer costs of the data.
* Join [discord server](https://discord.gg/mQhghxCRJU) if you want to collaborate on this repo or have any questions.
* **Be responsible, considerate, and think about the maintainers of the ecourts website. Avoid scraping with high concurrency.**


# Indian Court Judgments Downloader

This script automates the downloading of court judgments from the Indian eCourts website (https://judgments.ecourts.gov.in). It supports parallel downloading of judgments from multiple courts and date ranges.

## Features

- Download judgments from specific courts or all available courts
- Specify date ranges for downloading or continue from last downloaded date
- Process date ranges in parallel for faster downloading
- Automatic CAPTCHA solving using OCR
- compresses PDFs using Ghostscript (on an average, we get about 50% compression)

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

5. **Date Range Processing**: The script processes judgments for the specified date range, with support for parallel processing across multiple courts.

## File Structure

Downloaded judgments are stored in the `./data` directory locally, organized by their original path structure from the eCourts website. For each judgment, the script saves:

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
| 3~22   | High Court of Punjab and Haryana  |          137.17 |
| 33~10  | Madras High Court                 |          114.88 |
| 36~29  | High Court for State of Telangana |           92.80 |
| 28~2   | High Court of Andhra Pradesh      |           87.84 |
| 10~8   | Patna High Court                  |           74.23 |
| 32~4   | High Court of Kerala              |           72.25 |
| 8~9    | High Court of Rajasthan           |           65.38 |
| 21~11  | High Court of Orissa              |           65.03 |
| 9~13   | Allahabad High Court              |           63.64 |
| 22~18  | High Court of Chhattisgarh        |           56.08 |
| 27~1   | Bombay High Court                 |           53.13 |
| 29~3   | High Court of Karnataka           |           47.76 |
| 23~23  | High Court of Madhya Pradesh      |           37.87 |
| 7~26   | High Court of Delhi               |           34.19 |
| 20~7   | High Court of Jharkhand           |           29.28 |
| 19~16  | Calcutta High Court               |           21.67 |
| 24~17  | High Court of Gujarat             |           19.68 |
| 2~5    | High Court of Himachal Pradesh    |           19.51 |
| 18~6   | Gauhati High Court                |           16.01 |
| 1~12   | High Court of Jammu and Kashmir   |           14.29 |
| 5~15   | High Court of Uttarakhand         |            6.41 |
| 16~20  | High Court of Tripura             |            3.61 |
| 14~25  | High Court of Manipur             |            2.10 |
| 17~21  | High Court of Meghalaya           |            1.28 |
| 11~24  | High Court of Sikkim              |            0.62 |
---

## 2. Total Size by Year

| Year | Total Size (GB) | Total Size (MB) |
| :--- | --------------: | --------------: |
| 1950 |            0.00  |            2.87 |
| 1951 |            0.00  |            0.63 |
| 1952 |            0.00  |            0.69 |
| 1953 |            0.00  |            0.85 |
| 1954 |            0.00  |            2.06 |
| 1955 |            0.00  |            4.31 |
| 1956 |            0.00  |            2.23 |
| 1957 |            0.00  |            0.10 |
| 1958 |            0.00  |            2.57 |
| 1959 |            0.00  |            0.01 |
| 1960 |            0.00  |            0.20 |
| 1961 |            0.00  |            0.06 |
| 1962 |            0.00  |            0.04 |
| 1963 |            0.00  |            0.25 |
| 1964 |            0.00  |            0.06 |
| 1965 |            0.00  |            0.00 |
| 1966 |            0.00  |            0.05 |
| 1967 |            0.00  |            0.15 |
| 1968 |            0.00  |            0.10 |
| 1969 |            0.00  |            0.07 |
| 1970 |            0.00  |            0.39 |
| 1971 |            0.00  |            0.05 |
| 1972 |            0.00  |            0.05 |
| 1973 |            0.00  |            0.13 |
| 1974 |            0.00  |            0.08 |
| 1975 |            0.00  |            0.06 |
| 1976 |            0.00  |            0.33 |
| 1977 |            0.00  |            0.33 |
| 1978 |            0.01  |            9.33 |
| 1979 |            0.00  |            0.41 |
| 1980 |            0.00  |            0.92 |
| 1981 |            0.00  |            0.04 |
| 1982 |            0.00  |            1.19 |
| 1983 |            0.00  |            0.28 |
| 1984 |            0.00  |            0.20 |
| 1985 |            0.00  |            2.84 |
| 1986 |            0.00  |            0.54 |
| 1987 |            0.00  |            1.01 |
| 1988 |            0.00  |            0.81 |
| 1989 |            0.00  |            1.88 |
| 1990 |            0.00  |            1.94 |
| 1991 |            0.00  |            1.51 |
| 1992 |            0.00  |            2.62 |
| 1993 |            0.01  |           12.55 |
| 1994 |            0.00  |            3.28 |
| 1995 |            0.01  |            7.27 |
| 1996 |            0.03  |           27.10 |
| 1997 |            0.05  |           46.92 |
| 1998 |            0.20  |          200.13 |
| 1999 |            0.05  |           52.14 |
| 2000 |            0.09  |           91.01 |
| 2001 |            0.25  |          251.45 |
| 2002 |            0.22  |          224.48 |
| 2003 |            0.87  |          893.45 |
| 2004 |            1.56  |        1,595.21 |
| 2005 |            2.71  |        2,775.17 |
| 2006 |            4.62  |        4,731.73 |
| 2007 |            9.42  |        9,643.81 |
| 2008 |           11.27  |       11,544.32 |
| 2009 |           15.32  |       15,682.86 |
| 2010 |           20.85  |       21,353.52 |
| 2011 |           27.25  |       27,906.17 |
| 2012 |           27.56  |       28,225.60 |
| 2013 |           27.46  |       28,116.69 |
| 2014 |           31.05  |       31,794.33 |
| 2015 |           35.31  |       36,158.40 |
| 2016 |           37.03  |       37,923.79 |
| 2017 |           41.30  |       42,290.69 |
| 2018 |           62.14  |       63,628.12 |
| 2019 |           72.37  |       74,109.02 |
| 2020 |           53.07  |       54,348.66 |
| 2021 |           81.23  |       83,182.78 |
| 2022 |          124.62  |      127,610.14 |
| 2023 |          134.76  |      137,992.01 |
| 2024 |          124.78  |      127,773.17 |
| 2025 |          189.20  |      193,745.54 |

---

## 3. Total Across All Years

* **Total Size:** 1,163,985.75 MB
* **Converted to GB:** 1,136.70 GB  <BR> (using 1 GB = 1,024 MB)
* **Converted to TB:** 1.110 TB     <BR>   (using 1 TB = 1,024*1,024 MB)

## Key Insights

| Insight                          | Value                                                       |
| :------------------------------  | :---------------------------------------------------------  |
| Year with highest data volume     | **2025** (189.20 GB)                                       |
| Court with highest data volume    | **3~22** (High Court of Punjab and Haryana) (137.17 GB)   |
| Total data volume across all years| **1,136.70 GB**                                            |
| Total data volume across all years (TB) | **1.110 TB**                                         |


