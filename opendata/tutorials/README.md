# High Court Judgments PDF Processor

This script processes tar archives containing PDF files of Indian High Court judgments stored in an AWS S3 bucket. It extracts text from these PDFs, converts them into a structured text format (JSON-like), compresses them into a `tar.gz` archive, and uploads the results to a specified output S3 bucket.

---

## 📦 Features

- Fetches `.tar` archives of PDFs from an input S3 bucket.
- Extracts PDFs from tar archives.
- Converts PDFs to plain text (page-wise).
- Structures text content into arrays with each page content as an element.
- Archives all processed text files into a `.tar.gz` archive.
- Uploads the resulting archive to an output S3 bucket.

---

## 📁 Input S3 Structure

The script expects tar files (`pdfs.tar`) stored in the following path structure within the source bucket:

```
data/tar/year=<YEAR>/court=<COURT>/bench=<BENCH>/pdfs.tar
```

Example:
```
data/tar/year=2023/court=11_24/bench=sikkimhc_pg/pdfs.tar
```

---

## 🏷 Output S3 Structure

The resulting text archive is uploaded to:

```
data/text-tars/year=<YEAR>/court=<COURT>/bench=<BENCH>/texts.tar.gz
```

---

## ⚙️ Requirements

- Python 3.9
- AWS credentials configured for `boto3` to access S3
- Required Python packages:
  - `boto3`
  - `PyPDF2`

Install dependencies:
```bash
pip install boto3 PyPDF2
```

---

## 🛠 Usage

Run the script from the command line:

```bash
python pdf_processor.py --output-bucket <OUTPUT_BUCKET_NAME> --year <YEAR> --court <COURT> --bench <BENCH>
```

### Example:

```bash
python pdf_processor.py --output-bucket processed-judgments --year 2024 --court 11_24 --bench sikkimhc_pg
```
Saves the processed files in a tar.gz file like data/text-tars/year=2024/court=11_24/bench=sikkimhc_pg/texts.tar.gz

```bash
python pdf_processor.py --output-bucket processed-judgments --year 2025 --court 1_12
```
Saves the processed files in a tar.gz file like

OUTPUT_BUCKET_NAME/data/text-tars/year=2025/court=1_12/bench=jammuhc/texts.tar.gz

OUTPUT_BUCKET_NAME/data/text-tars/year=2025/court=1_12/bench=kashmirhc/texts.tar.gz
### Arguments:

| Argument        | Mandatory | Description                                            |
|-----------------|----------|--------------------------------------------------------|
| `--output-bucket` | ✅      | Name of the S3 bucket where processed results are stored |
| `--year`         | ❌       | Filter PDFs by year                                    |
| `--court`        | ❌       | Filter PDFs by court                                   |
| `--bench`        | ❌       | Filter PDFs by bench                                   |

---

## 🧪 Processing Steps

1. **Find Tar Files**  
   Searches S3 for files ending in `pdfs.tar` matching the optional filters.

2. **Download + Extract**  
   Downloads and extracts tar files to a temporary local directory.

3. **Text Extraction**  
   Converts each PDF to a JSON-like list of strings (one per page).

4. **Archive & Upload**  
   Bundles all text files into `texts.tar.gz` and uploads to the destination bucket.

---

## 🔒 AWS Permissions

Ensure the AWS credentials used to run this script have the following S3 permissions:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:ListBucket",
    "s3:PutObject"
  ],
  "Resource": [
    "arn:aws:s3:::indian-high-court-judgments/*",
    "arn:aws:s3:::<OUTPUT_BUCKET_NAME>/*"
  ]
}
```

---

## 🧹 Cleanup

All temporary files and directories are automatically cleaned up after processing.

---

## Some Screenshots

![Screenshot 1](./screenshots/Screenshot%20from%202025-05-27%2017-19-24.png)
![Screenshot 2](./screenshots/Screenshot%20from%202025-05-27%2017-46-23.png)

---
