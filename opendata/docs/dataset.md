# Indian High Court Judgements

### Summary
This dataset contains judgements from the Indian High Courts, downloaded from [ecourts website](https://judgments.ecourts.gov.in/). It contains judgments of 25 high courts, along with raw metadata (in json format) and structured metadata (in parquet format). Judgments from the website are further compressed to optimize for size (care has been taken to not have any loss of data either in content or in visual appearance). Judgments are available as both individual pdf files and as tar files for easier download.

## Data
 * 25 high courts
 * ~16M judgments
 * ~1TB of data
 * Code used to download and process the data is [here](https://github.com/vanga/indian-high-court-judgments)

#### Update cadence
 * Once every quarter

### Structure of the data in the bucket
    * data/pdf/year=2025/court=xyz/bench=xyz/judgment1.pdf,judgment2.pdf
    * metadata/json/year=2025/court=xyz/bench=xyz/judgment1.json,judgment2.json
    * metadata/parquet/year=2025/court=xyz/bench=xyz/metadata.parquet
    * metadata/tar/year=2025/court=xyz/bench=xyz/metadata.tar.gz
    * data/tar/year=2025/court=xyz/bench=xyz/pdfs.tar
