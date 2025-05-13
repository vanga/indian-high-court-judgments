# Indian High Court Judgements

### Summary
This dataset contains judgements from the Indian High Courts, downloaded from ecourts website. It contains judgments of 25 high courts, along with raw metadata (in json format) and structured metadata (in parquet format). Judgments from the website are further compressed to optimize for size (care has been taken to not have any loss of data either in content or in visual appearance). Individual pdf files are also made available in addition to the tar files to make it easier to use services like AWS Glue to process the PDFs.

## Data
 * 25 high courts
 * ~16M judgments
 * ~1TB of data
 * Code used to download and process the data is [here](https://github.com/vanga/indian-high-court-judgments)
 * Judgments for which decision date couldn't be parsed have been categorized under `unknown` month

#### Update cadence
 * Once every month

### Structure of the data in the bucket
    * pdfs/court=xyz/month=2025-01/abc.pdf
    * json/court=xyz/month=2025-01/abc.json
    * parquet/court=xyz/month=2025-01/consolidated.parquet
    * archive/court=xyz/month=2025-01.tar