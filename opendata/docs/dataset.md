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


### Example usage
* [Example script](../tutorials/README.md) for downloading the data in bulk and parsing to get text from the pdfs 
* [Querying the metadata using AWS Athena](../tutorials/README.md)
* Example command to download a court data fully from S3 `aws s3 sync s3://indian-high-court-judgments/data/tar/ ./data/tar/ --exclude "*" --include "*/court=27_1/*" --no-sign-request` - You do not need AWS account or credentials to do this, you just need to have [aws cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) installed.
* Court codes and bench codes can be referred from [here](./high_courts.csv) - Make sure to replace the ~ with _ in the court code while constructing the paths.
* Since the S3 bucket is public, it can be even downloaded using links like `https://indian-high-court-judgments.s3.ap-south-1.amazonaws.com/data/tar/year=2024/court=27_1/bench=hcaurdb/pdfs.tar`