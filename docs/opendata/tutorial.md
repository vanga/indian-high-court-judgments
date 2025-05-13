### Accessing metadata using AWS Athena

AWS Athena allows you to query the court case data stored in parquet files using standard SQL. Follow these steps to set up and run queries on the metadata:

1. **Sign in to the AWS Management Console** and navigate to the Athena service.

2. **Set up a query result location** if you haven't already:
   - Go to "Settings"
   - Configure an S3 bucket to store your query results (e.g., `s3://your-bucket/athena-results/`)
   - Click "Save"

3. **Create a database** for the court case data:
   ```sql
   CREATE DATABASE court_cases;
   ```

4. **Create a table** that maps to the parquet files:
   ```sql
   CREATE EXTERNAL TABLE court_cases.judgments (
     court_code STRING,
     title STRING,
     description STRING,
     judge STRING,
     pdf_link STRING,
     cnr STRING,
     date_of_registration STRING,
     decision_date STRING,
     disposal_nature STRING,
     court STRING,
     size BIGINT,
     file_type STRING,
     mime_type STRING,
     pdf_version DOUBLE,
     pdf_linearized BOOLEAN,
     pdf_pages BIGINT,
     pdf_producer STRING,
     pdf_language STRING
   )
   PARTITIONED BY (court_partition STRING, month STRING)
   STORED AS PARQUET
   LOCATION 's3://your-bucket/parquet/'
   TBLPROPERTIES ('has_encrypted_data'='false');
   ```

5. **Load partitions** to make Athena aware of the data structure:
   ```sql
   MSCK REPAIR TABLE court_cases.judgments;
   ```
   Alternatively, you can add partitions manually:
   ```sql
   ALTER TABLE court_cases.judgments ADD PARTITION (court_partition='xyz', month='2025-01');
   ```

6. **Run queries** on the data. Here are some example queries:

   - Count cases by court:
     ```sql
     SELECT court, COUNT(*) as case_count
     FROM court_cases.judgments
     GROUP BY court
     ORDER BY case_count DESC;
     ```

   - Find cases by a specific judge:
     ```sql
     SELECT title, decision_date, court
     FROM court_cases.judgments
     WHERE judge LIKE '%Smith%'
     ORDER BY decision_date DESC;
     ```

   - Get cases from a specific time period:
     ```sql
     SELECT title, court, decision_date
     FROM court_cases.judgments
     WHERE month = '2025-01'
     AND decision_date BETWEEN '2025-01-01' AND '2025-01-31'
     ORDER BY decision_date;
     ```

   - Find cases with specific keywords in the description:
     ```sql
     SELECT title, court, decision_date
     FROM court_cases.judgments
     WHERE description LIKE '%property dispute%'
     ORDER BY decision_date DESC
     LIMIT 100;
     ```

7. **Optimize your queries** for better performance:
   - Use partitioning predicates (court_partition, month) in your WHERE clause
   - Limit the number of columns in your SELECT statement
   - Use LIMIT to restrict large result sets

8. **Export results** by clicking "Download results" after your query completes, or configure Athena to automatically save results to your S3 bucket.

Note: Replace `your-bucket` with the actual S3 bucket name where the parquet files are stored. The table structure assumes the parquet files are located at `s3://your-bucket/parquet/court=xyz/month=2025-01/consolidated.parquet` following the partitioning scheme shown in the dataset structure.





