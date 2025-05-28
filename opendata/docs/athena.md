# AWS Athena Tutorial for Indian High Court Judgments

This guide walks you through querying Indian High Court judgment metadata stored in Parquet format on Amazon S3 using AWS Athena.

---

## 🏗️ Setup

### Create a Database

```sql
CREATE DATABASE court_cases;
```

### Create a Table for Parquet Files with Partitioning

```sql
CREATE EXTERNAL TABLE court_cases.judgments ( court_code STRING, title STRING, description STRING, judge STRING, pdf_link STRING, cnr STRING, date_of_registration STRING, decision_date TIMESTAMP, disposal_nature STRING, court_name STRING
)
PARTITIONED BY ( year STRING, court STRING)
STORED AS PARQUET
LOCATION 's3://indian-high-court-judgments/metadata/parquet/'
TBLPROPERTIES ( 'has_encrypted_data'='false',
  'projection.enabled'='true',
  'projection.year.type'='integer',
  'projection.year.range'='1950,2025',
  'projection.court.type'='enum',
  'projection.court.values'='9_13,27_1,19_16,18_6,36_29,28_2,22_18,7_26,24_17,2_5,1_12,20_7,29_3,32_4,23_23,14_25,17_21,21_11,3_22,8_9,11_24,16_20,5_15,33_10,10_8',
  'storage.location.template'='s3://indian-high-court-judgments/metadata/parquet/year=${year}/court=${court}/'
)
```

## 🔍 Query Examples

### Level 1: Basic Selection

```sql
SELECT 
  court_code,
  title,
  judge,
  date_of_registration,
  decision_date,
  court_name,
  year,
  court
FROM 
  court_cases.judgments
WHERE 
  year = '2025'
  AND court = '32_4'
LIMIT 10;
```

### Level 2: Sorted Results

```sql
SELECT 
  title,
  decision_date,
  judge
FROM 
  court_cases.judgments
WHERE 
  year = '2025'
ORDER BY 
  decision_date DESC
LIMIT 10;
```

### Level 3: Pattern Matching with `LIKE`

```sql
SELECT 
  title,
  judge,
  court_name
FROM 
  court_cases.judgments
WHERE 
  judge LIKE '%KRISHNA%'
  AND year = '2025'
LIMIT 10;
```

### Level 4: Aggregation with `GROUP BY`

```sql
SELECT 
  judge,
  COUNT(*) AS total_cases
FROM 
  court_cases.judgments
WHERE 
  year = '2025'
GROUP BY 
  judge
ORDER BY 
  total_cases DESC
LIMIT 5;
```

### Level 5: `HAVING` Clause

```sql
SELECT 
  court_name,
  COUNT(*) AS total_judgments
FROM 
  court_cases.judgments
WHERE 
  year = '2025'
GROUP BY 
  court_name
HAVING 
  COUNT(*) > 10
ORDER BY 
  total_judgments DESC;
```

### Level 6: Subquery in `WHERE` Clause

```sql
SELECT 
  title,
  judge,
  court_name
FROM 
  court_cases.judgments
WHERE 
  judge IN (
    SELECT 
      judge
    FROM 
      court_cases.judgments
    WHERE 
      court = '32_4'
    GROUP BY 
      judge
    HAVING 
      COUNT(*) > 5
  )
AND year = '2025';
```

### Level 7: Window Function (RANK)

```sql
SELECT 
  title,
  judge,
  court_name,
  decision_date,
  RANK() OVER (PARTITION BY court_name ORDER BY decision_date DESC) AS recent_rank
FROM 
  court_cases.judgments
WHERE 
  year = '2025';
```

---

### The year starts from 1950 to April 2025


## 🧭 Court Codes and Bench Mapping

| court_name                           | court_code | bench_name                           |
|---------------------------------------|------------|--------------------------------------|
| Allahabad High Court                  | 9_13       | cisdb_16012018                       |
| Allahabad High Court                  | 9_13       | cishclko                             |
| Bombay High Court                     | 27_1       | newos_spl                            |
| Bombay High Court                     | 27_1       | hcbgoa                               |
| Bombay High Court                     | 27_1       | testcase                             |
| Bombay High Court                     | 27_1       | hcaurdb                              |
| Bombay High Court                     | 27_1       | newos                                |
| Bombay High Court                     | 27_1       | newas                                |
| Calcutta High Court                   | 19_16      | calcutta_appellate_side              |
| Calcutta High Court                   | 19_16      | calcutta_circuit_bench_at_jalpaiguri |
| Calcutta High Court                   | 19_16      | calcutta_original_side               |
| Calcutta High Court                   | 19_16      | calcutta_circuit_bench_at_port_blair |
| Gauhati High Court                    | 18_6       | nlghccis                             |
| Gauhati High Court                    | 18_6       | azghccis                             |
| Gauhati High Court                    | 18_6       | arghccis                             |
| Gauhati High Court                    | 18_6       | asghccis                             |
| High Court for State of Telangana     | 36_29      | taphc                                |
| High Court of Andhra Pradesh          | 28_2       | aphc                                 |
| High Court of Chhattisgarh            | 22_18      | cghccisdb                            |
| High Court of Delhi                   | 7_26       | dhcdb                                |
| High Court of Gujarat                 | 24_17      | gujarathc                            |
| High Court of Himachal Pradesh        | 2_5        | cmis                                 |
| High Court of Jammu and Kashmir       | 1_12       | jammuhc                              |
| High Court of Jammu and Kashmir       | 1_12       | kashmirhc                            |
| High Court of Jharkhand               | 20_7       | jhar_pg                              |
| High Court of Karnataka               | 29_3       | karhcdharwad                         |
| High Court of Karnataka               | 29_3       | karnataka_bng_old                    |
| High Court of Karnataka               | 29_3       | karhckalaburagi                      |
| High Court of Kerala                  | 32_4       | highcourtofkerala                    |
| High Court of Madhya Pradesh          | 23_23      | mphc_db_ind                          |
| High Court of Madhya Pradesh          | 23_23      | mphc_db_jbp                          |
| High Court of Madhya Pradesh          | 23_23      | mphc_db_gwl                          |
| High Court of Manipur                 | 14_25      | manipurhc_pg                         |
| High Court of Meghalaya               | 17_21      | meghalaya                            |
| High Court of Orissa                  | 21_11      | cisnc                                |
| High Court of Punjab and Haryana      | 3_22       | phhc                                 |
| High Court of Rajasthan               | 8_9        | rhcjodh240618                        |
| High Court of Rajasthan               | 8_9        | jaipur                               |
| High Court of Sikkim                  | 11_24      | sikkimhc_pg                          |
| High Court of Tripura                 | 16_20      | thcnc                                |
| High Court of Uttarakhand             | 5_15       | ukhcucis_pg                          |
| Madras High Court                     | 33_10      | hc_cis_mas                           |
| Madras High Court                     | 33_10      | mdubench                             |
| Patna High Court                      | 10_8       | patnahcucisdb94                      |


---
