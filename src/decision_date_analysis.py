#!/usr/bin/env python3
"""
Script to analyze S3 partitioning correctness by comparing actual decision dates
with S3 partition years in parquet files.

This script:
1. Lists all parquet files in S3
2. Downloads and reads parquet files
3. Extracts decision dates from raw_html
4. Compares actual decision dates with S3 partition years
5. Reports mismatches
"""

import boto3
import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime
import re
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm
import json
from lxml import html

# Import the same HTML parsing logic used in the codebase
from html_utils import parse_decision_date_from_html

# S3 configuration
S3_BUCKET = "indian-high-court-judgments"
S3_PREFIX = "metadata/parquet/"


class S3PartitionAnalyzer:
    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.mismatches = []
        self.stats = {
            "total_files": 0,
            "total_records": 0,
            "records_with_decision_dates": 0,
            "mismatched_partitions": 0,
            "parsing_errors": 0,
        }

    def list_parquet_files(self) -> List[str]:
        """List all parquet files in S3"""
        print("Listing parquet files in S3...")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        parquet_files = []

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(".parquet"):
                        parquet_files.append(obj["Key"])

        print("Found {} parquet files".format(len(parquet_files)))
        return parquet_files

    def extract_partition_info(self, s3_key: str) -> Tuple[Optional[int], str, str]:
        """
        Extract year, court_code, and bench from S3 key path.
        Expected format: metadata/parquet/year=YYYY/court=CODE/bench=BENCH/metadata.parquet
        """
        # Parse path like: metadata/parquet/year=2024/court=27_1/bench=hcaurdb/metadata.parquet
        year_match = re.search(r"year=(\d{4})", s3_key)
        court_match = re.search(r"court=([^/]+)", s3_key)
        bench_match = re.search(r"bench=([^/]+)", s3_key)

        year = int(year_match.group(1)) if year_match else None
        court_code = court_match.group(1) if court_match else "unknown"
        bench = bench_match.group(1) if bench_match else "unknown"

        return year, court_code, bench

    def download_and_read_parquet(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Download and read a parquet file from S3"""
        try:
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ) as tmp_file:
                self.s3_client.download_file(S3_BUCKET, s3_key, tmp_file.name)
                df = pd.read_parquet(tmp_file.name)
                Path(tmp_file.name).unlink()  # Clean up
                return df
        except Exception as e:
            print("Error reading {}: {}".format(s3_key, e))
            return None

    def extract_decision_date_from_record(
        self, record: pd.Series
    ) -> Tuple[Optional[datetime], Optional[int]]:
        """
        Extract decision date from a parquet record.
        Returns both the full datetime and year for comprehensive validation.
        Uses the same logic as the codebase.
        """
        try:
            # Get raw_html from the record
            raw_html = record.get("raw_html", "")
            if not raw_html or pd.isna(raw_html):
                return None, None

            # Use the same parsing logic as the codebase
            decision_date, decision_year = parse_decision_date_from_html(raw_html)
            return decision_date, decision_year

        except Exception as e:
            print(f"Error parsing decision date: {e}")
            return None, None

    def validate_decision_date(
        self, decision_date: Optional[datetime], partition_year: int
    ) -> Dict:
        """
        Validate if the decision date makes sense and detect potential date format issues.
        Returns validation results including potential format swaps.
        """
        if decision_date is None:
            return {"status": "no_date", "issues": []}

        validation: Dict[str, any] = {
            "status": "valid",
            "issues": [],
            "potential_format_swap": None,
            "date_components": {
                "year": decision_date.year,
                "month": decision_date.month,
                "day": decision_date.day,
            },
        }

        # Check if the date is reasonable (not too far in future/past)
        current_year = datetime.now().year
        if decision_date.year < 1950 or decision_date.year > current_year + 1:
            validation["issues"].append(f"Unrealistic year: {decision_date.year}")
            validation["status"] = "suspicious"

        # Check for potential MM/DD vs DD/MM format swap
        # If month > 12, it might be a DD/MM format interpreted as MM/DD
        if decision_date.month > 12:
            validation["issues"].append(
                f"Month > 12: {decision_date.month} (possible DD/MM format swap)"
            )
            validation["status"] = "format_issue"
            validation["potential_format_swap"] = "DD/MM_interpreted_as_MM/DD"

        # Check for potential DD/MM vs MM/DD format swap
        # If day > 12 and month <= 12, it might be a MM/DD format interpreted as DD/MM
        elif decision_date.day > 12 and decision_date.month <= 12:
            # This is less clear-cut, but worth flagging for manual review
            validation["issues"].append(
                f"Day > 12: {decision_date.day} (possible MM/DD vs DD/MM confusion)"
            )
            validation["status"] = "needs_review"
            validation["potential_format_swap"] = "MM/DD_vs_DD/MM_ambiguous"

        # Check if the date is in the correct partition year range
        # Allow some tolerance for edge cases (e.g., year boundary issues)
        year_diff = abs(decision_date.year - partition_year)
        if year_diff > 1:
            validation["issues"].append(
                f"Large year difference: {year_diff} years from partition"
            )
            validation["status"] = "major_mismatch"

        return validation

    def analyze_parquet_file(self, s3_key: str) -> Dict:
        """Analyze a single parquet file for partitioning mismatches"""
        print(f"Analyzing {s3_key}...")

        # Extract partition info from S3 path
        partition_year, court_code, bench = self.extract_partition_info(s3_key)
        if partition_year is None:
            print("Could not extract year from path: {}".format(s3_key))
            return {"error": "Could not extract year from path"}

        # Download and read parquet
        df = self.download_and_read_parquet(s3_key)
        if df is None:
            return {"error": "Could not read parquet file"}

        print(
            "  Processing {} records from {}/{} (partition year: {})".format(
                len(df), court_code, bench, partition_year
            )
        )

        # Analyze each record
        mismatches = []
        records_with_dates = 0

        for idx, record in df.iterrows():
            decision_date, decision_year = self.extract_decision_date_from_record(
                record
            )

            if decision_year is not None:
                records_with_dates += 1

                # Check if decision year matches partition year
                if decision_year != partition_year:
                    # Additional validation: check if the actual date makes sense
                    # This helps detect date format issues (MM/DD vs DD/MM)
                    date_validation = self.validate_decision_date(
                        decision_date, partition_year
                    )

                    mismatch = {
                        "s3_key": s3_key,
                        "record_index": idx,
                        "partition_year": partition_year,
                        "actual_decision_year": decision_year,
                        "actual_decision_date": decision_date.isoformat()
                        if decision_date
                        else None,
                        "court_code": court_code,
                        "bench": bench,
                        "cnr": record.get("cnr", "N/A"),
                        "title": record.get("title", "N/A")[:100] + "..."
                        if len(str(record.get("title", ""))) > 100
                        else record.get("title", "N/A"),
                        "date_validation": date_validation,
                    }
                    mismatches.append(mismatch)

        return {
            "s3_key": s3_key,
            "partition_year": partition_year,
            "court_code": court_code,
            "bench": bench,
            "total_records": len(df),
            "records_with_decision_dates": records_with_dates,
            "mismatches": mismatches,
            "mismatch_count": len(mismatches),
        }

    def analyze_all_files(self, max_files: Optional[int] = None):
        """Analyze all parquet files in S3"""
        parquet_files = self.list_parquet_files()

        if max_files:
            parquet_files = parquet_files[:max_files]
            print(f"Limiting analysis to first {max_files} files")

        results = []

        for s3_key in tqdm(parquet_files, desc="Analyzing parquet files"):
            result = self.analyze_parquet_file(s3_key)
            results.append(result)

            # Update stats
            self.stats["total_files"] += 1
            if "total_records" in result:
                self.stats["total_records"] += result["total_records"]
                self.stats["records_with_decision_dates"] += result[
                    "records_with_decision_dates"
                ]
                self.stats["mismatched_partitions"] += result["mismatch_count"]

            if "error" in result:
                self.stats["parsing_errors"] += 1

        return results

    def generate_report(
        self,
        results: List[Dict],
        output_file: str = "partitioning_analysis_report.json",
    ):
        """Generate a detailed report of the analysis"""

        # Collect all mismatches
        all_mismatches = []
        for result in results:
            if "mismatches" in result:
                all_mismatches.extend(result["mismatches"])

        # Generate summary
        summary = {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_parquet_files": self.stats["total_files"],
            "total_records_analyzed": self.stats["total_records"],
            "records_with_decision_dates": self.stats["records_with_decision_dates"],
            "total_mismatches_found": len(all_mismatches),
            "files_with_mismatches": len(
                [r for r in results if r.get("mismatch_count", 0) > 0]
            ),
            "parsing_errors": self.stats["parsing_errors"],
        }

        # Group mismatches by partition year
        mismatches_by_partition: Dict[int, List[Dict]] = {}
        format_issues = []
        suspicious_dates = []

        for mismatch in all_mismatches:
            partition_year = mismatch["partition_year"]
            if partition_year not in mismatches_by_partition:
                mismatches_by_partition[partition_year] = []
            mismatches_by_partition[partition_year].append(mismatch)

            # Categorize by validation status
            validation = mismatch.get("date_validation", {})
            if validation.get("status") == "format_issue":
                format_issues.append(mismatch)
            elif validation.get("status") == "suspicious":
                suspicious_dates.append(mismatch)

        # Generate detailed report
        report = {
            "summary": summary,
            "mismatches_by_partition_year": mismatches_by_partition,
            "format_issues": format_issues,
            "suspicious_dates": suspicious_dates,
            "detailed_results": results,
        }

        # Save to file
        with open(output_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\nAnalysis complete!")
        print(f"Total files analyzed: {summary['total_parquet_files']}")
        print(f"Total records: {summary['total_records_analyzed']}")
        print(f"Records with decision dates: {summary['records_with_decision_dates']}")
        print(f"Total mismatches found: {summary['total_mismatches_found']}")
        print(f"Files with mismatches: {summary['files_with_mismatches']}")
        print(f"Format issues detected: {len(format_issues)}")
        print(f"Suspicious dates: {len(suspicious_dates)}")
        print(f"Detailed report saved to: {output_file}")

        # Print some examples of mismatches with validation details
        if all_mismatches:
            print(f"\nFirst 5 mismatches found:")
            for i, mismatch in enumerate(all_mismatches[:5]):
                validation = mismatch.get("date_validation", {})
                status = validation.get("status", "unknown")
                issues = validation.get("issues", [])

                print(
                    f"  {i + 1}. Partition: {mismatch['partition_year']}, "
                    f"Actual: {mismatch['actual_decision_year']}, "
                    f"Court: {mismatch['court_code']}/{mismatch['bench']}"
                )
                if status != "valid":
                    print(f"      Status: {status}")
                    if issues:
                        print(f"      Issues: {', '.join(issues)}")
                if mismatch.get("actual_decision_date"):
                    print(f"      Full date: {mismatch['actual_decision_date']}")

        # Print format issues summary
        if format_issues:
            print(f"\n⚠️  FORMAT ISSUES DETECTED:")
            print(
                f"Found {len(format_issues)} records with potential date format problems"
            )
            print("These may be due to MM/DD vs DD/MM format confusion during parsing")

        if suspicious_dates:
            print(f"\n⚠️  SUSPICIOUS DATES DETECTED:")
            print(f"Found {len(suspicious_dates)} records with unrealistic dates")
            print("These dates may indicate parsing errors or data quality issues")

        return report


def main():
    """Main function to run the analysis"""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze S3 partitioning correctness")
    parser.add_argument(
        "--max-files", type=int, help="Maximum number of parquet files to analyze"
    )
    parser.add_argument(
        "--output",
        default="partitioning_analysis_report.json",
        help="Output file for the analysis report",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Analyze only a small sample of files for testing",
    )

    args = parser.parse_args()

    if args.sample:
        args.max_files = 5
        print("Running in sample mode - analyzing only 5 files")

    analyzer = S3PartitionAnalyzer()

    try:
        results = analyzer.analyze_all_files(max_files=args.max_files)
        report = analyzer.generate_report(results, args.output)

        # Print summary to console
        summary = report["summary"]
        format_issues = report.get("format_issues", [])
        suspicious_dates = report.get("suspicious_dates", [])

        print(f"\n=== PARTITIONING ANALYSIS SUMMARY ===")
        print(f"Files analyzed: {summary['total_parquet_files']}")
        print(f"Records analyzed: {summary['total_records_analyzed']}")
        print(f"Records with decision dates: {summary['records_with_decision_dates']}")
        print(f"Total mismatches: {summary['total_mismatches_found']}")
        print(f"Files with mismatches: {summary['files_with_mismatches']}")
        print(f"Format issues: {len(format_issues)}")
        print(f"Suspicious dates: {len(suspicious_dates)}")

        if summary["total_mismatches_found"] > 0:
            print(
                f"\n⚠️  WARNING: Found {summary['total_mismatches_found']} records with incorrect partitioning!"
            )
            print(
                "These records are stored in the wrong year partition based on their actual decision dates."
            )

            if format_issues:
                print(
                    f"\n📅 DATE FORMAT ISSUES: {len(format_issues)} records have potential date format problems"
                )
                print(
                    "These may be due to MM/DD vs DD/MM format confusion during parsing"
                )

            if suspicious_dates:
                print(
                    f"\n🔍 SUSPICIOUS DATES: {len(suspicious_dates)} records have unrealistic dates"
                )
                print("These may indicate parsing errors or data quality issues")
        else:
            print(f"\n✅ All analyzed records are correctly partitioned!")

    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
