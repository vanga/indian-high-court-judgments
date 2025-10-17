#!/usr/bin/env python3
"""
Initialize index.json files in S3 bucket for Indian High Court Judgments

This script creates data.index.json and metadata.index.json files for all court/bench
combinations in the S3 bucket structure. It handles both existing data (with actual
file counts and sizes) and missing combinations (with empty arrays and zero values).

The script will:
1. Scan existing S3 structure for courts and benches
2. Find latest dates for each court from filename patterns
3. Create index files for all court/bench combinations from court-codes.json and bench-codes.json
4. Handle missing courts/benches with empty index files
"""

import json
import boto3
import os
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Set, List, Tuple, Optional
from tqdm import tqdm
from botocore import UNSIGNED
from botocore.client import Config
from models import IndexFile


class S3IndexInitializer:
    """Initialize index.json files in S3 bucket for court judgments data"""

    def __init__(self, bucket_name: str, year: str = None):
        """
        Initialize the S3 index generator

        Args:
            bucket_name: S3 bucket name
            year: Year to process (defaults to current year)
        """
        self.bucket_name = bucket_name
        self.year = year or str(datetime.now().year)

        # Initialize S3 clients
        self.s3_client = boto3.client("s3")
        self.s3_unsigned = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # Load court and bench codes
        self.court_codes = self._load_court_codes()
        self.bench_codes = self._load_bench_codes()

        # Create reverse mapping from bench names to court codes
        self.bench_to_court = self._create_bench_to_court_mapping()

        print(
            f"Initialized S3IndexInitializer for bucket: {bucket_name}, year: {self.year}"
        )
        print(
            f"Loaded {len(self.court_codes)} courts and {len(self.bench_codes)} benches"
        )

    def _load_court_codes(self) -> Dict[str, str]:
        """Load court codes from court-codes.json"""
        try:
            with open("court-codes.json", "r") as f:
                court_codes = json.load(f)
                # Convert ~ to _ in court codes for S3 paths
                return {k.replace("~", "_"): v for k, v in court_codes.items()}
        except FileNotFoundError:
            print("Warning: court-codes.json not found. Using discovered courts only.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing court-codes.json: {e}")
            return {}

    def _load_bench_codes(self) -> Dict[str, str]:
        """Load bench codes from bench-codes.json"""
        try:
            with open("bench-codes.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print("Warning: bench-codes.json not found. Using discovered benches only.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing bench-codes.json: {e}")
            return {}

    def _create_bench_to_court_mapping(self) -> Dict[str, str]:
        """Create mapping from bench names to court codes"""
        bench_to_court = {}
        for bench_name, court_code in self.bench_codes.items():
            bench_to_court[bench_name] = court_code
        return bench_to_court

    def _format_file_size(self, size_bytes: int) -> str:
        """Convert bytes to human readable format"""
        if size_bytes == 0:
            return "0 B"

        size_units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_index = 0

        while size >= 1024.0 and unit_index < len(size_units) - 1:
            size /= 1024.0
            unit_index += 1

        if unit_index == 0:
            return f"{int(size)} {size_units[unit_index]}"
        else:
            return f"{size:.2f} {size_units[unit_index]}"

    def _get_object_size(self, key: str) -> int:
        """Get size of S3 object"""
        try:
            response = self.s3_unsigned.head_object(Bucket=self.bucket_name, Key=key)
            return response["ContentLength"]
        except Exception as e:
            print(f"Warning: Could not get size for {key}: {e}")
            return 0

    def _list_s3_objects(self, prefix: str) -> List[Dict]:
        """List all objects with given prefix using pagination"""
        objects = []
        paginator = self.s3_unsigned.get_paginator("list_objects_v2")

        try:
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in tqdm(page_iterator, desc=f"Scanning S3 pages", unit="page"):
                if "Contents" in page:
                    objects.extend(page["Contents"])

        except Exception as e:
            print(f"Error listing objects with prefix {prefix}: {e}")

        return objects

    def _extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Extract date from filename pattern like SKHC010001662023_1_2025-04-07.json/pdf"""
        # Look for date pattern YYYY-MM-DD in filename
        date_pattern = r"(\d{4}-\d{2}-\d{2})"
        match = re.search(date_pattern, filename)
        if match:
            try:
                date_str = match.group(1)
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                current_date = datetime.now()

                # Skip unreasonable future dates (beyond current year)
                if date_obj > current_date:
                    print(f"Skipping future date {date_str} from filename {filename}")
                    return None

                # Date should be reasonable (not too far in the past)
                if date_obj.year < 2020:
                    print(f"Skipping old date {date_str} from filename {filename}")
                    return None

                return date_obj
            except ValueError as e:
                print(f"Invalid date format {date_str} in filename {filename}: {e}")
                return None
        return None

    def _get_max_valid_date(self, filenames: List[str]) -> Optional[datetime]:
        """Get the max valid date from a list of filenames"""
        dates = []
        for fname in filenames:
            dt = self._extract_date_from_filename(fname)
            if dt:
                dates.append(dt)

        if not dates:
            return None

        max_date = max(dates)

        # Ensure the max date is not in the future
        current_date = datetime.now()
        if max_date > current_date:
            return None

        return max_date

    def _create_data_index(
        self, court_code: str, bench_name: str, updated_at: datetime
    ) -> IndexFile:
        """Create data.index.json for a court/bench combination"""
        pdf_prefix = f"data/pdf/year={self.year}/court={court_code}/bench={bench_name}/"
        pdf_objects = self._list_s3_objects(pdf_prefix)
        pdf_files = [
            os.path.basename(obj["Key"])
            for obj in pdf_objects
            if obj["Key"].endswith(".pdf")
        ]

        # Find max date from pdf filenames
        max_date = self._get_max_valid_date(pdf_files)

        # Set updated_at based on requirements:
        # 1. If no valid date or all dates before 2025-01-01, set to 2025-01-01
        # 2. If max date is greater than today, skip it (already handled in _get_max_valid_date)
        # 3. Otherwise use the max date
        min_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        if max_date and max_date >= datetime(2025, 1, 1):
            updated_at = max_date.replace(tzinfo=timezone.utc)
        else:
            updated_at = min_date

        tar_key = (
            f"data/tar/year={self.year}/court={court_code}/bench={bench_name}/pdfs.tar"
        )
        tar_size = self._get_object_size(tar_key)

        return IndexFile(
            files=sorted(pdf_files),
            file_count=len(pdf_files),
            tar_size=tar_size,
            tar_size_human=self._format_file_size(tar_size),
            updated_at=updated_at.isoformat(),
        )

    def _create_metadata_index(
        self, court_code: str, bench_name: str, updated_at: datetime
    ) -> IndexFile:
        """Create metadata.index.json for a court/bench combination"""
        json_prefix = (
            f"metadata/json/year={self.year}/court={court_code}/bench={bench_name}/"
        )
        json_objects = self._list_s3_objects(json_prefix)
        json_files = [
            os.path.basename(obj["Key"])
            for obj in json_objects
            if obj["Key"].endswith(".json")
        ]

        # Find max date from json filenames
        max_date = self._get_max_valid_date(json_files)

        # Set updated_at based on same logic as data index
        min_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        if max_date and max_date >= datetime(2025, 1, 1):
            updated_at = max_date.replace(tzinfo=timezone.utc)
        else:
            updated_at = min_date

        tar_key = f"metadata/tar/year={self.year}/court={court_code}/bench={bench_name}/metadata.tar.gz"
        tar_size = self._get_object_size(tar_key)

        return IndexFile(
            files=sorted(json_files),
            file_count=len(json_files),
            tar_size=tar_size,
            tar_size_human=self._format_file_size(tar_size),
            updated_at=updated_at.isoformat(),
        )

    def _scan_existing_structure(self) -> Dict[str, Set[str]]:
        """Scan S3 bucket to find existing court/bench structure"""
        print("Scanning existing S3 structure...")
        structure = defaultdict(set)

        # Scan both data and metadata prefixes
        prefixes = [f"data/pdf/year={self.year}/", f"metadata/json/year={self.year}/"]

        for prefix in prefixes:
            try:
                # Use list_objects_v2 with delimiter to get "directories"
                paginator = self.s3_unsigned.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
                )

                for page in page_iterator:
                    # Process common prefixes (directories)
                    if "CommonPrefixes" in page:
                        for common_prefix in page["CommonPrefixes"]:
                            prefix_path = common_prefix["Prefix"]
                            # Extract court code from path like "data/pdf/year=2025/court=SKHC/"
                            court_match = re.search(r"court=([^/]+)/", prefix_path)
                            if court_match:
                                court_code = court_match.group(1)

                                # Now scan for benches under this court
                                court_prefix = prefix_path
                                bench_paginator = self.s3_unsigned.get_paginator(
                                    "list_objects_v2"
                                )
                                bench_page_iterator = bench_paginator.paginate(
                                    Bucket=self.bucket_name,
                                    Prefix=court_prefix,
                                    Delimiter="/",
                                )

                                for bench_page in bench_page_iterator:
                                    if "CommonPrefixes" in bench_page:
                                        for bench_prefix in bench_page[
                                            "CommonPrefixes"
                                        ]:
                                            bench_path = bench_prefix["Prefix"]
                                            # Extract bench name from path like "data/pdf/year=2025/court=SKHC/bench=Principal/"
                                            bench_match = re.search(
                                                r"bench=([^/]+)/", bench_path
                                            )
                                            if bench_match:
                                                bench_name = bench_match.group(1)
                                                structure[court_code].add(bench_name)

            except Exception as e:
                print(f"Warning: Error scanning prefix {prefix}: {e}")
                continue

        # Convert sets to regular sets for easier handling
        structure_dict = {court: set(benches) for court, benches in structure.items()}

        print(f"Found {len(structure_dict)} courts with existing data")
        return structure_dict

    def _find_latest_dates_for_courts(
        self, existing_structure: Dict[str, Set[str]]
    ) -> Dict[str, datetime]:
        """Find latest dates for courts from existing filenames"""
        print("Finding latest dates for courts...")
        court_dates = {}

        for court_code in existing_structure:
            all_dates = []

            # Check both PDF and JSON files for dates
            prefixes = [
                f"data/pdf/year={self.year}/court={court_code}/",
                f"metadata/json/year={self.year}/court={court_code}/",
            ]

            for prefix in prefixes:
                try:
                    objects = self._list_s3_objects(prefix)
                    for obj in objects:
                        filename = os.path.basename(obj["Key"])
                        date_obj = self._extract_date_from_filename(filename)
                        if date_obj:
                            all_dates.append(date_obj)
                except Exception as e:
                    print(f"Warning: Error scanning {prefix}: {e}")
                    continue

            if all_dates:
                # Get max date but ensure it's not in the unreasonable future
                max_date = max(all_dates)
                current_date = datetime.now()
                if max_date.year <= current_date.year:
                    court_dates[court_code] = max_date
                else:
                    # If max date is in the future, use default
                    court_dates[court_code] = datetime(2025, 1, 1)
            else:
                # No valid dates found, use default
                court_dates[court_code] = datetime(2025, 1, 1)

        return court_dates

    def _get_all_court_bench_combinations(
        self, existing_structure: Dict[str, Set[str]]
    ) -> List[Tuple[str, str]]:
        """Get all possible court/bench combinations from both existing data and config files"""
        combinations = set()

        # Add combinations from existing S3 structure
        for court_code, benches in existing_structure.items():
            for bench_name in benches:
                combinations.add((court_code, bench_name))

        # Add combinations from config files (court-codes.json and bench-codes.json)
        # This ensures we create index files even for courts/benches that don't have data yet
        for bench_name, court_code in self.bench_codes.items():
            # Convert court code format to match S3 structure
            court_code_s3 = court_code.replace("~", "_")
            combinations.add((court_code_s3, bench_name))

        # Also add all courts from court-codes.json with their default benches
        for court_code_config in self.court_codes.keys():
            # Find benches that belong to this court
            court_benches = [
                bench
                for bench, court in self.bench_codes.items()
                if court.replace("~", "_") == court_code_config
            ]

            if court_benches:
                for bench in court_benches:
                    combinations.add((court_code_config, bench))
            else:
                # If no specific benches found, use a default bench
                combinations.add((court_code_config, "Principal"))

        return sorted(list(combinations))

    def _create_empty_index(self, updated_at: datetime) -> IndexFile:
        """Create empty index structure for courts without data"""
        return IndexFile(
            files=[],
            file_count=0,
            tar_size=0,
            tar_size_human="0 B",
            updated_at=updated_at.isoformat(),
        )

    def _upload_index_file(self, content: IndexFile, key: str) -> bool:
        """Upload index file to S3"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content.model_dump_json(indent=2),
                ContentType="application/json",
            )
            return True
        except Exception as e:
            print(f"Error uploading {key}: {e}")
            return False

    def generate_all_indexes(self) -> bool:
        """Generate all index files"""
        print("Starting index generation...")

        # Scan existing structure
        existing_structure = self._scan_existing_structure()

        # Find latest dates for existing courts
        court_latest_dates = self._find_latest_dates_for_courts(existing_structure)

        # Get all possible court/bench combinations
        all_combinations = self._get_all_court_bench_combinations(existing_structure)

        print(f"Processing {len(all_combinations)} court/bench combinations...")

        success_count = 0
        error_count = 0

        # Process each combination
        with tqdm(
            total=len(all_combinations), desc="Generating indexes", unit="combination"
        ) as pbar:
            for court_code, bench_name in sorted(all_combinations):
                try:
                    # Determine updated_at date (will be recalculated in index creation)
                    updated_at = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    has_existing_data = (
                        court_code in existing_structure
                        and bench_name in existing_structure[court_code]
                    )
                    # Create data index
                    if has_existing_data:
                        data_index = self._create_data_index(
                            court_code, bench_name, updated_at
                        )
                    else:
                        data_index = self._create_empty_index(updated_at)
                    data_key = f"data/tar/year={self.year}/court={court_code}/bench={bench_name}/data.index.json"
                    data_success = self._upload_index_file(data_index, data_key)
                    # Create metadata index
                    if has_existing_data:
                        metadata_index = self._create_metadata_index(
                            court_code, bench_name, updated_at
                        )
                    else:
                        metadata_index = self._create_empty_index(updated_at)
                    metadata_key = f"metadata/tar/year={self.year}/court={court_code}/bench={bench_name}/metadata.index.json"
                    metadata_success = self._upload_index_file(
                        metadata_index, metadata_key
                    )
                    if data_success and metadata_success:
                        success_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    print(f"Error processing {court_code}/{bench_name}: {e}")
                    error_count += 1

                pbar.update(1)

        print(f"Index generation complete!")
        print(f"Successfully processed: {success_count} combinations")
        if error_count > 0:
            print(f"Errors: {error_count} combinations")

        return error_count == 0


def main():
    """Main function to run the index initialization"""
    # Initialize the index generator with hardcoded values
    try:
        initializer = S3IndexInitializer(
            bucket_name="indian-high-court-judgments-test", year="2025"
        )

        # Generate all indexes
        success = initializer.generate_all_indexes()

        if success:
            print("All index files generated successfully!")
        else:
            print("Some errors occurred during index generation")

    except KeyboardInterrupt:
        print("Operation cancelled by user")
    except Exception as e:
        print(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
