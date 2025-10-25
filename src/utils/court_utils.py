"""Court and bench utility functions for managing mappings and tracking data."""

import csv
import json
import threading
from pathlib import Path


BENCH_CODES_FILE = "bench-codes.json"
lock = threading.Lock()


def to_s3_format(court_code):
    """Convert court code from tilde to underscore format (for S3 paths)"""
    return court_code.replace("~", "_")


def from_s3_format(court_code):
    """Convert court code from underscore to tilde format (from S3 paths)"""
    return court_code.replace("_", "~")


def get_json_file(file_path) -> dict:
    with open(file_path) as f:
        return json.load(f)


def get_court_codes():
    court_codes = get_json_file("./court-codes.json")
    return court_codes


def get_bench_codes():
    """Load bench to court mappings from bench-codes.json"""
    with open(BENCH_CODES_FILE, "r") as f:
        return json.load(f)


def load_court_bench_mapping():
    """
    Load court-bench mapping from opendata/docs/high_courts.csv
    Returns: dict[bench_name] = court_code (with underscore format)
    """
    csv_path = Path("opendata/docs/high_courts.csv")
    mapping = {}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bench_name = row["bench_name"]
            court_code = row["court_code"].replace("~", "_")
            mapping[bench_name] = court_code

    print(f"Loaded {len(mapping)} bench-to-court mappings from {csv_path}")
    return mapping
