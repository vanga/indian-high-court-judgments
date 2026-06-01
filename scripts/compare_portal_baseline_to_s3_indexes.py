#!/usr/bin/env python3
"""Compare saved portal count baseline to S3 tar index counts."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def parse_int(value: str | None) -> int:
    value = (value or "").strip()
    return int(value) if value else 0


def open_reader(path: Path):
    handle = path.open(newline="")
    sample = handle.read(4096)
    handle.seek(0)
    dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
    return handle, csv.DictReader(handle, dialect=dialect)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal-baseline", required=True, type=Path)
    parser.add_argument("--s3-index-audit", required=True, type=Path)
    args = parser.parse_args()

    s3_rows: dict[tuple[str, str], dict[str, int]] = {}
    s3_handle, s3_reader = open_reader(args.s3_index_audit)
    with s3_handle:
        for row in s3_reader:
            s3_rows[(row["court"], row["year"])] = {
                "metadata_index": parse_int(row.get("metadata_index_file_count")),
                "metadata_index_distinct": parse_int(row.get("metadata_index_distinct")),
                "metadata_index_duplicate_entries": parse_int(
                    row.get("metadata_index_duplicate_entries")
                ),
                "metadata_index_files": parse_int(row.get("metadata_index_files")),
                "data_index": parse_int(row.get("data_index_file_count")),
                "data_index_distinct": parse_int(row.get("data_index_distinct")),
                "data_index_duplicate_entries": parse_int(
                    row.get("data_index_duplicate_entries")
                ),
                "data_index_files": parse_int(row.get("data_index_files")),
            }

    fieldnames = [
        "court",
        "court_name",
        "year",
        "portal_total",
        "metadata_index",
        "metadata_index_distinct",
        "metadata_index_duplicate_entries",
        "metadata_index_files",
        "data_index",
        "data_index_distinct",
        "data_index_duplicate_entries",
        "data_index_files",
        "portal_minus_metadata_index",
        "portal_minus_data_index",
        "metadata_index_minus_data_index",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    baseline_handle, baseline_reader = open_reader(args.portal_baseline)
    with baseline_handle:
        for baseline in baseline_reader:
            portal_total = parse_int(baseline.get("portal_total"))
            s3 = s3_rows.get(
                (baseline["court"], baseline["year"]),
                {
                    "metadata_index": 0,
                    "metadata_index_distinct": 0,
                    "metadata_index_duplicate_entries": 0,
                    "metadata_index_files": 0,
                    "data_index": 0,
                    "data_index_distinct": 0,
                    "data_index_duplicate_entries": 0,
                    "data_index_files": 0,
                },
            )
            writer.writerow(
                {
                    "court": baseline["court"],
                    "court_name": baseline.get("court_name", ""),
                    "year": baseline["year"],
                    "portal_total": portal_total,
                    **s3,
                    "portal_minus_metadata_index": portal_total - s3["metadata_index"],
                    "portal_minus_data_index": portal_total - s3["data_index"],
                    "metadata_index_minus_data_index": s3["metadata_index"]
                    - s3["data_index"],
                }
            )


if __name__ == "__main__":
    main()
