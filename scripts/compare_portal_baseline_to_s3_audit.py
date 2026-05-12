#!/usr/bin/env python3
"""Compare stored portal count baseline with a fresh S3 layer audit TSV."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


S3_FIELDS = {
    "raw_json": "metadata_json_objects",
    "raw_pdf": "data_pdf_objects",
    "metadata_index": "metadata_index_file_count",
    "data_index": "data_index_file_count",
    "parquet_rows": "parquet_rows",
}


def parse_int(value: str) -> int:
    value = (value or "").strip()
    return int(value) if value else 0


def read_s3_audit(path: Path) -> dict[tuple[str, str], dict[str, int]]:
    rows: dict[tuple[str, str], dict[str, int]] = {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            key = (row["court"], row["year"])
            rows[key] = {
                output_name: parse_int(row.get(input_name, "0"))
                for output_name, input_name in S3_FIELDS.items()
            }
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal-baseline-tsv", required=True, type=Path)
    parser.add_argument("--s3-audit-tsv", required=True, type=Path)
    args = parser.parse_args()

    s3_rows = read_s3_audit(args.s3_audit_tsv)
    fieldnames = [
        "court",
        "court_name",
        "year",
        "portal_total",
        "raw_json",
        "raw_pdf",
        "metadata_index",
        "data_index",
        "parquet_rows",
        "portal_minus_raw_json",
        "portal_minus_raw_pdf",
        "portal_minus_data_index",
        "portal_minus_parquet",
        "raw_json_minus_parquet",
        "raw_pdf_minus_data_index",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()

    with args.portal_baseline_tsv.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for baseline in reader:
            key = (baseline["court"], baseline["year"])
            portal_total = parse_int(baseline["portal_total"])
            s3 = s3_rows.get(
                key,
                {
                    "raw_json": 0,
                    "raw_pdf": 0,
                    "metadata_index": 0,
                    "data_index": 0,
                    "parquet_rows": 0,
                },
            )
            writer.writerow(
                {
                    "court": baseline["court"],
                    "court_name": baseline["court_name"],
                    "year": baseline["year"],
                    "portal_total": portal_total,
                    **s3,
                    "portal_minus_raw_json": portal_total - s3["raw_json"],
                    "portal_minus_raw_pdf": portal_total - s3["raw_pdf"],
                    "portal_minus_data_index": portal_total - s3["data_index"],
                    "portal_minus_parquet": portal_total - s3["parquet_rows"],
                    "raw_json_minus_parquet": s3["raw_json"] - s3["parquet_rows"],
                    "raw_pdf_minus_data_index": s3["raw_pdf"] - s3["data_index"],
                }
            )


if __name__ == "__main__":
    main()
