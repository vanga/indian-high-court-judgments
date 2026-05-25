#!/usr/bin/env python3
"""Count S3 tar index entries by court/year.

This intentionally reads only metadata.index.json and data.index.json files.
It does not list raw PDF/JSON objects or read parquet files.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass

import boto3

from src.utils.court_utils import to_s3_format


BUCKET = "indian-high-court-judgments"


@dataclass
class IndexStats:
    index_files: int = 0
    top_file_count: int = 0
    part_entries: int = 0
    distinct_filenames: int = 0
    duplicate_entries: int = 0


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def index_stats(s3, prefix: str, suffix: str) -> IndexStats:
    stats = IndexStats()
    filenames: list[str] = []
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith(suffix):
            continue
        stats.index_files += 1
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        data = json.loads(body)
        stats.top_file_count += int(data.get("file_count", 0))
        for part in data.get("parts", []):
            files = part.get("files", [])
            stats.part_entries += len(files)
            filenames.extend(files)
    stats.distinct_filenames = len(set(filenames))
    stats.duplicate_entries = stats.part_entries - stats.distinct_filenames
    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--courts", required=True, help="Comma-separated court codes")
    parser.add_argument("--years", required=True, help="Comma-separated years")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=[
            "court",
            "year",
            "metadata_index_file_count",
            "metadata_index_distinct",
            "metadata_index_duplicate_entries",
            "metadata_index_files",
            "data_index_file_count",
            "data_index_distinct",
            "data_index_duplicate_entries",
            "data_index_files",
        ],
    )
    writer.writeheader()
    for court_arg in args.courts.split(","):
        court = to_s3_format(court_arg.strip())
        for year_arg in args.years.split(","):
            year = int(year_arg.strip())
            metadata = index_stats(
                s3,
                f"metadata/tar/year={year}/court={court}/",
                "/metadata.index.json",
            )
            data = index_stats(
                s3,
                f"data/tar/year={year}/court={court}/",
                "/data.index.json",
            )
            writer.writerow(
                {
                    "court": court,
                    "year": year,
                    "metadata_index_file_count": metadata.top_file_count,
                    "metadata_index_distinct": metadata.distinct_filenames,
                    "metadata_index_duplicate_entries": metadata.duplicate_entries,
                    "metadata_index_files": metadata.index_files,
                    "data_index_file_count": data.top_file_count,
                    "data_index_distinct": data.distinct_filenames,
                    "data_index_duplicate_entries": data.duplicate_entries,
                    "data_index_files": data.index_files,
                }
            )


if __name__ == "__main__":
    main()
