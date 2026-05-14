#!/usr/bin/env python3
"""Audit S3 raw object, tar index, and parquet counts for court/year partitions."""

from __future__ import annotations

import argparse
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.court_utils import to_s3_format  # noqa: E402


BUCKET = "indian-high-court-judgments"


@dataclass
class ObjectStats:
    count: int = 0
    bytes: int = 0


@dataclass
class IndexStats:
    index_files: int = 0
    top_file_count: int = 0
    part_file_entries: int = 0
    distinct_filenames: int = 0
    duplicate_entries: int = 0
    index_bytes: int = 0


@dataclass
class ParquetStats:
    files: int = 0
    rows: int = 0
    bytes: int = 0


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def object_stats(s3, prefix: str, suffix: str) -> ObjectStats:
    stats = ObjectStats()
    for obj in iter_objects(s3, prefix):
        if obj["Key"].endswith(suffix):
            stats.count += 1
            stats.bytes += obj["Size"]
    return stats


def index_stats(s3, prefix: str, suffix: str) -> IndexStats:
    stats = IndexStats()
    filenames = []
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith(suffix):
            continue
        stats.index_files += 1
        stats.index_bytes += obj["Size"]
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        data = json.loads(body)
        stats.top_file_count += int(data.get("file_count", 0))
        for part in data.get("parts", []):
            files = part.get("files", [])
            stats.part_file_entries += len(files)
            filenames.extend(files)
    stats.distinct_filenames = len(set(filenames))
    stats.duplicate_entries = stats.part_file_entries - stats.distinct_filenames
    return stats


def parquet_stats(s3, prefix: str) -> ParquetStats:
    stats = ParquetStats()
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith("/metadata.parquet"):
            continue
        stats.files += 1
        stats.bytes += obj["Size"]
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        try:
            df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link"])
        except Exception:
            df = pd.read_parquet(io.BytesIO(body))
        stats.rows += len(df)
    return stats


def audit_partition(s3, court: str, year: int) -> dict:
    court = to_s3_format(court)
    return {
        "court": court,
        "year": year,
        "metadata_json_objects": object_stats(
            s3, f"metadata/json/year={year}/court={court}/", ".json"
        ),
        "data_pdf_objects": object_stats(
            s3, f"data/pdf/year={year}/court={court}/", ".pdf"
        ),
        "metadata_tar_index": index_stats(
            s3, f"metadata/tar/year={year}/court={court}/", "/metadata.index.json"
        ),
        "data_tar_index": index_stats(
            s3, f"data/tar/year={year}/court={court}/", "/data.index.json"
        ),
        "metadata_parquet": parquet_stats(
            s3, f"metadata/parquet/year={year}/court={court}/"
        ),
    }


def print_row(result: dict) -> None:
    mj = result["metadata_json_objects"]
    dp = result["data_pdf_objects"]
    mi = result["metadata_tar_index"]
    di = result["data_tar_index"]
    pq = result["metadata_parquet"]
    print(
        "\t".join(
            str(v)
            for v in [
                result["court"],
                result["year"],
                mj.count,
                mj.bytes,
                dp.count,
                dp.bytes,
                mi.top_file_count,
                mi.part_file_entries,
                mi.distinct_filenames,
                mi.duplicate_entries,
                mi.index_files,
                di.top_file_count,
                di.part_file_entries,
                di.distinct_filenames,
                di.duplicate_entries,
                di.index_files,
                pq.rows,
                pq.files,
                pq.bytes,
            ]
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--courts", required=True, help="Comma-separated court codes")
    parser.add_argument("--years", required=True, help="Comma-separated years")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    print(
        "\t".join(
            [
                "court",
                "year",
                "metadata_json_objects",
                "metadata_json_bytes",
                "data_pdf_objects",
                "data_pdf_bytes",
                "metadata_index_file_count",
                "metadata_index_part_entries",
                "metadata_index_distinct",
                "metadata_index_duplicate_entries",
                "metadata_index_files",
                "data_index_file_count",
                "data_index_part_entries",
                "data_index_distinct",
                "data_index_duplicate_entries",
                "data_index_files",
                "parquet_rows",
                "parquet_files",
                "parquet_bytes",
            ]
        )
    )
    for court in args.courts.split(","):
        for year_str in args.years.split(","):
            print_row(audit_partition(s3, court.strip(), int(year_str.strip())))


if __name__ == "__main__":
    main()
