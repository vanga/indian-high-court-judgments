#!/usr/bin/env python3
"""Compare raw S3 object names with tar indexes and parquet ids for one partition."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path, PurePosixPath

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.court_utils import to_s3_format  # noqa: E402


BUCKET = "indian-high-court-judgments"


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def object_basenames(s3, prefix: str, suffix: str) -> set[str]:
    names = set()
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if key.endswith(suffix):
            names.add(PurePosixPath(key).name)
    return names


def index_basenames(s3, prefix: str, suffix: str) -> tuple[set[str], Counter[str]]:
    counts: Counter[str] = Counter()
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith(suffix):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        data = json.loads(body)
        for part in data.get("parts", []):
            for filename in part.get("files", []):
                counts[PurePosixPath(filename).name] += 1
    return set(counts), counts


def parquet_pdf_basenames(s3, prefix: str) -> set[str]:
    names = set()
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith("/metadata.parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link"])
        for value in df["pdf_link"].dropna().astype(str):
            match = re.search(r"([^/]+\.pdf)$", value)
            if match:
                names.add(match.group(1))
    return names


def sample(values: set[str], limit: int) -> str:
    return ",".join(sorted(values)[:limit])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--sample", type=int, default=10)
    args = parser.parse_args()

    court = to_s3_format(args.court)
    year = args.year
    s3 = boto3.client("s3")

    json_names = object_basenames(
        s3, f"metadata/json/year={year}/court={court}/", ".json"
    )
    pdf_names = object_basenames(s3, f"data/pdf/year={year}/court={court}/", ".pdf")
    metadata_index_names, metadata_index_counts = index_basenames(
        s3, f"metadata/tar/year={year}/court={court}/", "/metadata.index.json"
    )
    data_index_names, data_index_counts = index_basenames(
        s3, f"data/tar/year={year}/court={court}/", "/data.index.json"
    )
    parquet_pdf_names = parquet_pdf_basenames(
        s3, f"metadata/parquet/year={year}/court={court}/"
    )

    json_as_pdf = {os.path.splitext(name)[0] + ".pdf" for name in json_names}
    metadata_index_as_pdf = {
        os.path.splitext(name)[0] + ".pdf" for name in metadata_index_names
    }

    comparisons = {
        "raw_json": json_names,
        "raw_pdf": pdf_names,
        "metadata_index": metadata_index_names,
        "data_index": data_index_names,
        "parquet_pdf_link": parquet_pdf_names,
        "json_not_metadata_index": json_names - metadata_index_names,
        "metadata_index_not_json": metadata_index_names - json_names,
        "json_without_pdf": json_as_pdf - pdf_names,
        "pdf_without_json": pdf_names - json_as_pdf,
        "json_without_data_index": json_as_pdf - data_index_names,
        "data_index_without_json": data_index_names - json_as_pdf,
        "json_without_parquet": json_as_pdf - parquet_pdf_names,
        "parquet_without_json": parquet_pdf_names - json_as_pdf,
        "metadata_index_without_parquet": metadata_index_as_pdf - parquet_pdf_names,
        "parquet_without_metadata_index": parquet_pdf_names - metadata_index_as_pdf,
    }

    print(f"court={court} year={year}")
    for name, values in comparisons.items():
        print(f"{name}\t{len(values)}")

    duplicate_metadata_index = sum(
        count - 1 for count in metadata_index_counts.values() if count > 1
    )
    duplicate_data_index = sum(count - 1 for count in data_index_counts.values() if count > 1)
    print(f"metadata_index_duplicate_entries\t{duplicate_metadata_index}")
    print(f"data_index_duplicate_entries\t{duplicate_data_index}")

    for name in [
        "json_not_metadata_index",
        "json_without_pdf",
        "json_without_data_index",
        "json_without_parquet",
        "parquet_without_json",
    ]:
        print(f"sample_{name}\t{sample(comparisons[name], args.sample)}")


if __name__ == "__main__":
    main()
