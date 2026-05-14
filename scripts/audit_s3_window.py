#!/usr/bin/env python3
"""Audit S3 object pairing using dates embedded in filenames.

The filename date is the eCourts portal file/path date, not the authoritative
decision date. Use this only to diagnose JSON/PDF/index pairing, not to compare
against eCourts decision-date counts.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path, PurePosixPath

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.court_utils import to_s3_format  # noqa: E402


BUCKET = "indian-high-court-judgments"
DATE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.(json|pdf)$")


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def parse_date_from_name(name: str) -> date | None:
    match = DATE_RE.search(name)
    if not match:
        return None
    return date.fromisoformat(match.group(1))


def in_window(name: str, start: date, end: date) -> bool:
    parsed = parse_date_from_name(name)
    return parsed is not None and start <= parsed <= end


def object_names(s3, prefix: str, suffix: str, start: date, end: date) -> set[str]:
    names = set()
    for obj in iter_objects(s3, prefix):
        name = PurePosixPath(obj["Key"]).name
        if name.endswith(suffix) and in_window(name, start, end):
            names.add(name)
    return names


def index_names(s3, prefix: str, suffix: str, start: date, end: date) -> tuple[set[str], int]:
    counts: Counter[str] = Counter()
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith(suffix):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        data = json.loads(body)
        for part in data.get("parts", []):
            for filename in part.get("files", []):
                name = PurePosixPath(filename).name
                if in_window(name, start, end):
                    counts[name] += 1
    duplicate_entries = sum(count - 1 for count in counts.values() if count > 1)
    return set(counts), duplicate_entries


def parquet_names(s3, prefix: str, start: date, end: date) -> set[str]:
    names = set()
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith("/metadata.parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link", "decision_date"])
        dates = pd.to_datetime(df["decision_date"], errors="coerce").dt.date
        filtered = df.loc[(dates >= start) & (dates <= end), "pdf_link"]
        for value in filtered.dropna().astype(str):
            basename = PurePosixPath(value).name
            if basename.endswith(".pdf"):
                names.add(basename)
    return names


def print_count(name: str, values: set[str]) -> None:
    print(f"{name}\t{len(values)}")


def sample(values: set[str], limit: int) -> str:
    return ",".join(sorted(values)[:limit])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample", type=int, default=10)
    args = parser.parse_args()

    court = to_s3_format(args.court)
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    years = range(start.year, end.year + 1)
    s3 = boto3.client("s3")

    raw_json: set[str] = set()
    raw_pdf: set[str] = set()
    metadata_index: set[str] = set()
    data_index: set[str] = set()
    parquet_pdf: set[str] = set()
    metadata_index_duplicates = 0
    data_index_duplicates = 0

    for year in years:
        raw_json.update(
            object_names(s3, f"metadata/json/year={year}/court={court}/", ".json", start, end)
        )
        raw_pdf.update(
            object_names(s3, f"data/pdf/year={year}/court={court}/", ".pdf", start, end)
        )
        names, duplicates = index_names(
            s3,
            f"metadata/tar/year={year}/court={court}/",
            "/metadata.index.json",
            start,
            end,
        )
        metadata_index.update(names)
        metadata_index_duplicates += duplicates
        names, duplicates = index_names(
            s3,
            f"data/tar/year={year}/court={court}/",
            "/data.index.json",
            start,
            end,
        )
        data_index.update(names)
        data_index_duplicates += duplicates
        parquet_pdf.update(
            parquet_names(s3, f"metadata/parquet/year={year}/court={court}/", start, end)
        )

    json_as_pdf = {os.path.splitext(name)[0] + ".pdf" for name in raw_json}
    metadata_index_as_pdf = {
        os.path.splitext(name)[0] + ".pdf" for name in metadata_index
    }

    print(f"court={court} start={start} end={end}")
    print_count("raw_json", raw_json)
    print_count("raw_pdf", raw_pdf)
    print_count("metadata_index", metadata_index)
    print_count("data_index", data_index)
    print_count("parquet_pdf_link", parquet_pdf)
    print_count("json_without_pdf", json_as_pdf - raw_pdf)
    print_count("json_without_data_index", json_as_pdf - data_index)
    print_count("json_without_parquet", json_as_pdf - parquet_pdf)
    print_count("pdf_without_json", raw_pdf - json_as_pdf)
    print_count("data_index_without_json", data_index - json_as_pdf)
    print_count("parquet_without_json", parquet_pdf - json_as_pdf)
    print(f"metadata_index_duplicate_entries\t{metadata_index_duplicates}")
    print(f"data_index_duplicate_entries\t{data_index_duplicates}")
    for name, values in [
        ("json_without_pdf", json_as_pdf - raw_pdf),
        ("json_without_parquet", json_as_pdf - parquet_pdf),
        ("parquet_without_json", parquet_pdf - json_as_pdf),
    ]:
        print(f"sample_{name}\t{sample(values, args.sample)}")


if __name__ == "__main__":
    main()
