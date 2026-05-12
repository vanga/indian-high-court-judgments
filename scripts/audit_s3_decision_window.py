#!/usr/bin/env python3
"""Audit S3 counts for a court/date window using parsed HTML decision_date."""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils.html_utils import parse_decision_date_from_html


BUCKET = "indian-high-court-judgments"


@dataclass(frozen=True)
class RawRecord:
    key: str
    basename: str
    bench: str
    pdf_basename: str
    pdf_link: str
    decision_date: date
    downloaded: bool | None


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def bench_from_key(key: str) -> str:
    parts = PurePosixPath(key).parts
    for part in parts:
        if part.startswith("bench="):
            return part.split("=", 1)[1]
    return ""


def object_basenames_by_bench(s3, prefix: str, suffix: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith(suffix):
            continue
        result.setdefault(bench_from_key(key), set()).add(PurePosixPath(key).name)
    return result


def index_basenames_by_bench(s3, prefix: str, suffix: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith(suffix):
            continue
        bench = bench_from_key(key)
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        data = json.loads(body)
        for part in data.get("parts", []):
            for filename in part.get("files", []):
                result.setdefault(bench, set()).add(PurePosixPath(filename).name)
    return result


def parse_date(value) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def parquet_records_by_bench(
    s3, prefix: str, start: date, end: date
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith("/metadata.parquet"):
            continue
        bench = bench_from_key(key)
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link", "decision_date"])
        dates = pd.to_datetime(df["decision_date"], errors="coerce").dt.date
        filtered = df.loc[(dates >= start) & (dates <= end), "pdf_link"]
        for value in filtered.dropna().astype(str):
            basename = PurePosixPath(value).name
            if basename.endswith(".pdf"):
                result.setdefault(bench, set()).add(basename)
    return result


def parse_raw_record(s3, obj_key: str) -> RawRecord | None:
    body = s3.get_object(Bucket=BUCKET, Key=obj_key)["Body"].read()
    metadata = json.loads(body)
    date_str, _year = parse_decision_date_from_html(metadata.get("raw_html", ""))
    if not date_str:
        return None
    try:
        parsed_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        return None
    pdf_link = metadata.get("pdf_link", "")
    pdf_basename = PurePosixPath(pdf_link).name
    if not pdf_basename.endswith(".pdf"):
        pdf_basename = os.path.splitext(PurePosixPath(obj_key).name)[0] + ".pdf"
    return RawRecord(
        key=obj_key,
        basename=PurePosixPath(obj_key).name,
        bench=bench_from_key(obj_key),
        pdf_basename=pdf_basename,
        pdf_link=pdf_link,
        decision_date=parsed_date,
        downloaded=metadata.get("downloaded"),
    )


def raw_records_for_window(
    s3, court: str, years: range, start: date, end: date, max_workers: int
) -> list[RawRecord]:
    keys = []
    for year in years:
        prefix = f"metadata/json/year={year}/court={court}/"
        keys.extend(
            obj["Key"]
            for obj in iter_objects(s3, prefix)
            if obj["Key"].endswith(".json")
        )

    records: list[RawRecord] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(parse_raw_record, s3, key) for key in keys]
        for future in as_completed(futures):
            record = future.result()
            if record and start <= record.decision_date <= end:
                records.append(record)
    return records


def count_missing(records: list[RawRecord], by_bench: dict[str, set[str]], attr: str) -> int:
    missing = 0
    for record in records:
        value = getattr(record, attr)
        if value not in by_bench.get(record.bench, set()):
            missing += 1
    return missing


def sample_missing(
    records: list[RawRecord], by_bench: dict[str, set[str]], attr: str, limit: int
) -> str:
    values = []
    for record in sorted(records, key=lambda r: (r.decision_date, r.basename)):
        value = getattr(record, attr)
        if value not in by_bench.get(record.bench, set()):
            values.append(value)
        if len(values) >= limit:
            break
    return ",".join(values)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-workers", type=int, default=32)
    parser.add_argument("--sample", type=int, default=10)
    args = parser.parse_args()

    court = args.court.replace("~", "_")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    years = range(start.year, end.year + 1)
    s3 = boto3.client("s3")

    records = raw_records_for_window(s3, court, years, start, end, args.max_workers)
    raw_json_by_bench: dict[str, set[str]] = {}
    raw_pdf_names_by_bench: dict[str, set[str]] = {}
    for record in records:
        raw_json_by_bench.setdefault(record.bench, set()).add(record.basename)
        raw_pdf_names_by_bench.setdefault(record.bench, set()).add(record.pdf_basename)

    data_pdf_by_bench: dict[str, set[str]] = {}
    metadata_index_by_bench: dict[str, set[str]] = {}
    data_index_by_bench: dict[str, set[str]] = {}
    parquet_by_bench: dict[str, set[str]] = {}
    for year in years:
        merge_sets(
            data_pdf_by_bench,
            object_basenames_by_bench(s3, f"data/pdf/year={year}/court={court}/", ".pdf"),
        )
        merge_sets(
            metadata_index_by_bench,
            index_basenames_by_bench(
                s3, f"metadata/tar/year={year}/court={court}/", "/metadata.index.json"
            ),
        )
        merge_sets(
            data_index_by_bench,
            index_basenames_by_bench(
                s3, f"data/tar/year={year}/court={court}/", "/data.index.json"
            ),
        )
        merge_sets(
            parquet_by_bench,
            parquet_records_by_bench(
                s3, f"metadata/parquet/year={year}/court={court}/", start, end
            ),
        )

    print(f"court={court} start={start} end={end}")
    print(f"raw_json_decision_window\t{len(records)}")
    print(f"raw_json_downloaded_true\t{sum(1 for r in records if r.downloaded is True)}")
    print(f"raw_json_downloaded_false\t{sum(1 for r in records if r.downloaded is False)}")
    print(f"raw_json_downloaded_missing\t{sum(1 for r in records if r.downloaded is None)}")
    print(f"raw_json_missing_metadata_index\t{count_missing(records, metadata_index_by_bench, 'basename')}")
    print(f"raw_json_missing_pdf_object\t{count_missing(records, data_pdf_by_bench, 'pdf_basename')}")
    print(f"raw_json_missing_data_index\t{count_missing(records, data_index_by_bench, 'pdf_basename')}")
    print(f"raw_json_missing_parquet\t{count_missing(records, parquet_by_bench, 'pdf_basename')}")
    print(f"parquet_decision_window\t{sum(len(v) for v in parquet_by_bench.values())}")
    print(f"sample_missing_pdf\t{sample_missing(records, data_pdf_by_bench, 'pdf_basename', args.sample)}")
    print(f"sample_missing_metadata_index\t{sample_missing(records, metadata_index_by_bench, 'basename', args.sample)}")
    print(f"sample_missing_parquet\t{sample_missing(records, parquet_by_bench, 'pdf_basename', args.sample)}")


def merge_sets(target: dict[str, set[str]], source: dict[str, set[str]]) -> None:
    for key, values in source.items():
        target.setdefault(key, set()).update(values)


if __name__ == "__main__":
    main()
