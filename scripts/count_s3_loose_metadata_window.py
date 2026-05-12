#!/usr/bin/env python3
"""Count loose raw metadata JSON objects not present in metadata tar index."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path, PurePosixPath

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.html_utils import parse_decision_date_from_html  # noqa: E402


BUCKET = "indian-high-court-judgments"


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def bench_from_key(key: str) -> str:
    for part in PurePosixPath(key).parts:
        if part.startswith("bench="):
            return part.split("=", 1)[1]
    return ""


def index_names_by_bench(s3, prefix: str, index_suffix: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if not key.endswith(index_suffix):
            continue
        bench = bench_from_key(key)
        data = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        for part in data.get("parts", []):
            for filename in part.get("files", []):
                result.setdefault(bench, set()).add(PurePosixPath(filename).name)
    return result


def object_names_by_bench(s3, prefix: str, suffix: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for obj in iter_objects(s3, prefix):
        key = obj["Key"]
        if key.endswith(suffix):
            result.setdefault(bench_from_key(key), set()).add(PurePosixPath(key).name)
    return result


def parse_raw_record(s3, key: str):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    metadata = json.loads(body)
    date_str, _year = parse_decision_date_from_html(metadata.get("raw_html", ""))
    if not date_str:
        return None
    try:
        decision_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        return None
    pdf_link = metadata.get("pdf_link") or ""
    pdf_basename = PurePosixPath(pdf_link).name
    if not pdf_basename.endswith(".pdf"):
        pdf_basename = PurePosixPath(key).with_suffix(".pdf").name
    return {
        "key": key,
        "bench": bench_from_key(key),
        "decision_date": decision_date,
        "downloaded": metadata.get("downloaded"),
        "pdf_basename": pdf_basename,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-workers", type=int, default=96)
    parser.add_argument("--sample", type=int, default=5)
    args = parser.parse_args()

    court = args.court.replace("~", "_")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    s3 = boto3.client(
        "s3", config=Config(max_pool_connections=max(args.max_workers, 10))
    )

    raw_prefix = f"metadata/json/year={args.year}/court={court}/"
    index_prefix = f"metadata/tar/year={args.year}/court={court}/"
    indexed = index_names_by_bench(s3, index_prefix, "/metadata.index.json")

    raw_by_bench: dict[str, dict[str, str]] = {}
    for obj in iter_objects(s3, raw_prefix):
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        raw_by_bench.setdefault(bench_from_key(key), {})[PurePosixPath(key).name] = key

    loose_keys = []
    for bench, names_to_keys in raw_by_bench.items():
        indexed_names = indexed.get(bench, set())
        for name, key in names_to_keys.items():
            if name not in indexed_names:
                loose_keys.append(key)

    data_pdf = object_names_by_bench(
        s3, f"data/pdf/year={args.year}/court={court}/", ".pdf"
    )
    data_index = index_names_by_bench(
        s3, f"data/tar/year={args.year}/court={court}/", "/data.index.json"
    )

    counts: Counter[date] = Counter()
    downloaded_counts: Counter[str] = Counter()
    parsed = 0
    missing_or_unparseable = 0
    samples = []
    window_missing_pdf_object = 0
    window_missing_data_index = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(parse_raw_record, s3, key): key for key in loose_keys}
        for future in as_completed(futures):
            key = futures[future]
            record = future.result()
            if record is None:
                missing_or_unparseable += 1
                continue
            parsed += 1
            decision_date = record["decision_date"]
            if start <= decision_date <= end:
                counts[decision_date] += 1
                downloaded_counts[str(record["downloaded"])] += 1
                bench = record["bench"]
                pdf_basename = record["pdf_basename"]
                if pdf_basename not in data_pdf.get(bench, set()):
                    window_missing_pdf_object += 1
                if pdf_basename not in data_index.get(bench, set()):
                    window_missing_data_index += 1
                if len(samples) < args.sample:
                    samples.append(
                        {
                            "key": key,
                            "decision_date": decision_date.isoformat(),
                            "downloaded": record["downloaded"],
                            "pdf_basename": pdf_basename,
                            "pdf_object_present": pdf_basename
                            in data_pdf.get(bench, set()),
                            "data_index_present": pdf_basename
                            in data_index.get(bench, set()),
                        }
                    )

    print(f"court={court}")
    print(f"year={args.year}")
    print(f"start={start}")
    print(f"end={end}")
    print(f"raw_json_objects={sum(len(v) for v in raw_by_bench.values())}")
    print(f"metadata_index_distinct={sum(len(v) for v in indexed.values())}")
    print(f"loose_raw_json_not_in_metadata_index={len(loose_keys)}")
    print(f"loose_raw_dates_parsed={parsed}")
    print(f"loose_raw_missing_or_unparseable_date={missing_or_unparseable}")
    print(f"window_loose_raw_json={sum(counts.values())}")
    print(f"window_loose_downloaded_counts={dict(sorted(downloaded_counts.items()))}")
    print(f"window_loose_missing_pdf_object={window_missing_pdf_object}")
    print(f"window_loose_missing_data_index={window_missing_data_index}")
    for decision_date, count in sorted(counts.items()):
        print(f"{decision_date}\t{count}")
    print("sample_records=" + json.dumps(samples, sort_keys=True))


if __name__ == "__main__":
    main()
