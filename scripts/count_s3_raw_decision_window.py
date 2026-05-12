#!/usr/bin/env python3
"""Count raw S3 metadata JSON objects by parsed HTML Decision Date."""

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


def parse_raw_date(s3, key: str):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    metadata = json.loads(body)
    date_str, _year = parse_decision_date_from_html(metadata.get("raw_html", ""))
    if not date_str:
        return None
    try:
        decision_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        return None
    return {
        "key": key,
        "basename": PurePosixPath(key).name,
        "decision_date": decision_date,
        "downloaded": metadata.get("downloaded"),
        "pdf_link": metadata.get("pdf_link"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-workers", type=int, default=96)
    parser.add_argument("--sample", type=int, default=5)
    args = parser.parse_args()

    court = args.court.replace("~", "_")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    years = range(start.year, end.year + 1)
    s3 = boto3.client(
        "s3", config=Config(max_pool_connections=max(args.max_workers, 10))
    )

    keys = []
    for year in years:
        prefix = f"metadata/json/year={year}/court={court}/"
        keys.extend(
            obj["Key"]
            for obj in iter_objects(s3, prefix)
            if obj["Key"].endswith(".json")
        )

    counts: Counter[date] = Counter()
    downloaded_counts: Counter[str] = Counter()
    samples = []
    parsed = 0
    missing_or_unparseable = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(parse_raw_date, s3, key) for key in keys]
        for future in as_completed(futures):
            record = future.result()
            if not record:
                missing_or_unparseable += 1
                continue
            parsed += 1
            decision_date = record["decision_date"]
            if start <= decision_date <= end:
                counts[decision_date] += 1
                downloaded_counts[str(record["downloaded"])] += 1
                if len(samples) < args.sample:
                    sample = dict(record)
                    sample["decision_date"] = decision_date.isoformat()
                    samples.append(sample)

    print(f"court={court}")
    print(f"start={start}")
    print(f"end={end}")
    print(f"partition_json_keys_scanned={len(keys)}")
    print(f"partition_json_dates_parsed={parsed}")
    print(f"partition_json_missing_or_unparseable_date={missing_or_unparseable}")
    print(f"window_raw_json={sum(counts.values())}")
    print(f"window_downloaded_counts={dict(sorted(downloaded_counts.items()))}")
    for decision_date, count in sorted(counts.items()):
        print(f"{decision_date}\t{count}")
    print("sample_records=" + json.dumps(samples, sort_keys=True))


if __name__ == "__main__":
    main()
