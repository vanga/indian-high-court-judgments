#!/usr/bin/env python3
"""Count S3 parquet rows whose parsed decision_date falls in a date window."""

from __future__ import annotations

import argparse
import io
from collections import Counter
from datetime import date

import boto3
import pandas as pd


BUCKET = "indian-high-court-judgments"


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def parquet_window_counts(s3, court: str, start: date, end: date):
    counts: Counter[date] = Counter()
    files = 0
    rows_total = 0
    rows_with_dates = 0
    years = range(start.year, end.year + 1)
    for year in years:
        prefix = f"metadata/parquet/year={year}/court={court}/"
        for obj in iter_objects(s3, prefix):
            if not obj["Key"].endswith("/metadata.parquet"):
                continue
            files += 1
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            df = pd.read_parquet(io.BytesIO(body), columns=["decision_date"])
            rows_total += len(df)
            dates = pd.to_datetime(df["decision_date"], errors="coerce").dt.date
            rows_with_dates += int(dates.notna().sum())
            for decision_date in dates[(dates >= start) & (dates <= end)].dropna():
                counts[decision_date] += 1
    return files, rows_total, rows_with_dates, counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    court = args.court.replace("~", "_")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    s3 = boto3.client("s3")
    files, rows_total, rows_with_dates, counts = parquet_window_counts(
        s3, court, start, end
    )
    print(f"court={court}")
    print(f"start={start}")
    print(f"end={end}")
    print(f"parquet_files={files}")
    print(f"partition_rows_total={rows_total}")
    print(f"partition_rows_with_decision_date={rows_with_dates}")
    print(f"window_rows={sum(counts.values())}")
    for decision_date, count in sorted(counts.items()):
        print(f"{decision_date}\t{count}")


if __name__ == "__main__":
    main()
