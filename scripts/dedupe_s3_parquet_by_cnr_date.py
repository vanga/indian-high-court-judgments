#!/usr/bin/env python3
"""Rewrite S3 parquet files after deduping logical judgment rows.

The raw dataset may legitimately contain multiple PDF URLs for the same case
and decision date when courts re-upload a judgment. The public parquet layer is
the query layer, so it should expose one row per logical judgment:

    cnr + decision_date

When either field is missing, the shared parquet dedupe helper falls back to
pdf_link to avoid collapsing unrelated incomplete records.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.s3_utils import S3_BUCKET, _build_parquet_dedupe_key_frame


PARQUET_RE = re.compile(
    r"^metadata/parquet/year=(?P<year>\d{4})/court=(?P<court>[^/]+)/bench=(?P<bench>[^/]+)/metadata\.parquet$"
)


@dataclass(frozen=True)
class ParquetObject:
    key: str
    year: int
    court: str
    bench: str
    last_modified: object


def list_parquet_objects(s3, years: set[int] | None, courts: set[str] | None):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="metadata/parquet/"):
        for obj in page.get("Contents", []):
            match = PARQUET_RE.match(obj["Key"])
            if not match:
                continue
            year = int(match.group("year"))
            court = match.group("court")
            if years and year not in years:
                continue
            if courts and court not in courts:
                continue
            yield ParquetObject(
                key=obj["Key"],
                year=year,
                court=court,
                bench=match.group("bench"),
                last_modified=obj["LastModified"],
            )


def read_parquet(s3, obj: ParquetObject) -> pd.DataFrame:
    body = s3.get_object(Bucket=S3_BUCKET, Key=obj.key)["Body"].read()
    frame = pd.read_parquet(io.BytesIO(body))
    frame["_source_key"] = obj.key
    frame["_source_row"] = range(len(frame))
    return frame


def parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def process_group(s3, objects: list[ParquetObject], write: bool) -> dict[str, int]:
    # LastModified ordering gives the most recently rewritten parquet partition
    # precedence for cross-bench duplicates; row order gives new appended rows
    # precedence within a partition.
    ordered_objects = sorted(objects, key=lambda obj: (obj.last_modified, obj.key))
    frames = [read_parquet(s3, obj) for obj in ordered_objects]
    if not frames:
        return {"files": 0, "rows_before": 0, "rows_after": 0, "duplicates": 0, "rewritten": 0}

    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    combined["_dedupe_key"] = _build_parquet_dedupe_key_frame(combined)
    combined = combined.drop_duplicates(subset=["_dedupe_key"], keep="last")
    after = len(combined)

    rewritten = 0
    by_source = {key: frame for key, frame in combined.groupby("_source_key", sort=False)}
    for obj, original in zip(ordered_objects, frames):
        cleaned = by_source.get(obj.key)
        if cleaned is None:
            cleaned = original.iloc[0:0].copy()
        cleaned = cleaned.drop(columns=["_source_key", "_source_row", "_dedupe_key"], errors="ignore")
        original_clean = original.drop(columns=["_source_key", "_source_row"], errors="ignore")
        changed = len(cleaned) != len(original_clean)
        if not changed:
            continue
        rewritten += 1
        print(
            f"{'WRITE' if write else 'DRY'}\t{obj.key}\t"
            f"{len(original_clean)} -> {len(cleaned)} rows"
        )
        if write:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=obj.key,
                Body=parquet_bytes(cleaned),
                ContentType="application/octet-stream",
            )

    return {
        "files": len(objects),
        "rows_before": before,
        "rows_after": after,
        "duplicates": before - after,
        "rewritten": rewritten,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", action="append", type=int, help="Limit to one or more years")
    parser.add_argument("--court", action="append", help="Limit to one or more S3-format court codes")
    parser.add_argument("--write", action="store_true", help="Upload rewritten parquet files")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    groups: dict[tuple[int, str], list[ParquetObject]] = defaultdict(list)
    for obj in list_parquet_objects(
        s3,
        set(args.year) if args.year else None,
        set(args.court) if args.court else None,
    ):
        groups[(obj.year, obj.court)].append(obj)

    totals = {"groups": 0, "files": 0, "rows_before": 0, "rows_after": 0, "duplicates": 0, "rewritten": 0}
    for (year, court), objects in sorted(groups.items()):
        stats = process_group(s3, objects, args.write)
        if stats["duplicates"]:
            print(
                f"SUMMARY\tyear={year}\tcourt={court}\t"
                f"duplicates={stats['duplicates']}\trewritten={stats['rewritten']}\t"
                f"rows={stats['rows_before']}->{stats['rows_after']}"
            )
        totals["groups"] += 1
        for key in ("files", "rows_before", "rows_after", "duplicates", "rewritten"):
            totals[key] += stats[key]

    print(
        "TOTAL\t"
        + "\t".join(f"{key}={value}" for key, value in totals.items())
        + f"\tmode={'write' if args.write else 'dry-run'}"
    )


if __name__ == "__main__":
    main()
