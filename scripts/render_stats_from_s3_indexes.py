#!/usr/bin/env python3
"""Render STATS.md from S3 tar indexes and a portal/S3 comparison CSV.

The comparison CSV is produced by compare_portal_baseline_to_s3_indexes.py.
Counts come from *.index.json summaries; sizes come from public S3 tar object
bytes so this can run without downloading the dataset.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import boto3
from botocore import UNSIGNED
from botocore.config import Config


BUCKET = "indian-high-court-judgments"
TAR_KEY_RE = re.compile(
    r"^(?P<kind>data|metadata)/tar/year=(?P<year>\d{4})/"
    r"court=(?P<court>[^/]+)/bench=(?P<bench>[^/]+)/"
)


@dataclass
class CountRow:
    court: str
    court_name: str
    year: int
    pdfs: int
    metadata: int


def parse_int(value: str | None) -> int:
    value = (value or "").strip()
    return int(value) if value else 0


def display_court_code(s3_code: str) -> str:
    return s3_code.replace("_", "~")


def gib(size_bytes: int) -> float:
    return size_bytes / (1024**3)


def tib(size_bytes: int) -> float:
    return size_bytes / (1024**4)


def iter_s3_objects(prefix: str) -> Iterable[dict]:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def load_counts(path: Path) -> list[CountRow]:
    rows: list[CountRow] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                CountRow(
                    court=row["court"],
                    court_name=row.get("court_name", row["court"]),
                    year=parse_int(row["year"]),
                    pdfs=parse_int(row.get("data_index")),
                    metadata=parse_int(row.get("metadata_index")),
                )
            )
    return rows


def load_tar_sizes() -> tuple[dict[tuple[str, int], int], set[tuple[str, str]], int]:
    sizes: dict[tuple[str, int], int] = defaultdict(int)
    benches: set[tuple[str, str]] = set()
    total = 0

    for prefix in ("data/tar/", "metadata/tar/"):
        for obj in iter_s3_objects(prefix):
            key = obj["Key"]
            if key.endswith(".index.json") or not (key.endswith(".tar") or key.endswith(".tar.gz")):
                continue
            match = TAR_KEY_RE.match(key)
            if not match:
                continue
            court = match.group("court")
            year = int(match.group("year"))
            bench = match.group("bench")
            size = int(obj.get("Size", 0))
            sizes[(court, year)] += size
            benches.add((court, bench))
            total += size

    return sizes, benches, total


def render_markdown(rows: list[CountRow], snapshot: date, min_year_gib: float) -> str:
    sizes, benches, total_size = load_tar_sizes()

    court_names = {row.court: row.court_name for row in rows}
    def empty_totals() -> dict[str, int]:
        return {"pdfs": 0, "metadata": 0, "size": 0}

    court_totals: dict[str, dict[str, int]] = defaultdict(empty_totals)
    year_totals: dict[int, dict[str, int]] = defaultdict(empty_totals)

    for row in rows:
        court_totals[row.court]["pdfs"] += row.pdfs
        court_totals[row.court]["metadata"] += row.metadata
        year_totals[row.year]["pdfs"] += row.pdfs
        year_totals[row.year]["metadata"] += row.metadata

    for (court, year), size in sizes.items():
        court_totals[court]["size"] += size
        year_totals[year]["size"] += size

    nonzero_years = [
        year
        for year, values in year_totals.items()
        if values["pdfs"] or values["metadata"] or values["size"]
    ]
    total_pdfs = sum(values["pdfs"] for values in court_totals.values())
    total_metadata = sum(values["metadata"] for values in court_totals.values())
    top_year = max(year_totals.items(), key=lambda item: item[1]["pdfs"])
    top_court = max(court_totals.items(), key=lambda item: item[1]["pdfs"])

    active_courts = sum(
        1 for values in court_totals.values() if values["pdfs"] or values["metadata"]
    )

    lines = [
        "# Dataset Size",
        "",
        (
            f"> **Snapshot as of {snapshot:%B %-d, %Y}.** These numbers are a point-in-time "
            "view generated from S3 `metadata.index.json` and `data.index.json` records, "
            "with S3 tar archive object bytes from the same audit. Daily scraping adds new "
            "judgments continuously, so the totals below will drift."
        ),
        "",
        "## Totals",
        "",
        "| | |",
        "|---|---:|",
        f"| Judgments/PDFs | **{total_pdfs:,}** |",
        f"| Metadata records | **{total_metadata:,}** |",
        f"| Total archive size | **{gib(total_size):,.2f} GiB** ({tib(total_size):.2f} TiB) |",
        f"| Courts | **{active_courts}** |",
        f"| Benches | **{len(benches)}** |",
        f"| Years covered | **{min(nonzero_years)}-{max(nonzero_years)}** |",
        (
            f"| Year with highest volume | **{top_year[0]}** "
            f"({gib(top_year[1]['size']):,.2f} GiB, {top_year[1]['pdfs']:,} PDFs) |"
        ),
        (
            f"| Court with highest volume | **{court_names.get(top_court[0], top_court[0])}** "
            f"(`{display_court_code(top_court[0])}`, {gib(top_court[1]['size']):,.2f} GiB, "
            f"{top_court[1]['pdfs']:,} PDFs) |"
        ),
        "",
        "## By Court",
        "",
        "| Code | Court | PDFs | Metadata | Size (GiB) |",
        "|---|---|---:|---:|---:|",
    ]

    sorted_courts = sorted(
        court_totals.items(), key=lambda item: item[1]["pdfs"], reverse=True
    )
    for court, values in sorted_courts:
        if not (values["pdfs"] or values["metadata"]):
            continue
        lines.append(
            f"| {display_court_code(court)} | {court_names.get(court, court)} | "
            f"{values['pdfs']:,} | {values['metadata']:,} | {gib(values['size']):,.2f} |"
        )

    lines.extend(
        [
            "",
            "## By Year",
            "",
            f"Years below {min_year_gib:g} GiB are omitted for brevity.",
            "",
            "| Year | PDFs | Metadata | Size (GiB) |",
            "|---:|---:|---:|---:|",
        ]
    )

    for year, values in sorted(year_totals.items()):
        if not (values["pdfs"] or values["metadata"] or values["size"]):
            continue
        if gib(values["size"]) < min_year_gib:
            continue
        lines.append(
            f"| {year} | {values['pdfs']:,} | {values['metadata']:,} | {gib(values['size']):,.2f} |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-csv", type=Path, required=True)
    parser.add_argument("--snapshot-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--min-year-gib", type=float, default=1.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    markdown = render_markdown(load_counts(args.compare_csv), args.snapshot_date, args.min_year_gib)
    if args.output:
        args.output.write_text(markdown, encoding="utf-8")
    else:
        sys.stdout.write(markdown)


if __name__ == "__main__":
    main()
