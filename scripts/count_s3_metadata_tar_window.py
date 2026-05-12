#!/usr/bin/env python3
"""Count metadata tar JSON entries by parsed HTML Decision Date."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
import tarfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.html_utils import parse_decision_date_from_html  # noqa: E402


BUCKET = "indian-high-court-judgments"


def index_key(year: int, court: str, bench: str):
    return (
        f"metadata/tar/year={year}/court={court}/bench={bench}/"
        "metadata.index.json"
    )


def parse_member_date(member_file) -> date | None:
    metadata = json.load(member_file)
    date_str, _year = parse_decision_date_from_html(metadata.get("raw_html", ""))
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3_22 or 3~22")
    parser.add_argument("--bench", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample", type=int, default=5)
    args = parser.parse_args()

    court = args.court.replace("~", "_")
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    years = range(start.year, end.year + 1)
    s3 = boto3.client("s3")

    counts: Counter[date] = Counter()
    parsed = 0
    missing_or_unparseable = 0
    members_seen = 0
    tar_parts = 0
    samples = []

    for year in years:
        key = index_key(year, court, args.bench)
        index = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        prefix = key.rsplit("/", 1)[0] + "/"
        for part in index.get("parts", []):
            tar_key = prefix + part["name"]
            body = s3.get_object(Bucket=BUCKET, Key=tar_key)["Body"].read()
            tar_parts += 1
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                with tarfile.open(fileobj=gz, mode="r:") as tar:
                    for member in tar:
                        if not member.isfile() or not member.name.endswith(".json"):
                            continue
                        members_seen += 1
                        extracted = tar.extractfile(member)
                        if extracted is None:
                            missing_or_unparseable += 1
                            continue
                        decision_date = parse_member_date(extracted)
                        if decision_date is None:
                            missing_or_unparseable += 1
                            continue
                        parsed += 1
                        if start <= decision_date <= end:
                            counts[decision_date] += 1
                            if len(samples) < args.sample:
                                samples.append(
                                    {
                                        "member": member.name,
                                        "decision_date": decision_date.isoformat(),
                                    }
                                )

    print(f"court={court}")
    print(f"bench={args.bench}")
    print(f"start={start}")
    print(f"end={end}")
    print(f"tar_parts={tar_parts}")
    print(f"tar_members_seen={members_seen}")
    print(f"tar_members_dates_parsed={parsed}")
    print(f"tar_members_missing_or_unparseable_date={missing_or_unparseable}")
    print(f"window_metadata_tar_json={sum(counts.values())}")
    for decision_date, count in sorted(counts.items()):
        print(f"{decision_date}\t{count}")
    print("sample_records=" + json.dumps(samples, sort_keys=True))


if __name__ == "__main__":
    main()
