#!/usr/bin/env python3
"""List data/pdf/ objects that are too small to be real PDFs.

Early runs of download.py saved the portal's error responses (HTML "page not
found", JSON {"msg":"error",...}, short text errors) under the judgment's
filename. This script finds them so they can be re-fetched with
``python download.py --refetch-list <csv>``.

Writes a CSV with columns bucket,key,bytes (plus search_date with
--resolve-dates). Reads the public bucket anonymously by default.

Examples:
  # Size-only listing of one court
  python scripts/find_placeholder_pdfs.py --prefix data/pdf/year=2008/court=27_1/ \\
      --output placeholder-pdfs.csv

  # Confirm each candidate's first bytes are not %PDF, and fill search_date
  # from the metadata JSON where the filename date is in another year
  python scripts/find_placeholder_pdfs.py --verify-magic --resolve-dates \\
      --output placeholder-pdfs.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.html_utils import parse_decision_date_from_html  # noqa: E402
from src.utils.refetch_utils import filename_date, parse_refetch_key  # noqa: E402


DEFAULT_BUCKET = "indian-high-court-judgments"
DEFAULT_PREFIX = "data/pdf/"
DEFAULT_MAX_BYTES = 1024
PDF_MAGIC = b"%PDF"


def iter_objects(s3, bucket: str, prefix: str) -> Iterator[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def is_size_candidate(obj: dict, max_bytes: int) -> bool:
    """A .pdf object strictly smaller than max_bytes."""
    return obj["Key"].lower().endswith(".pdf") and obj["Size"] < max_bytes


def read_head(s3, bucket: str, key: str, length: int = 8) -> bytes:
    response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{length - 1}")
    return response["Body"].read()


def is_placeholder(head: bytes) -> bool:
    """Anything that does not start with %PDF is not a PDF (empty included)."""
    return not head.startswith(PDF_MAGIC)


def metadata_key_for(pdf_key: str) -> str:
    """data/pdf/.../X.pdf -> metadata/json/.../X.json"""
    return "metadata/json/" + pdf_key[len("data/pdf/"):-len(".pdf")] + ".json"


def needs_date_resolution(key: str) -> bool:
    """True when the filename date can't be trusted as the search date.

    Partitions are by Decision Date year (parsed from the result HTML). If the
    filename has no date, or its year differs from the partition year, the
    filename date is probably not the Decision Date the portal filters on.
    """
    target, _ = parse_refetch_key(key, search_date="1970-01-01")
    if target is None:
        return False
    when = filename_date(target.filename)
    return when is None or when.year != target.year


def resolve_search_date(s3, bucket: str, pdf_key: str) -> Optional[str]:
    """Decision Date (YYYY-MM-DD) from the key's metadata JSON, if readable."""
    try:
        body = s3.get_object(Bucket=bucket, Key=metadata_key_for(pdf_key))["Body"].read()
        raw_html = json.loads(body).get("raw_html", "")
    except Exception:
        return None
    date_str, _year = parse_decision_date_from_html(raw_html)
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date().isoformat()
    except ValueError:
        return None


def find_placeholders(
    s3,
    bucket: str,
    prefix: str = DEFAULT_PREFIX,
    max_bytes: int = DEFAULT_MAX_BYTES,
    verify_magic: bool = False,
    resolve_dates: bool = False,
    objects: Optional[Iterable[dict]] = None,
) -> Iterator[dict]:
    """Yield {bucket, key, bytes[, search_date]} rows for placeholder objects."""
    if objects is None:
        objects = iter_objects(s3, bucket, prefix)
    for obj in objects:
        if not is_size_candidate(obj, max_bytes):
            continue
        key = obj["Key"]
        # A ranged GET on an empty object fails, and empty is a placeholder anyway.
        if verify_magic and obj["Size"] > 0:
            if not is_placeholder(read_head(s3, bucket, key)):
                continue
        row = {"bucket": bucket, "key": key, "bytes": obj["Size"]}
        if resolve_dates:
            search_date = ""
            if needs_date_resolution(key):
                search_date = resolve_search_date(s3, bucket, key) or ""
            row["search_date"] = search_date
        yield row


def make_client(signed: bool):
    if signed:
        return boto3.client("s3")
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Key prefix to scan (default data/pdf/); narrow it to one year or court",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="Objects strictly smaller than this are candidates (default 1024)",
    )
    parser.add_argument(
        "--verify-magic",
        action="store_true",
        help="Read each candidate's first bytes and keep only those not starting with %%PDF",
    )
    parser.add_argument(
        "--resolve-dates",
        action="store_true",
        help=(
            "Add a search_date column from the metadata JSON's Decision Date for "
            "keys whose filename date is missing or in another year than the partition"
        ),
    )
    parser.add_argument(
        "--signed",
        action="store_true",
        help="Use AWS credentials instead of anonymous access",
    )
    parser.add_argument("--output", required=True, help="CSV file to write")
    args = parser.parse_args(argv)

    s3 = make_client(args.signed)
    fields = ["bucket", "key", "bytes"] + (["search_date"] if args.resolve_dates else [])
    count = 0
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in find_placeholders(
            s3,
            args.bucket,
            args.prefix,
            args.max_bytes,
            args.verify_magic,
            args.resolve_dates,
        ):
            writer.writerow(row)
            count += 1
    print(f"Wrote {count} placeholder candidate(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
