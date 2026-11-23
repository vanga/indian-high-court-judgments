#!/usr/bin/env python3
"""Backfill ``members`` (name + size) into existing data.index.json parts.

Parts written before sizes were recorded list only ``files``. The PDF objects
under data/pdf/ hold the same bytes as the tar members, so one S3 listing per
bench/year gives every size without downloading a tar.

The same listing is the expensive part of the placeholder audit, so this
script can also write the placeholder candidates it sees (objects under
--max-bytes) in find_placeholder_pdfs.py's CSV format.

A part gets ``members`` only if every name in its ``files`` has a data/pdf
object; otherwise it is left unchanged and counted as unresolved.

Dry run by default. With --apply, each index is written with a conditional PUT
(If-Match on the ETag that was read), so a concurrent pipeline write is never
overwritten: that index is reported as "conflict" and can be re-run.

Examples:
  # Count what would change for one court, no writes
  python scripts/backfill_index_member_sizes.py --prefix data/tar/year=2008/court=27_1/

  # Write sizes everywhere and collect placeholder candidates in the same pass
  python scripts/backfill_index_member_sizes.py --apply --verify-magic \\
      --placeholders-csv placeholder-pdfs.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.find_placeholder_pdfs import DEFAULT_MAX_BYTES, find_placeholders  # noqa: E402
from src.models import IndexFileV2, members_for  # noqa: E402
from src.utils.shared_utils import utc_now_iso  # noqa: E402

BUCKET = "indian-high-court-judgments"
INDEX_NAME = "data.index.json"


@dataclass
class BenchResult:
    index_key: str
    status: str = "unchanged"  # unchanged | updated | would_update | conflict | error
    parts_filled: int = 0
    parts_unresolved: int = 0
    names_without_object: int = 0
    placeholders: List[dict] = field(default_factory=list)
    error: str = ""


def iter_objects(s3, bucket: str, prefix: str) -> Iterator[dict]:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def pdf_prefix_for(index_key: str) -> str:
    """data/tar/year=Y/court=C/bench=B/data.index.json -> data/pdf/year=Y/court=C/bench=B/"""
    return "data/pdf/" + index_key[len("data/tar/"):-len(INDEX_NAME)]


def backfill_index(
    s3,
    bucket: str,
    index_key: str,
    apply: bool,
    collect_placeholders: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
    verify_magic: bool = False,
) -> BenchResult:
    result = BenchResult(index_key)
    try:
        objects = list(iter_objects(s3, bucket, pdf_prefix_for(index_key)))
        sizes: Dict[str, int] = {o["Key"].rsplit("/", 1)[1]: o["Size"] for o in objects}
        if collect_placeholders:
            result.placeholders = list(
                find_placeholders(
                    s3, bucket, max_bytes=max_bytes, verify_magic=verify_magic, objects=objects
                )
            )

        resp = s3.get_object(Bucket=bucket, Key=index_key)
        etag = resp["ETag"]
        index = IndexFileV2.model_validate(json.loads(resp["Body"].read()))
        for part in index.parts:
            if part.members is not None:
                continue
            members = members_for(part.files, sizes)
            if members is None:
                result.parts_unresolved += 1
                result.names_without_object += sum(1 for n in part.files if n not in sizes)
                continue
            part.members = members
            result.parts_filled += 1

        if not result.parts_filled:
            return result
        if not apply:
            result.status = "would_update"
            return result
        # Sizes are an annotation, not new content: updated_at is left alone so
        # consumers keying off it do not re-download every index.
        try:
            s3.put_object(
                Bucket=bucket,
                Key=index_key,
                Body=index.model_dump_json(indent=2),
                ContentType="application/json",
                IfMatch=etag,
            )
            result.status = "updated"
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("PreconditionFailed", "ConditionalRequestConflict"):
                result.status = "conflict"
            else:
                raise
    except Exception as e:  # noqa: BLE001 - one bad bench must not stop the pass
        result.status = "error"
        result.error = str(e)[:300]
    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--prefix", default="data/tar/", help="Index prefix to scan (default data/tar/)")
    parser.add_argument("--apply", action="store_true", help="Write the indexes (default: dry run)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--placeholders-csv", help="Also write placeholder candidates to this CSV")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument(
        "--verify-magic",
        action="store_true",
        help="Keep only candidates whose first bytes are not %%PDF (reads each candidate's head)",
    )
    parser.add_argument("--report", help="Write a per-index CSV report here")
    args = parser.parse_args(argv)
    if not args.prefix.startswith("data/tar/"):
        parser.error("--prefix must be under data/tar/")

    s3 = boto3.client("s3")
    index_keys = [
        o["Key"] for o in iter_objects(s3, args.bucket, args.prefix) if o["Key"].endswith("/" + INDEX_NAME)
    ]
    print(f"{utc_now_iso()} {len(index_keys)} index file(s) under {args.prefix}; apply={args.apply}")

    results: List[BenchResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(
                backfill_index, s3, args.bucket, k, args.apply,
                bool(args.placeholders_csv), args.max_bytes, args.verify_magic,
            )
            for k in index_keys
        ]
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            results.append(r)
            if r.status in ("conflict", "error"):
                print(f"  {r.status}: {r.index_key} {r.error}")
            if i % 200 == 0:
                print(f"  {i}/{len(futures)} indexes done")

    if args.placeholders_csv:
        with open(args.placeholders_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["bucket", "key", "bytes"])
            w.writeheader()
            for r in sorted(results, key=lambda r: r.index_key):
                w.writerows(r.placeholders)
    if args.report:
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["index_key", "status", "parts_filled", "parts_unresolved", "names_without_object", "error"])
            for r in sorted(results, key=lambda r: r.index_key):
                w.writerow([r.index_key, r.status, r.parts_filled, r.parts_unresolved, r.names_without_object, r.error])

    by_status: Dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1
    print(
        f"{utc_now_iso()} indexes by status: {by_status}; "
        f"parts filled: {sum(r.parts_filled for r in results)}; "
        f"parts left without sizes: {sum(r.parts_unresolved for r in results)} "
        f"({sum(r.names_without_object for r in results)} names with no data/pdf object); "
        f"placeholder candidates: {sum(len(r.placeholders) for r in results)}"
    )
    return 1 if any(r.status in ("conflict", "error") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
