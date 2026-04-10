"""Remove orphaned sync-part entries from S3 index files.

These entries were created by a partial pipeline run on Feb 20, 2026.
They reference files that exist as loose objects but have no tar archive,
inflating file_count and causing the pipeline to skip re-uploads.

Usage:
    # Dry run (default) — shows what would change
    python cleanup_sync_parts.py

    # Actually fix the index files
    python cleanup_sync_parts.py --apply

    # Target a specific bucket
    S3_WRITE_BUCKET=indian-high-court-judgments-test python cleanup_sync_parts.py --apply
"""

import argparse
import json
import os

import boto3
from botocore import UNSIGNED
from botocore.client import Config

BUCKET = os.environ.get("S3_WRITE_BUCKET", "indian-high-court-judgments")


def find_and_clean_indexes(apply: bool):
    s3_unsigned = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3_write = boto3.client("s3")

    prefixes = ["metadata/tar/", "data/tar/"]
    total_cleaned = 0

    for prefix in prefixes:
        paginator = s3_unsigned.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".index.json"):
                    continue

                resp = s3_unsigned.get_object(Bucket=BUCKET, Key=key)
                index = json.loads(resp["Body"].read())

                parts = index.get("parts", [])
                orphaned = [p for p in parts if p["name"].startswith("sync-part")]
                if not orphaned:
                    continue

                orphaned_files = sum(p.get("file_count", len(p.get("files", []))) for p in orphaned)
                print(f"\n{key}")
                for p in orphaned:
                    print(f"  ORPHAN: {p['name']} ({p.get('file_count', '?')} files)")

                if not apply:
                    print(f"  [dry-run] Would remove {len(orphaned)} orphaned part(s), {orphaned_files} file refs")
                    total_cleaned += len(orphaned)
                    continue

                # Remove orphaned parts and recalculate aggregates
                clean_parts = [p for p in parts if not p["name"].startswith("sync-part")]
                index["parts"] = clean_parts
                index["file_count"] = sum(p.get("file_count", len(p.get("files", []))) for p in clean_parts)
                index["tar_size"] = sum(p.get("size", 0) for p in clean_parts)

                from src.utils.shared_utils import format_size
                index["tar_size_human"] = format_size(index["tar_size"])

                s3_write.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=json.dumps(index, indent=2),
                    ContentType="application/json",
                )
                print(f"  FIXED: removed {len(orphaned)} part(s), {orphaned_files} file refs. New file_count={index['file_count']}")
                total_cleaned += len(orphaned)

    if total_cleaned == 0:
        print("\nNo orphaned sync-part entries found.")
    else:
        action = "Cleaned" if apply else "Found"
        print(f"\n{action} {total_cleaned} orphaned sync-part entries.")
        if not apply:
            print("Run with --apply to fix.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean orphaned sync-part entries from S3 index files")
    parser.add_argument("--apply", action="store_true", help="Actually modify index files (default is dry-run)")
    args = parser.parse_args()

    print(f"Bucket: {BUCKET}")
    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}\n")
    find_and_clean_indexes(apply=args.apply)
