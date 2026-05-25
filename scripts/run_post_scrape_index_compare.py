#!/usr/bin/env python3
"""Wait for a scrape process, then produce portal-vs-S3-index reports."""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def csv_values(path: Path, column: str) -> list[str]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return sorted({row[column] for row in reader if row.get(column)})


def write_summary(compare_csv: Path, summary_path: Path) -> None:
    with compare_csv.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    def as_int(row: dict[str, str], key: str) -> int:
        return int(row.get(key) or 0)

    positive = sorted(
        rows, key=lambda row: as_int(row, "portal_minus_data_index"), reverse=True
    )[:30]
    negative = sorted(rows, key=lambda row: as_int(row, "portal_minus_data_index"))[:30]
    total_portal = sum(as_int(row, "portal_total") for row in rows)
    total_data_index = sum(as_int(row, "data_index") for row in rows)
    total_metadata_index = sum(as_int(row, "metadata_index") for row in rows)

    with summary_path.open("w") as handle:
        handle.write(f"rows={len(rows)}\n")
        handle.write(f"portal_total={total_portal}\n")
        handle.write(f"metadata_index={total_metadata_index}\n")
        handle.write(f"data_index={total_data_index}\n")
        handle.write(f"portal_minus_data_index={total_portal - total_data_index}\n")
        handle.write(f"metadata_index_minus_data_index={total_metadata_index - total_data_index}\n")
        handle.write("\nTop positive portal_minus_data_index gaps:\n")
        for row in positive:
            handle.write(
                f"{row['court']}\t{row['year']}\tportal={row['portal_total']}\t"
                f"data_index={row['data_index']}\tgap={row['portal_minus_data_index']}\t"
                f"{row.get('court_name', '')}\n"
            )
        handle.write("\nTop negative portal_minus_data_index gaps:\n")
        for row in negative:
            handle.write(
                f"{row['court']}\t{row['year']}\tportal={row['portal_total']}\t"
                f"data_index={row['data_index']}\tgap={row['portal_minus_data_index']}\t"
                f"{row.get('court_name', '')}\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-pid", type=int, required=True)
    parser.add_argument("--portal-baseline", type=Path, required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--audit-out", type=Path, required=True)
    parser.add_argument("--compare-out", type=Path, required=True)
    parser.add_argument("--summary-out", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()

    while process_exists(args.wait_pid):
        time.sleep(args.poll_seconds)

    subprocess.run(
        [
            sys.executable,
            "scripts/sync_local_backfill_minimal.py",
            "--end-date",
            args.end_date,
        ],
        check=True,
        env={**os.environ, "PYTHONPATH": "."},
    )

    courts = ",".join(csv_values(args.portal_baseline, "court"))
    years = ",".join(csv_values(args.portal_baseline, "year"))
    with args.audit_out.open("w") as audit_handle:
        subprocess.run(
            [
                sys.executable,
                "scripts/audit_s3_index_counts.py",
                "--courts",
                courts,
                "--years",
                years,
            ],
            check=True,
            stdout=audit_handle,
            env={**os.environ, "PYTHONPATH": "."},
        )

    with args.compare_out.open("w") as compare_handle:
        subprocess.run(
            [
                sys.executable,
                "scripts/compare_portal_baseline_to_s3_indexes.py",
                "--portal-baseline",
                str(args.portal_baseline),
                "--s3-index-audit",
                str(args.audit_out),
            ],
            check=True,
            stdout=compare_handle,
            env={**os.environ, "PYTHONPATH": "."},
        )

    write_summary(args.compare_out, args.summary_out)


if __name__ == "__main__":
    main()
