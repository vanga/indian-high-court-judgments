#!/usr/bin/env python3
"""Inspect live portal and parquet rows for specific CNRs in a date window."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compare_portal_parquet_window import fetch_portal_records, parquet_records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--cnr", action="append", required=True)
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    portal_total, portal = fetch_portal_records(args.court, args.start_date, args.end_date)
    parquet = parquet_records(args.court, start, end)

    output = {
        "court": args.court,
        "from_date": args.start_date,
        "to_date": args.end_date,
        "portal_total": portal_total,
        "portal_rows": len(portal),
        "portal_unique_cnr": len({r["cnr"] for r in portal if r["cnr"]}),
        "portal_unique_pdf": len({r["pdf_basename"] for r in portal if r["pdf_basename"]}),
        "cnrs": {},
    }
    for cnr in args.cnr:
        output["cnrs"][cnr] = {
            "portal": [r for r in portal if r["cnr"] == cnr],
            "parquet": [r for r in parquet if r["cnr"] == cnr],
        }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
