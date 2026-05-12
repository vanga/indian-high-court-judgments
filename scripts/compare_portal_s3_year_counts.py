#!/usr/bin/env python3
"""Compare eCourts count-only totals with S3 layer counts by court/year."""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from download import CourtDateTask, Downloader
from src.utils.court_utils import from_s3_format, get_court_codes, to_s3_format

for handler in logging.getLogger("download").handlers:
    handler.stream = sys.stderr


@dataclass
class PortalCount:
    total: int | None
    first_page_rows: int
    session_expire_events: int
    api_error_events: int
    error: str = ""
    chunk_errors: int = 0


def parse_count(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    return int(text)


def fetch_portal_count(court_code: str, year: int, attempts: int = 3) -> PortalCount:
    last_error = ""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    for attempt in range(1, attempts + 1):
        try:
            task = CourtDateTask(court_code, start_date, end_date)
            downloader = Downloader(task)
            payload = downloader.default_search_payload()
            payload["from_date"] = start_date
            payload["to_date"] = end_date
            payload["state_code"] = court_code

            downloader.init_user_session()
            payload["app_token"] = downloader.app_token
            response = downloader.request_api("POST", downloader.search_url, payload)
            data = response.json()
            downloader._raise_for_terminal_search_error(data)

            report = data.get("reportrow", {})
            total = (
                parse_count(report.get("iTotalRecords"))
                or parse_count(report.get("iTotalDisplayRecords"))
                or parse_count(data.get("total"))
            )
            return PortalCount(
                total=total,
                first_page_rows=len(report.get("aaData", [])),
                session_expire_events=downloader.task_stats["session_expire_events"],
                api_error_events=downloader.task_stats["errormsg_events"],
            )
        except Exception as exc:
            last_error = str(exc)
            if attempt < attempts:
                time.sleep(attempt)

    return PortalCount(
        total=None,
        first_page_rows=0,
        session_expire_events=0,
        api_error_events=0,
        error=last_error,
    )


def date_chunks(year: int, chunk_days: int):
    current = datetime(year, 1, 1).date()
    end = datetime(year, 12, 31).date()
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)


def fetch_portal_count_chunked(
    court_code: str, year: int, chunk_days: int, attempts: int = 3
) -> PortalCount:
    total = 0
    first_page_rows = 0
    session_expire_events = 0
    api_error_events = 0
    errors = []
    for start_date, end_date in date_chunks(year, chunk_days):
        last_error = ""
        for attempt in range(1, attempts + 1):
            try:
                task = CourtDateTask(court_code, start_date, end_date)
                downloader = Downloader(task)
                payload = downloader.default_search_payload()
                payload["from_date"] = start_date
                payload["to_date"] = end_date
                payload["state_code"] = court_code

                downloader.init_user_session()
                payload["app_token"] = downloader.app_token
                response = downloader.request_api("POST", downloader.search_url, payload)
                data = response.json()
                downloader._raise_for_terminal_search_error(data)

                report = data.get("reportrow", {})
                chunk_total = (
                    parse_count(report.get("iTotalRecords"))
                    or parse_count(report.get("iTotalDisplayRecords"))
                    or parse_count(data.get("total"))
                    or 0
                )
                total += chunk_total
                first_page_rows += len(report.get("aaData", []))
                session_expire_events += downloader.task_stats["session_expire_events"]
                api_error_events += downloader.task_stats["errormsg_events"]
                break
            except Exception as exc:
                last_error = str(exc)
                if attempt < attempts:
                    time.sleep(attempt)
        else:
            errors.append(f"{start_date}:{end_date}:{last_error}")

    return PortalCount(
        total=total if not errors else None,
        first_page_rows=first_page_rows,
        session_expire_events=session_expire_events,
        api_error_events=api_error_events,
        error="; ".join(errors[:3]),
        chunk_errors=len(errors),
    )


def read_s3_audit(path: Path) -> dict[tuple[str, int], dict[str, int]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = {}
        for row in reader:
            key = (row["court"], int(row["year"]))
            rows[key] = {
                "metadata_json_objects": int(row["metadata_json_objects"]),
                "data_pdf_objects": int(row["data_pdf_objects"]),
                "metadata_index_file_count": int(row["metadata_index_file_count"]),
                "data_index_file_count": int(row["data_index_file_count"]),
                "parquet_rows": int(row["parquet_rows"]),
            }
        return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", default="2025,2026")
    parser.add_argument("--courts", default="", help="Comma-separated court codes")
    parser.add_argument("--s3-audit-tsv", required=True)
    parser.add_argument("--chunk-days", type=int, default=0)
    args = parser.parse_args()

    years = [int(value.strip()) for value in args.years.split(",") if value.strip()]
    court_codes = get_court_codes()
    if args.courts:
        courts = [from_s3_format(value.strip()) for value in args.courts.split(",")]
    else:
        courts = list(court_codes.keys())

    s3_rows = read_s3_audit(Path(args.s3_audit_tsv))

    fieldnames = [
        "court",
        "court_name",
        "year",
        "portal_total",
        "raw_json",
        "raw_pdf",
        "metadata_index",
        "data_index",
        "parquet_rows",
        "portal_minus_raw_json",
        "portal_minus_raw_pdf",
        "portal_minus_data_index",
        "portal_minus_parquet",
        "raw_json_minus_parquet",
        "raw_pdf_minus_data_index",
        "first_page_rows",
        "session_expire_events",
        "api_error_events",
        "chunk_errors",
        "error",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()

    for court in courts:
        s3_court = to_s3_format(court)
        for year in years:
            if args.chunk_days > 0:
                portal = fetch_portal_count_chunked(court, year, args.chunk_days)
            else:
                portal = fetch_portal_count(court, year)
            s3 = s3_rows.get((s3_court, year), {})
            raw_json = s3.get("metadata_json_objects", 0)
            raw_pdf = s3.get("data_pdf_objects", 0)
            data_index = s3.get("data_index_file_count", 0)
            parquet_rows = s3.get("parquet_rows", 0)
            portal_total = portal.total
            writer.writerow(
                {
                    "court": s3_court,
                    "court_name": court_codes.get(court, ""),
                    "year": year,
                    "portal_total": "" if portal_total is None else portal_total,
                    "raw_json": raw_json,
                    "raw_pdf": raw_pdf,
                    "metadata_index": s3.get("metadata_index_file_count", 0),
                    "data_index": data_index,
                    "parquet_rows": parquet_rows,
                    "portal_minus_raw_json": ""
                    if portal_total is None
                    else portal_total - raw_json,
                    "portal_minus_raw_pdf": ""
                    if portal_total is None
                    else portal_total - raw_pdf,
                    "portal_minus_data_index": ""
                    if portal_total is None
                    else portal_total - data_index,
                    "portal_minus_parquet": ""
                    if portal_total is None
                    else portal_total - parquet_rows,
                    "raw_json_minus_parquet": raw_json - parquet_rows,
                    "raw_pdf_minus_data_index": raw_pdf - data_index,
                    "first_page_rows": portal.first_page_rows,
                    "session_expire_events": portal.session_expire_events,
                    "api_error_events": portal.api_error_events,
                    "chunk_errors": portal.chunk_errors,
                    "error": portal.error,
                }
            )
            sys.stdout.flush()


if __name__ == "__main__":
    main()
