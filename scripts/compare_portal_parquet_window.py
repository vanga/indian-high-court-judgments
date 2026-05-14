#!/usr/bin/env python3
"""Compare live eCourts rows with S3 parquet rows for one court/date window."""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import date
from pathlib import Path, PurePosixPath

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from download import CourtDateTask, Downloader, page_size  # noqa: E402
from src.utils.court_utils import from_s3_format, to_s3_format  # noqa: E402
from src.utils.html_utils import parse_case_details_from_html  # noqa: E402


BUCKET = "indian-high-court-judgments"


def result_html_from_row(row):
    if isinstance(row, list) and len(row) > 1:
        return row[1]
    if isinstance(row, dict):
        return row.get("1") or row.get(1) or row.get("html") or ""
    return ""


def fetch_portal_records(court: str, start_date: str, end_date: str):
    task = CourtDateTask(from_s3_format(court), start_date, end_date)
    downloader = Downloader(task)
    payload = downloader.default_search_payload()
    payload["from_date"] = task.from_date
    payload["to_date"] = task.to_date
    payload["state_code"] = task.court_code

    downloader.init_user_session()
    payload["app_token"] = downloader.app_token
    expected_total = None
    records = []

    while True:
        response = downloader.request_api("POST", downloader.search_url, payload)
        data = response.json()
        downloader._raise_for_terminal_search_error(data)
        report = data.get("reportrow", {})
        if expected_total is None:
            expected_total = int(str(report.get("iTotalRecords", 0)).replace(",", ""))
        rows = report.get("aaData", [])
        if not rows:
            break
        for row in rows:
            raw_html = result_html_from_row(row)
            details = parse_case_details_from_html(raw_html)
            fragment = None
            try:
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(raw_html, "html.parser")
                if soup.button and "onclick" in soup.button.attrs:
                    fragment = downloader.extract_pdf_fragment(soup.button["onclick"])
            except Exception:
                fragment = None
            records.append(
                {
                    "cnr": details.get("cnr"),
                    "decision_date": details.get("decision_date"),
                    "pdf_link": fragment,
                    "pdf_basename": PurePosixPath(fragment or "").name,
                }
            )
        if len(rows) < page_size:
            break
        payload = downloader._prepare_next_iteration(payload)
        payload["app_token"] = downloader.app_token

    return expected_total, records


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def parquet_records(court: str, start: date, end: date):
    court = to_s3_format(court)
    s3 = boto3.client("s3")
    records = []
    for year in range(start.year, end.year + 1):
        prefix = f"metadata/parquet/year={year}/court={court}/"
        for obj in iter_objects(s3, prefix):
            if not obj["Key"].endswith("/metadata.parquet"):
                continue
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link", "cnr", "decision_date"])
            dates = pd.to_datetime(df["decision_date"], errors="coerce").dt.date
            filtered = df.loc[(dates >= start) & (dates <= end)].copy()
            for item in filtered.to_dict("records"):
                pdf_link = item.get("pdf_link") or ""
                records.append(
                    {
                        "cnr": item.get("cnr"),
                        "decision_date": str(item.get("decision_date")),
                        "pdf_link": pdf_link,
                        "pdf_basename": PurePosixPath(pdf_link).name,
                    }
                )
    return records


def sample(records, limit: int):
    return records[:limit]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--sample", type=int, default=20)
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    portal_total, portal = fetch_portal_records(args.court, args.start_date, args.end_date)
    parquet = parquet_records(args.court, start, end)

    portal_pdf = {r["pdf_basename"] for r in portal if r["pdf_basename"]}
    parquet_pdf = {r["pdf_basename"] for r in parquet if r["pdf_basename"]}
    portal_cnr = {r["cnr"] for r in portal if r["cnr"]}
    parquet_cnr = {r["cnr"] for r in parquet if r["cnr"]}
    parquet_by_pdf = {r["pdf_basename"]: r for r in parquet if r["pdf_basename"]}
    portal_by_pdf = {r["pdf_basename"]: r for r in portal if r["pdf_basename"]}
    parquet_by_cnr = {}
    for record in parquet:
        parquet_by_cnr.setdefault(record["cnr"], []).append(record)

    extra_pdf = sorted(parquet_pdf - portal_pdf)
    missing_pdf = sorted(portal_pdf - parquet_pdf)
    duplicate_cnr_records = [
        values
        for values in parquet_by_cnr.values()
        if values and values[0].get("cnr") and len(values) > 1
    ]

    output = {
        "court": from_s3_format(args.court),
        "from_date": args.start_date,
        "to_date": args.end_date,
        "portal_total": portal_total,
        "portal_rows_fetched": len(portal),
        "portal_pdf_unique": len(portal_pdf),
        "portal_cnr_unique": len(portal_cnr),
        "parquet_rows": len(parquet),
        "parquet_pdf_unique": len(parquet_pdf),
        "parquet_cnr_unique": len(parquet_cnr),
        "extra_parquet_pdf_count": len(extra_pdf),
        "missing_parquet_pdf_count": len(missing_pdf),
        "extra_parquet_cnr_count": len(parquet_cnr - portal_cnr),
        "missing_parquet_cnr_count": len(portal_cnr - parquet_cnr),
        "duplicate_parquet_cnr_groups": len(duplicate_cnr_records),
        "sample_extra_parquet_pdf": [
            parquet_by_pdf[name] for name in extra_pdf[: args.sample]
        ],
        "sample_missing_parquet_pdf": [
            portal_by_pdf[name] for name in missing_pdf[: args.sample]
        ],
        "sample_duplicate_parquet_cnr": duplicate_cnr_records[: args.sample],
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
