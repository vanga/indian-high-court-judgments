#!/usr/bin/env python3
"""Probe whether eCourts date filters match parsed result-row Decision Date.

This does not download PDFs. It fetches one search-result page for a
court/date window and parses the same HTML snippet that metadata processing
uses, then compares parsed Decision Date values with the requested filter.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from download import CourtDateTask, Downloader  # noqa: E402
from src.utils.court_utils import from_s3_format  # noqa: E402
from src.utils.html_utils import parse_case_details_from_html  # noqa: E402


DATE_FORMAT = "%d-%m-%Y"
INPUT_DATE_FORMAT = "%Y-%m-%d"


def parse_input_date(value: str):
    return datetime.strptime(value, INPUT_DATE_FORMAT).date()


def parse_portal_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), DATE_FORMAT).date()
    except ValueError:
        return None


def result_html_from_row(row):
    if isinstance(row, list) and len(row) > 1:
        return row[1]
    if isinstance(row, dict):
        return row.get("1") or row.get(1) or row.get("html") or ""
    return ""


def extract_labels(raw_html: str) -> dict[str, str]:
    soup = BeautifulSoup(raw_html, "html.parser")
    labels = {}
    for span in soup.find_all("span"):
        label = span.get_text(" ", strip=True).rstrip(":")
        font = span.find_next_sibling("font")
        if label and font:
            labels[label] = font.get_text(" ", strip=True)
    return labels


def fetch_rows(court: str, start_date: str, end_date: str, page_size: int):
    task = CourtDateTask(from_s3_format(court), start_date, end_date)
    downloader = Downloader(task)
    search_payload = downloader.default_search_payload()
    search_payload["from_date"] = task.from_date
    search_payload["to_date"] = task.to_date
    search_payload["state_code"] = task.court_code
    search_payload["iDisplayLength"] = page_size

    downloader.init_user_session()
    search_payload["app_token"] = downloader.app_token
    response = downloader.request_api("POST", downloader.search_url, search_payload)
    data = response.json()
    downloader._raise_for_terminal_search_error(data)
    report = data.get("reportrow", {})
    return downloader, data, report.get("aaData", [])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--sample-limit", type=int, default=5)
    args = parser.parse_args()

    start = parse_input_date(args.start_date)
    end = parse_input_date(args.end_date)
    downloader, data, rows = fetch_rows(
        args.court, args.start_date, args.end_date, args.page_size
    )

    parsed_rows = []
    outside_samples = []
    missing_samples = []
    label_samples = []
    decisions = []
    registration_dates = []
    disposal_natures = Counter()

    for position, row in enumerate(rows):
        raw_html = result_html_from_row(row)
        details = parse_case_details_from_html(raw_html)
        labels = extract_labels(raw_html)
        decision_date = parse_portal_date(details.get("decision_date"))
        registration_date = parse_portal_date(details.get("date_of_registration"))
        inside = decision_date is not None and start <= decision_date <= end
        parsed = {
            "position": position,
            "cnr": details.get("cnr"),
            "decision_date": details.get("decision_date"),
            "date_of_registration": details.get("date_of_registration"),
            "disposal_nature": details.get("disposal_nature"),
            "court": details.get("court"),
            "inside_filter_window": inside,
        }
        parsed_rows.append(parsed)
        if decision_date:
            decisions.append(decision_date)
        else:
            missing_samples.append(parsed)
        if registration_date:
            registration_dates.append(registration_date)
        if details.get("disposal_nature"):
            disposal_natures[details["disposal_nature"]] += 1
        if not inside and len(outside_samples) < args.sample_limit:
            outside_samples.append(parsed)
        if len(label_samples) < args.sample_limit:
            label_samples.append(
                {
                    "position": position,
                    "labels": labels,
                }
            )

    inside_count = sum(1 for row in parsed_rows if row["inside_filter_window"])
    missing_decision_count = sum(1 for row in parsed_rows if not row["decision_date"])
    report = data.get("reportrow", {})
    count_fields = {
        key: report.get(key, data.get(key))
        for key in [
            "iTotalRecords",
            "iTotalDisplayRecords",
            "recordsTotal",
            "recordsFiltered",
            "total",
        ]
    }

    output = {
        "court": from_s3_format(args.court),
        "court_name": downloader.court_name,
        "from_date": args.start_date,
        "to_date": args.end_date,
        "portal_count_fields": count_fields,
        "rows_fetched": len(rows),
        "parsed_decision_dates": len(decisions),
        "missing_decision_dates": missing_decision_count,
        "inside_decision_date_window": inside_count,
        "outside_decision_date_window": len(parsed_rows) - inside_count,
        "decision_date_min": min(decisions).isoformat() if decisions else None,
        "decision_date_max": max(decisions).isoformat() if decisions else None,
        "registration_date_min": (
            min(registration_dates).isoformat() if registration_dates else None
        ),
        "registration_date_max": (
            max(registration_dates).isoformat() if registration_dates else None
        ),
        "decision_date_counts": {
            day.isoformat(): count
            for day, count in sorted(Counter(decisions).items())
        },
        "top_disposal_natures": disposal_natures.most_common(10),
        "outside_samples": outside_samples,
        "missing_decision_samples": missing_samples[: args.sample_limit],
        "label_samples": label_samples,
        "session_expire_events": downloader.task_stats["session_expire_events"],
        "api_error_events": downloader.task_stats["errormsg_events"],
        "top_level_keys": sorted(data.keys()),
        "reportrow_keys": sorted(report.keys()),
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
