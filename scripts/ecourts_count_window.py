#!/usr/bin/env python3
"""Fetch eCourts search count for a court/date window without downloading files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from download import CourtDateTask, Downloader


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--court", required=True, help="Court code, e.g. 3~22")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--display-start", type=int, default=0)
    args = parser.parse_args()

    task = CourtDateTask(args.court, args.start_date, args.end_date)
    downloader = Downloader(task)
    search_payload = downloader.default_search_payload()
    search_payload["from_date"] = task.from_date
    search_payload["to_date"] = task.to_date
    search_payload["state_code"] = task.court_code
    search_payload["iDisplayStart"] = args.display_start

    downloader.init_user_session()
    search_payload["app_token"] = downloader.app_token
    response = downloader.request_api("POST", downloader.search_url, search_payload)
    data = response.json()
    downloader._raise_for_terminal_search_error(data)

    report = data.get("reportrow", {})
    rows = report.get("aaData", [])
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
    print(
        json.dumps(
            {
                "court": task.court_code,
                "from_date": task.from_date,
                "to_date": task.to_date,
                "first_page_rows": len(rows),
                "display_start": args.display_start,
                "count_fields": count_fields,
                "top_level_keys": sorted(data.keys()),
                "reportrow_keys": sorted(report.keys()),
                "session_expire_events": downloader.task_stats["session_expire_events"],
                "api_error_events": downloader.task_stats["errormsg_events"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
