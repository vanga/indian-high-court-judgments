#!/usr/bin/env python3
"""Expand weekly failed windows into daily tasks and run them via one queue."""

from __future__ import annotations

import argparse
import concurrent.futures
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True, order=True)
class DayTask:
    court_code: str
    day: str


def parse_tsv(path: Path) -> list[tuple[str, str, str]]:
    rows = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        court_code, from_date, to_date = line.split("\t")
        rows.append((court_code, from_date, to_date))
    return rows


def expand_daily_tasks(path: Path) -> list[DayTask]:
    tasks: set[DayTask] = set()
    for court_code, from_date, to_date in parse_tsv(path):
        current = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date)
        while current <= end:
            tasks.add(DayTask(court_code, current.isoformat()))
            current += timedelta(days=1)
    return sorted(tasks)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_task(
    task: DayTask,
    log_path: Path,
    write_lock: threading.Lock,
    compression: bool,
    download_args: list[str],
) -> int:
    cmd = [
        sys.executable,
        "download.py",
        "--court_code",
        task.court_code,
        "--start_date",
        task.day,
        "--end_date",
        task.day,
        "--day_step",
        "1",
        "--max_workers",
        "1",
    ]
    if compression:
        cmd.append("--compress-pdfs")
    cmd.extend(download_args)

    with write_lock:
        with log_path.open("a") as handle:
            handle.write(
                f"rerun_task court={task.court_code} from={task.day} "
                f"to={task.day} started_at={utc_now()}\n"
            )

    result = subprocess.run(
        cmd,
        cwd=log_path.parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    with write_lock:
        with log_path.open("a") as handle:
            if result.stdout:
                handle.write(result.stdout)
            if result.stderr:
                handle.write(result.stderr)
            handle.write(
                f"rerun_task court={task.court_code} from={task.day} "
                f"to={task.day} finished_at={utc_now()} "
                f"returncode={result.returncode}\n"
            )
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weekly-tsv", required=True, type=Path)
    parser.add_argument("--log-path", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--compress-pdfs", action="store_true")
    parser.add_argument(
        "--download-arg",
        action="append",
        default=[],
        help=(
            "Additional argument passed to download.py. Repeat for option/value "
            "pairs, for example --download-arg --dist-code --download-arg 1."
        ),
    )
    args = parser.parse_args()

    args.log_path.parent.mkdir(parents=True, exist_ok=True)
    daily_tasks = expand_daily_tasks(args.weekly_tsv)
    args.log_path.write_text(
        f"watcher_started_at={utc_now()} mode=global_daily_queue "
        f"workers={args.workers} daily_tasks={len(daily_tasks)} "
        f"source_weekly_tsv={args.weekly_tsv}\n"
    )

    lock = threading.Lock()
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                run_task,
                task,
                args.log_path,
                lock,
                args.compress_pdfs,
                args.download_arg,
            )
            for task in daily_tasks
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.result() != 0:
                failures += 1

    with args.log_path.open("a") as handle:
        handle.write(
            f"watcher_finished_at={utc_now()} failures={failures} "
            f"daily_tasks={len(daily_tasks)}\n"
        )

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
