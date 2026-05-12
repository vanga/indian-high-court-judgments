#!/usr/bin/env python3
"""Rerun court/date tasks that failed after downloader retries.

The downloader intentionally logs transient per-attempt failures. This script
only follows up tasks that reached the final `_run_tasks` failure collection,
then reruns those exact windows with S3 sync and PDF compression.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


TASK_RE = re.compile(
    r"CourtDateTask\([^)]*court_code=([^,\s]+), "
    r"from_date=(\d{4}-\d{2}-\d{2}), "
    r"to_date=(\d{4}-\d{2}-\d{2})"
)
RERUN_FINISHED_RE = re.compile(
    r"rerun_task court=([^,\s]+) "
    r"from=(\d{4}-\d{2}-\d{2}) "
    r"to=(\d{4}-\d{2}-\d{2}) .* returncode=(\d+)"
)


@dataclass(frozen=True, order=True)
class FailedTask:
    court_code: str
    from_date: str
    to_date: str


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def process_running(pid: int) -> bool:
    result = subprocess.run(
        ["ps", "-p", str(pid)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def wait_for_pid(pid: int, poll_seconds: int) -> None:
    while process_running(pid):
        print(
            f"waiting_for_primary_pid={pid} at="
            f"{datetime.now(timezone.utc).isoformat()}",
            flush=True,
        )
        time.sleep(poll_seconds)


def parse_failed_tasks(log_path: Path) -> list[FailedTask]:
    tasks: set[FailedTask] = set()
    if not log_path.exists():
        return []

    for line in log_path.read_text(errors="replace").splitlines():
        if (
            "Task failed after retries" not in line
            and not line.lstrip().startswith("- CourtDateTask(")
        ):
            continue
        match = TASK_RE.search(line)
        if not match:
            continue
        tasks.add(FailedTask(*match.groups()))
    return sorted(tasks)


def parse_failed_reruns(log_path: Path) -> list[FailedTask]:
    tasks: set[FailedTask] = set()
    if not log_path.exists():
        return []

    for line in log_path.read_text(errors="replace").splitlines():
        match = RERUN_FINISHED_RE.search(line)
        if not match:
            continue
        court_code, from_date, to_date, returncode = match.groups()
        if int(returncode) != 0:
            tasks.add(FailedTask(court_code, from_date, to_date))
    return sorted(tasks)


def run_task(task: FailedTask, log_path: Path, max_workers: int) -> bool:
    cmd = [
        sys.executable,
        "download.py",
        "--court_code",
        task.court_code,
        "--start_date",
        task.from_date,
        "--end_date",
        task.to_date,
        "--day_step",
        "1",
        "--max_workers",
        str(max_workers),
        "--sync-s3",
        "--compress-pdfs",
    ]
    with log_path.open("a") as handle:
        handle.write(
            "\n"
            f"rerun_task court={task.court_code} "
            f"from={task.from_date} to={task.to_date} "
            f"started_at={datetime.now(timezone.utc).isoformat()}\n"
        )
        handle.flush()
        result = subprocess.run(
            cmd,
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
        handle.write(
            f"rerun_task court={task.court_code} "
            f"from={task.from_date} to={task.to_date} "
            f"finished_at={datetime.now(timezone.utc).isoformat()} "
            f"returncode={result.returncode}\n"
        )
    return result.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-log", required=True, type=Path)
    parser.add_argument("--primary-pid", type=int, default=0)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--logs-dir", type=Path, default=Path("logs"))
    args = parser.parse_args()

    args.logs_dir.mkdir(parents=True, exist_ok=True)

    if args.primary_pid:
        wait_for_pid(args.primary_pid, args.poll_seconds)

    tasks = parse_failed_tasks(args.primary_log)
    for round_no in range(1, args.rounds + 1):
        print(f"round={round_no} failed_tasks={len(tasks)}")
        if not tasks:
            return

        tasks_file = args.logs_dir / f"failed_tasks_round_{round_no}_{utc_stamp()}.tsv"
        tasks_file.write_text(
            "\n".join(
                f"{task.court_code}\t{task.from_date}\t{task.to_date}"
                for task in tasks
            )
            + "\n"
        )
        print(f"round={round_no} tasks_file={tasks_file}", flush=True)

        round_log = args.logs_dir / f"failed_task_rerun_round_{round_no}_{utc_stamp()}.log"
        successes = 0
        for task in tasks:
            if run_task(task, round_log, args.max_workers):
                successes += 1
        print(
            f"round={round_no} successes={successes} "
            f"failures={len(tasks) - successes} round_log={round_log}",
            flush=True,
        )
        tasks = parse_failed_reruns(round_log)

    if tasks:
        print(f"remaining_failed_tasks={len(tasks)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
