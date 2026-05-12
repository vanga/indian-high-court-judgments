#!/usr/bin/env python3

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from src.utils.court_utils import from_s3_format, load_court_bench_mapping
from src.utils.file_utils import extract_decision_date_from_json
from src.utils.s3_utils import (
    create_and_upload_parquet_files,
    get_existing_files_from_s3_v2,
    upload_files_to_s3_v2,
    write_scraped_through_date,
)


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def group_files_by_year(files: List[Path]) -> Dict[int, List[Path]]:
    files_by_year: Dict[int, List[Path]] = {}
    skipped: List[Path] = []
    for file in files:
        json_file = file.with_suffix(".json")
        year = extract_decision_date_from_json(json_file)
        if year is None:
            skipped.append(file)
            continue
        files_by_year.setdefault(year, []).append(file)
    if skipped:
        logger.warning(
            "Skipped %s file(s) with unparseable decision dates in %s",
            len(skipped),
            skipped[0].parent if skipped else "unknown",
        )
    return files_by_year


def sync_bench(court_code_underscore: str, bench: str, end_date: datetime) -> None:
    bench_path = Path("data/court/cnrorders") / bench
    if not bench_path.exists():
        return

    json_files = list(bench_path.glob("**/*.json"))
    pdf_files = list(bench_path.glob("**/*.pdf"))
    json_files_by_year = group_files_by_year(json_files)
    pdf_files_by_year = group_files_by_year(pdf_files)

    bench_files = {}

    for year, files in json_files_by_year.items():
        existing_files = set(
            get_existing_files_from_s3_v2("metadata", year, court_code_underscore, bench)
        )
        local_json_map = {f.name: f for f in files}
        new_basenames = set(local_json_map.keys()) - existing_files
        new_files = {local_json_map[name] for name in new_basenames}
        bench_files.setdefault(
            year, {"parquet_metadata": set(), "metadata": set(), "data": set()}
        )
        bench_files[year]["parquet_metadata"] = set(files)
        bench_files[year]["metadata"] = new_files

    for year, files in pdf_files_by_year.items():
        existing_pdf_files = set(
            get_existing_files_from_s3_v2("data", year, court_code_underscore, bench)
        )
        local_pdf_map = {f.name: f for f in files}
        new_pdf_basenames = set(local_pdf_map.keys()) - existing_pdf_files
        new_pdf_files = {local_pdf_map[name] for name in new_pdf_basenames}
        bench_files.setdefault(
            year, {"parquet_metadata": set(), "metadata": set(), "data": set()}
        )
        bench_files[year]["data"] = new_pdf_files

    synced_years = []
    failed_years = set()

    for year, year_files in sorted(bench_files.items()):
        try:
            logger.info(
                "Syncing bench=%s court=%s year=%s metadata=%s data=%s parquet_seed=%s",
                bench,
                court_code_underscore,
                year,
                len(year_files["metadata"]),
                len(year_files["data"]),
                len(year_files["parquet_metadata"]),
            )
            success = create_and_upload_parquet_files(
                year,
                court_code_underscore,
                bench,
                {
                    "metadata": year_files["parquet_metadata"],
                    "data": year_files["data"],
                },
            )
            if not success:
                raise RuntimeError("parquet update failed before raw upload")

            if year_files["metadata"]:
                upload_files_to_s3_v2(
                    "metadata",
                    year,
                    court_code_underscore,
                    bench,
                    year_files["metadata"],
                )

            if year_files["data"]:
                upload_files_to_s3_v2(
                    "data",
                    year,
                    court_code_underscore,
                    bench,
                    year_files["data"],
                )

            synced_years.append(year)
        except Exception:
            logger.exception(
                "Failed to sync bench=%s court=%s year=%s",
                bench,
                court_code_underscore,
                year,
            )
            failed_years.add(year)

    if not failed_years:
        scraped_through_str = end_date.strftime("%Y-%m-%d")
        years_to_update = set(bench_files.keys()) | {end_date.year}
        for year in years_to_update:
            write_scraped_through_date(
                "data", year, court_code_underscore, bench, scraped_through_str
            )

    uploaded_files = []
    for year in synced_years:
        year_files = bench_files[year]
        uploaded_files.extend(year_files.get("parquet_metadata", []))
        uploaded_files.extend(year_files.get("data", []))
    for file in uploaded_files:
        try:
            Path(file).unlink(missing_ok=True)
        except OSError:
            pass

    remaining_files = [p for p in bench_path.rglob("*") if p.is_file()]
    if bench_path.exists() and not remaining_files:
        shutil.rmtree(bench_path)
    elif remaining_files:
        logger.warning(
            "Preserved %s file(s) in %s after sync",
            len(remaining_files),
            bench_path,
        )

    if failed_years:
        raise RuntimeError(
            f"bench={bench} court={court_code_underscore} failed years={sorted(failed_years)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", required=True, help="Scraped-through date (YYYY-MM-DD)")
    parser.add_argument(
        "--court-codes",
        nargs="*",
        default=None,
        help="Optional court codes in tilde format, for example 33~10 27~1",
    )
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    target_courts = set(args.court_codes or [])
    bench_to_court = load_court_bench_mapping()

    failures = []
    for bench_dir in sorted(Path("data/court/cnrorders").iterdir()):
        if not bench_dir.is_dir():
            continue
        bench = bench_dir.name
        court_code_underscore = bench_to_court.get(bench)
        if not court_code_underscore:
            logger.warning("Skipping unmapped bench %s", bench)
            continue
        court_code_tilde = from_s3_format(court_code_underscore)
        if target_courts and court_code_tilde not in target_courts:
            continue
        try:
            sync_bench(court_code_underscore, bench, end_date)
        except Exception as exc:
            failures.append(str(exc))

    if failures:
        raise SystemExit("Sync completed with failures:\n- " + "\n- ".join(failures))


if __name__ == "__main__":
    main()
