"""File utility functions for grouping, cleaning up, and extracting metadata from files."""

import json
from pathlib import Path
from typing import Dict, List, Optional

from html_utils import parse_decision_date_from_html


def extract_decision_date_from_json(json_file_path: Path) -> Optional[int]:
    """Extract decision date year from judgment metadata JSON file"""
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_html = data.get("raw_html", "")
        if not raw_html:
            return None

        date_str, year = parse_decision_date_from_html(raw_html)

        if year is not None:
            return year

        if date_str:
            print(
                f"Warning: Could not parse decision date '{date_str}' from {json_file_path}"
            )

        return None
    except Exception as e:
        print(f"Warning: Error extracting decision date from {json_file_path}: {e}")
        return None


def extract_bench_from_path(file_path: Path) -> Optional[str]:
    """Extract bench name from file path like data/court/cnrorders/sikkimhc_pg/orders/..."""
    parts = Path(file_path).parts

    if "cnrorders" in parts:
        idx = parts.index("cnrorders")
        if idx + 1 < len(parts):
            return parts[idx + 1]

    return None


def group_files_by_year_and_bench(
    downloaded_files: Dict[str, List[Path]],
) -> Dict[int, Dict[str, Dict[str, List[Path]]]]:
    """Group downloaded files by year (from decision date) and bench

    Returns: dict[year][bench] = {"metadata": [], "data": []}
    """
    json_to_year: Dict[Path, int] = {}
    pdf_to_year: Dict[Path, int] = {}

    files_by_year_bench = {}

    for json_file in downloaded_files["metadata"]:
        year = extract_decision_date_from_json(json_file)

        if year is None:
            print(f"Error: Could not extract decision date from {json_file}")
            print("Skipping this file - it will not be uploaded")
            continue

        bench = extract_bench_from_path(json_file)
        if not bench:
            print(f"Warning: Could not extract bench from {json_file}")
            continue

        json_to_year[json_file] = year
        pdf_path = Path(json_file).with_suffix(".pdf")
        pdf_to_year[pdf_path] = year

        if year not in files_by_year_bench:
            files_by_year_bench[year] = {}
        if bench not in files_by_year_bench[year]:
            files_by_year_bench[year][bench] = {"metadata": [], "data": []}

        files_by_year_bench[year][bench]["metadata"].append(json_file)

    for pdf_file in downloaded_files["data"]:
        bench = extract_bench_from_path(pdf_file)

        if not bench:
            print(f"Warning: Could not extract bench from {pdf_file}")
            continue

        year = None

        if pdf_file in pdf_to_year:
            year = pdf_to_year[pdf_file]

        if year is None:
            json_file = str(Path(pdf_file).with_suffix(".json"))
            if Path(json_file).exists():
                year = extract_decision_date_from_json(json_file)

        if year is None:
            print(f"Error: Could not extract decision date for {pdf_file}")
            print(f"  Corresponding JSON: {json_file}")
            print("  Skipping this file - it will not be uploaded")
            continue

        if year not in files_by_year_bench:
            files_by_year_bench[year] = {}
        if bench not in files_by_year_bench[year]:
            files_by_year_bench[year][bench] = {"metadata": [], "data": []}

        files_by_year_bench[year][bench]["data"].append(pdf_file)

    return files_by_year_bench


def cleanup_uploaded_files(files: Dict[str, List[Path]]) -> tuple[int, int]:
    """Delete local files after successful upload to S3 and remove empty directories"""
    deleted_count = 0
    failed_count = 0
    directories_to_check = set()

    all_files = files.get("metadata", []) + files.get("data", [])

    for file_path in all_files:
        try:
            if file_path.exists():
                directories_to_check.add(file_path.parent)
                file_path.unlink()
                deleted_count += 1
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")
            failed_count += 1

    for directory in sorted(directories_to_check, reverse=True):
        try:
            if directory.exists() and not any(directory.iterdir()):
                directory.rmdir()
                print(f"  Removed empty directory: {directory}")
        except Exception:
            pass

    return deleted_count, failed_count

