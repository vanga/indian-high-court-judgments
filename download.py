import argparse
import concurrent.futures
import json
import logging
import re
import threading
import traceback
import urllib.parse
import uuid
import warnings
from datetime import datetime, timedelta
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Optional, Generator, List, Dict

import lxml.html as LH
import requests
import urllib3
from bs4 import BeautifulSoup
from tqdm import tqdm
from src.captcha_solver.main import get_text

from court_utils import (
    to_s3_format,
    from_s3_format,
    get_json_file,
    get_court_codes,
    get_tracking_data,
    save_court_tracking_date,
    get_bench_codes,
    load_court_bench_mapping,
)
from s3_utils import (
    S3_READ_BUCKET,
    S3_WRITE_BUCKET,
    S3_AVAILABLE,
    get_court_dates_from_index_files,
    get_existing_files_from_s3,
    update_index_files_after_download,
    upload_files_to_s3,
)


S3_ENABLED = False
from file_utils import (
    extract_decision_date_from_json,
    extract_bench_from_path,
    group_files_by_year_and_bench,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

root_url = "https://judgments.ecourts.gov.in"
output_dir = Path("./data")
START_DATE = "2008-01-01"


payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=27~1&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&int_fin_party_val=undefined&int_fin_case_val=undefined&int_fin_court_val=undefined&int_fin_decision_val=undefined&act=&sel_search_by=undefined&sections=undefined&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=2&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ajax_req=true&app_token=1fbc7fbb840eb95975c684565909fe6b3b82b8119472020ff10f40c0b1c901fe"


pdf_link_payload = "val=0&lang_flg=undefined&path=cnrorders/taphc/orders/2017/HBHC010262202017_1_2047-06-29.pdf#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined&ajax_req=true&app_token=c64944b84c687f501f9692e239e2a0ab007eabab497697f359a2f62e4fcd3d10"

page_size = 1000
MATH_CAPTCHA = False
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)

S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
OUTPUT_DIR = output_dir

_existing_files_cache = {}


def get_new_date_range(
    last_date: str, day_step: int = 1
) -> tuple[str | None, str | None]:
    last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")
    new_from_date_dt = last_date_dt + timedelta(days=1)
    new_to_date_dt = new_from_date_dt + timedelta(days=day_step - 1)
    if new_from_date_dt.date() > datetime.now().date():
        return None, None

    if new_to_date_dt.date() > datetime.now().date():
        new_to_date_dt = datetime.now()
    new_from_date = new_from_date_dt.strftime("%Y-%m-%d")
    new_to_date = new_to_date_dt.strftime("%Y-%m-%d")
    return new_from_date, new_to_date


def get_date_ranges_to_process(court_code, start_date=None, end_date=None, day_step=1):
    """
    Generate date ranges to process for a given court.
    If start_date is provided but no end_date, use current date as end_date.
    If neither is provided, use tracking data to determine the next date range.
    """
    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date and end_date:
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        current_date = start_date_dt
        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)
    else:
        tracking_data = get_tracking_data()
        court_tracking = tracking_data.get(court_code, {})
        last_date = court_tracking.get("last_date", START_DATE)

        current_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        end_date_dt = datetime.now()

        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)


class CourtDateTask:
    def __init__(self, court_code, from_date, to_date):
        self.id = str(uuid.uuid4())
        self.court_code = court_code
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self):
        return f"CourtDateTask(id={self.id}, court_code={self.court_code}, from_date={self.from_date}, to_date={self.to_date})"


def generate_tasks(
    court_codes: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    day_step: int = 1,
) -> Generator[CourtDateTask, None, None]:
    """Generate tasks for processing courts and date ranges as a generator"""
    all_court_codes = get_court_codes()
    if not court_codes:
        court_codes = all_court_codes
    else:
        normalized_codes = []
        for court_code in court_codes:
            normalized_code = from_s3_format(court_code)
            if normalized_code in all_court_codes:
                normalized_codes.append(normalized_code)
            else:
                raise ValueError(
                    f"Court code {court_code} (normalized to {normalized_code}) not found in court-codes.json"
                )
        court_codes = normalized_codes

    for code in court_codes:
        for from_date, to_date in get_date_ranges_to_process(
            code, start_date, end_date, day_step
        ):
            yield CourtDateTask(code, from_date, to_date)


def process_task(task):
    """Process a single court-date task"""
    try:
        global _existing_files_cache
        downloader = Downloader(
            task,
            existing_files_cache=_existing_files_cache
            if _existing_files_cache
            else None,
        )
        downloader.download()
    except Exception as e:
        court_codes = get_court_codes()
        logger.error(
            f"Error processing court {task.court_code} {court_codes.get(task.court_code, 'Unknown')}: {e}"
        )
        traceback.print_exc()


def run(court_codes=None, start_date=None, end_date=None, day_step=1, max_workers=2):
    """
    Run the downloader with intelligent date detection and S3 integration.

    Behavior:
    - If no dates provided: Read dates from S3 index files and download from next day
    - If dates provided: Download for specified range, skipping already-downloaded files
    - Always checks S3 for existing files to avoid re-downloading
    - Appends to existing tar files in S3 after download
    """
    global _existing_files_cache

    if isinstance(court_codes, str):
        court_codes = [court_codes]

    if start_date is None and end_date is None:
        print("No dates provided, fetching latest dates from S3 index files...")
        if not S3_AVAILABLE:
            print("ERROR: S3 not available, cannot fetch dates")
            return

        s3_court_dates = get_court_dates_from_index_files()

        if not s3_court_dates:
            print("No dates found in S3, please provide start_date manually")
            return

        if court_codes:
            filtered_dates = {}
            for cc in court_codes:
                s3_format = to_s3_format(cc)
                if s3_format in s3_court_dates:
                    filtered_dates[s3_format] = s3_court_dates[s3_format]
            s3_court_dates = filtered_dates

        if not s3_court_dates:
            print("No matching courts found in S3")
            return

        print(f"Found {len(s3_court_dates)} courts to process")
        for s3_court_code, benches in s3_court_dates.items():
            court_code = from_s3_format(s3_court_code)

            earliest_date = None
            for bench_name, updated_at_str in benches.items():
                try:
                    bench_date = datetime.fromisoformat(
                        updated_at_str.replace("Z", "+00:00")
                    ).date()
                    if earliest_date is None or bench_date < earliest_date:
                        earliest_date = bench_date
                except Exception as e:
                    print(f"Warning: Invalid date for {bench_name}: {e}")
                    continue

            if not earliest_date:
                print(f"Skipping {court_code}: no valid dates found")
                continue

            start_from = earliest_date + timedelta(days=1)
            today = datetime.now().date()

            if start_from > today:
                print(f"Skipping {court_code}: already up to date")
                continue

            print(f"\nProcessing court {court_code}: {start_from} to {today}")

            _existing_files_cache = get_existing_files_from_s3(s3_court_code, benches)

            tasks = list(
                generate_tasks(
                    [court_code],
                    start_from.strftime("%Y-%m-%d"),
                    today.strftime("%Y-%m-%d"),
                    day_step,
                )
            )
            _run_tasks_and_upload_to_s3(tasks, court_code, max_workers, today)

            _existing_files_cache = {}

    else:
        print(
            f"Downloading for specified date range: {start_date} to {end_date or start_date}"
        )

        # Determine which courts to process
        if court_codes is None:
            # Process all courts
            target_courts = list(get_court_codes().keys())
        else:
            # Process specified courts
            target_courts = court_codes

        print(f"Processing {len(target_courts)} court(s): {', '.join(target_courts)}")

        # Process each court individually for proper S3 handling
        for court_code in target_courts:
            print(f"\nProcessing court {court_code}...")

            # Load S3 cache for this court if S3 is enabled
            if S3_ENABLED:
                year_to_check = None
                if start_date:
                    try:
                        year_to_check = datetime.strptime(start_date, "%Y-%m-%d").year
                    except Exception:
                        pass

                s3_court_dates = get_court_dates_from_index_files(year=year_to_check)
                s3_format = to_s3_format(court_code)
                if s3_format in s3_court_dates:
                    print(f"Loading existing files cache for {court_code} from S3...")
                    _existing_files_cache = get_existing_files_from_s3(
                        s3_format, s3_court_dates[s3_format], year=year_to_check
                    )

            # Generate tasks for this specific court
            tasks = list(generate_tasks([court_code], start_date, end_date, day_step))

            if not tasks:
                print(f"No tasks to process for court {court_code}")
                continue

            print(f"Generated {len(tasks)} tasks for court {court_code}")

            # Run tasks with S3 upload if enabled
            if S3_ENABLED:
                end_date_obj = (
                    datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
                )
                _run_tasks_and_upload_to_s3(
                    tasks, court_code, max_workers, end_date_obj
                )
            else:
                _run_tasks(tasks, max_workers)

            # Clear cache after each court
            _existing_files_cache = {}

    logger.info("All tasks completed")


def _run_tasks(tasks, max_workers):
    """Run download tasks without S3 upload"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(tasks), desc="Processing tasks", unit="task") as pbar:
            for i, result in enumerate(executor.map(process_task, tasks)):
                task = tasks[i]
                pbar.set_description(
                    f"Processing {task.court_code} ({task.from_date} to {task.to_date})"
                )
                pbar.update(1)


def _run_tasks_and_upload_to_s3(tasks, court_code, max_workers, end_date=None):
    """
    Run download tasks and upload results to S3

    Args:
        tasks: List of CourtDateTask objects
        court_code: Court code being processed
        max_workers: Number of parallel workers
        end_date: End date of the download range (for index timestamp)
    """
    if not S3_ENABLED:
        print("INFO: S3 disabled, will only download locally")
        _run_tasks(tasks, max_workers)
        return

    _run_tasks(tasks, max_workers)

    if end_date is None and tasks:
        end_date = max(task.to_date for task in tasks)

    print(f"\nCollecting NEW files for {court_code}...")
    downloaded_files = {"metadata": [], "data": []}

    bench_to_court = load_court_bench_mapping()
    bench_codes = get_bench_codes()

    for bench, cc in bench_codes.items():
        if bench not in bench_to_court:
            bench_to_court[bench] = cc

    court_code_underscore = to_s3_format(court_code)

    target_benches = [
        bench for bench, cc in bench_to_court.items() if cc == court_code_underscore
    ]

    if not target_benches:
        print(f"Warning: No benches found for court code {court_code}")
        return

    print(
        f"Found {len(target_benches)} benches for court {court_code}: {', '.join(target_benches)}"
    )

    print(f"Scanning local files to identify year partitions...")
    years_in_local_files = set()

    for bench in target_benches:
        bench_path = Path(f"data/court/cnrorders/{bench}")
        if not bench_path.exists():
            continue

        json_files = list(bench_path.glob("**/*.json"))
        sample_size = min(100, len(json_files))

        for json_file in json_files[:sample_size]:
            year = extract_decision_date_from_json(str(json_file))
            if year:
                years_in_local_files.add(year)

    if end_date:
        years_in_local_files.add(end_date.year)

    if not years_in_local_files:
        print("Warning: Could not extract any years from decision dates in local files")
        print("This might indicate an issue with the JSON files or HTML parsing")
        return

    print(
        f"Found files spanning {len(years_in_local_files)} year(s): {sorted(years_in_local_files)}"
    )

    print(
        f"Loading S3 index files for {len(years_in_local_files)} year partition(s)..."
    )

    global _existing_files_cache
    if not _existing_files_cache:
        _existing_files_cache = {}
        for year in sorted(years_in_local_files):
            print(f"  Loading S3 index for year {year}...")
            year_cache = get_existing_files_from_s3(
                court_code_underscore, {b: None for b in target_benches}, year
            )
            for bench, files in year_cache.items():
                if bench not in _existing_files_cache:
                    _existing_files_cache[bench] = {"metadata": set(), "data": set()}
                _existing_files_cache[bench]["metadata"].update(
                    files.get("metadata", set())
                )
                _existing_files_cache[bench]["data"].update(files.get("data", set()))

    total_on_disk = 0
    total_new = 0

    try:
        for bench in target_benches:
            bench_path = Path(f"data/court/cnrorders/{bench}")
            if not bench_path.exists():
                continue

            json_files = list(bench_path.glob("**/*.json"))
            pdf_files = list(bench_path.glob("**/*.pdf"))

            total_on_disk += len(json_files) + len(pdf_files)

            bench_cache = _existing_files_cache.get(
                bench, {"metadata": set(), "data": set()}
            )

            for json_file in json_files:
                filename = json_file.name
                base_name = filename.replace(".json", "")

                if base_name not in bench_cache.get("metadata", set()):
                    downloaded_files["metadata"].append(json_file)
                    total_new += 1

            for pdf_file in pdf_files:
                filename = pdf_file.name

                if filename not in bench_cache.get("data", set()):
                    downloaded_files["data"].append(pdf_file)
                    total_new += 1

        print(f"Found {total_on_disk} total files on disk")
        print(
            f"Filtered to {total_new} NEW files not yet in S3: {len(downloaded_files['metadata'])} metadata, {len(downloaded_files['data'])} PDF"
        )

        if downloaded_files["metadata"] or downloaded_files["data"]:
            print(f"\nUploading files to S3 for {court_code}...")
            bench_upload_status = upload_files_to_s3(court_code, downloaded_files)

            s3_court_code = to_s3_format(court_code)

            files_by_year_bench = group_files_by_year_and_bench(downloaded_files)

            for year_bench_key, success in bench_upload_status.items():
                if success:
                    year_str, bench = year_bench_key.split("/", 1)
                    year = int(year_str)

                    if (
                        year in files_by_year_bench
                        and bench in files_by_year_bench[year]
                    ):
                        bench_files = files_by_year_bench[year][bench]

                        if bench_files["metadata"] or bench_files["data"]:
                            update_index_files_after_download(
                                s3_court_code,
                                bench,
                                bench_files,
                                to_date=datetime(year, 12, 31),
                            )
                            print(f"Updated index files for {year}/{bench}")

    except Exception as e:
        print(f"Error during S3 upload: {e}")
        traceback.print_exc()


def run_incremental_download(court_code_dl, start_date, end_date=None, max_workers=4):
    """Run download for specific court and date range with multiprocessing support"""
    # If no end_date provided, make it same as start_date
    if end_date is None:
        end_date = start_date

    print(f"Downloading {court_code_dl}: {start_date} → {end_date}")

    try:
        downloaded_files = {"metadata": [], "data": []}

        print(f"Starting multi-threaded download with {max_workers} workers...")

        run(
            court_codes=[court_code_dl],
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            day_step=1,
            max_workers=max_workers,
        )

        print(f"Multi-threaded download completed: {court_code_dl}")

        court_code_underscore = to_s3_format(court_code_dl)

        print(
            f"Debug: court_code_dl = {court_code_dl}, court_code_underscore = {court_code_underscore}"
        )

        # Load bench codes to find the bench name
        try:
            with open("bench-codes.json", "r") as f:
                bench_codes = json.load(f)

            # Find ALL bench names for this court code (not just the first one)
            target_benches = []
            for bench_name, court_code in bench_codes.items():
                if court_code == court_code_underscore:
                    target_benches.append(bench_name)

            if target_benches:
                print(
                    f"Targeting {len(target_benches)} bench directories for court {court_code_underscore}: {target_benches}"
                )
                # Get files from ALL bench directories for this court
                for target_bench in target_benches:
                    bench_path = Path(f"./data/court/cnrorders/{target_bench}")
                    if bench_path.exists():
                        json_files = list(bench_path.glob("**/*.json"))
                        pdf_files = list(bench_path.glob("**/*.pdf"))

                        # Add all files from this bench
                        for json_file in json_files:
                            downloaded_files["metadata"].append(str(json_file))

                        for pdf_file in pdf_files:
                            downloaded_files["data"].append(str(pdf_file))

                        print(
                            f"Found {len(json_files)} JSON and {len(pdf_files)} PDF files in {target_bench}"
                        )
                    else:
                        print(f"Warning: Bench directory {bench_path} does not exist")
            else:
                print(
                    f"Warning: Could not find any bench for court code {court_code_underscore}"
                )
                print(
                    f"Available court codes in bench-codes.json: {list(bench_codes.values())}"
                )

        except Exception as e:
            print(f"Warning: Could not load bench codes: {e}")
            # Fallback to scanning all directories (old behavior)
            print("Falling back to scanning all bench directories...")
            court_output_dir = Path(f"./data/court/cnrorders")
            if court_output_dir.exists():
                for bench_dir in court_output_dir.iterdir():
                    if bench_dir.is_dir():
                        json_files = list(bench_dir.glob("**/*.json"))
                        pdf_files = list(bench_dir.glob("**/*.pdf"))

                        for json_file in json_files:
                            downloaded_files["metadata"].append(str(json_file))

                        for pdf_file in pdf_files:
                            downloaded_files["data"].append(str(pdf_file))

        print(
            f"Collected {len(downloaded_files['metadata'])} metadata files and {len(downloaded_files['data'])} PDF files"
        )
        return downloaded_files

    except Exception as e:
        print(f"Download failed {court_code_dl}: {e}")
        traceback.print_exc()
        return {"metadata": [], "data": []}


def sync_to_s3(max_workers=4, court_codes=None):
    """Sync new data to S3 for all courts or specific court_codes"""
    if not S3_ENABLED:
        print("ERROR: S3 disabled by S3_ENABLED flag")
        return

    if court_codes:
        print(f"Starting S3 sync for courts: {court_codes}")
    else:
        print("Starting S3 sync for all courts")

    # Get current dates from S3 index files
    court_dates = get_court_dates_from_index_files()
    print(court_dates)
    if not court_dates:
        print("No existing data found in S3, exiting")
        return

    # Filter by court_codes if provided
    if court_codes:
        # Convert court_codes from format like '33_10' to match keys in court_dates
        filtered_court_dates = {}
        for cc in court_codes:
            s3_format = cc.replace("~", "_")
            if s3_format in court_dates:
                filtered_court_dates[s3_format] = court_dates[s3_format]
            else:
                print(f"Warning: Court code {cc} not found in S3 data")
        court_dates = filtered_court_dates

    if not court_dates:
        print("No courts to process after filtering")
        return

    # Count total benches across all courts
    total_benches = sum(len(benches) for benches in court_dates.values())
    print(
        f"Found {len(court_dates)} courts with {total_benches} total benches to process"
    )

    for s3_court_code, benches in court_dates.items():
        court_code = s3_court_code.replace("_", "~")
        print(f"Processing court {court_code} with {len(benches)} benches")

        # **NEW: Fetch existing files from S3 to avoid re-downloading**
        print(f"  Fetching existing files from S3 for optimization...")
        global _existing_files_cache
        _existing_files_cache = get_existing_files_from_s3(s3_court_code, benches)

        # Step 1: Find the earliest date among all benches for this court
        earliest_date = None
        bench_dates = {}  # Store each bench's latest date for later use

        for bench_name, updated_at_str in benches.items():
            try:
                bench_latest_date = datetime.fromisoformat(
                    updated_at_str.replace("Z", "+00:00")
                ).date()
                bench_dates[bench_name] = bench_latest_date
                print(f"  Bench {bench_name}: latest date = {bench_latest_date}")

                if earliest_date is None or bench_latest_date < earliest_date:
                    earliest_date = bench_latest_date
            except Exception as e:
                print(
                    f"  Warning: Invalid date {updated_at_str} for bench {bench_name}: {e}"
                )
                continue

        if not bench_dates:
            print(f"  No valid bench dates found for court {court_code}, skipping")
            continue

        print(f"  Earliest date across all benches: {earliest_date}")

        # Step 2: Download for entire court from earliest date to today (ONCE)
        # Files already in S3 will be skipped automatically via existing_files_cache
        today = datetime.now().date()
        start_date = earliest_date + timedelta(days=1)

        if start_date > today:
            print(
                f"  No data to download for court {court_code} (start_date {start_date} > today {today})"
            )
            continue

        print(f"  Downloading court {court_code}: {start_date} to {today}")
        print(f"  (Files already in S3 will be automatically skipped)")
        all_downloaded_files = run_incremental_download(
            court_code, start_date, today, max_workers
        )

        if not all_downloaded_files or (
            not all_downloaded_files["metadata"] and not all_downloaded_files["data"]
        ):
            print(f"  No files downloaded for court {court_code}")
            continue

        print(
            f"  Downloaded {len(all_downloaded_files['metadata'])} metadata and {len(all_downloaded_files['data'])} data files"
        )

        # Step 3: Split downloaded files by bench
        bench_files = {}
        for bench_name in bench_dates.keys():
            bench_files[bench_name] = {"metadata": [], "data": []}

        # Distribute metadata files to benches
        for metadata_file in all_downloaded_files.get("metadata", []):
            file_bench = extract_bench_from_path(metadata_file)
            if file_bench and file_bench in bench_files:
                bench_files[file_bench]["metadata"].append(metadata_file)

        # Distribute data files to benches
        for data_file in all_downloaded_files.get("data", []):
            file_bench = extract_bench_from_path(data_file)
            if file_bench and file_bench in bench_files:
                bench_files[file_bench]["data"].append(data_file)

        # Show what we found for each bench
        for bench_name, files in bench_files.items():
            print(
                f"  Found {len(files['metadata'])} metadata and {len(files['data'])} data files for bench {bench_name}"
            )

        # Step 4: Upload and update index for each bench that has files
        for bench_name, files in bench_files.items():
            if not files["metadata"] and not files["data"]:
                print(f"  No new files for bench {bench_name}, skipping")
                continue

            print(f"  Uploading files for bench {bench_name}...")

            # Upload files to S3 - returns dict of bench -> success status
            bench_upload_status = upload_files_to_s3(court_code, files)

            # Update index files for successfully uploaded benches
            for uploaded_bench, upload_success in bench_upload_status.items():
                if upload_success:
                    print(
                        f"    S3 upload successful, updating index files for bench {uploaded_bench}"
                    )

                    # Use today as the end_date for index update (we downloaded up to today)
                    update_index_files_after_download(
                        s3_court_code, uploaded_bench, files, today
                    )

                    print(f"    Index files updated for bench {uploaded_bench}")
                else:
                    print(
                        f"    S3 upload failed for bench {uploaded_bench}, NOT updating index files"
                    )

        # Clear cache for this court
        _existing_files_cache = {}

        print(f"Completed court {court_code}")

    print("S3 sync completed for all courts")


def download_bench_data(court_code, bench_name, latest_date, max_workers=4):
    """Download data for a specific bench of a court"""
    today = datetime.now().date()

    # Calculate the next date to download (latest_date + 1)
    start_date = latest_date + timedelta(days=1)

    # For full sync, download up to today
    end_date = today

    # Check if we need to download anything
    if start_date > end_date:
        print(
            f"    Court {court_code}, bench {bench_name}: No data to download (start_date {start_date} > end_date {end_date})"
        )
        return {"metadata": [], "data": []}

    # Convert court code for download (replace _ with ~)
    court_code_dl = court_code.replace("_", "~")

    print(
        f"    Downloading: Court {court_code}, bench {bench_name}: {start_date} to {end_date}"
    )

    # Run download with max_workers support (this downloads for entire court)
    all_downloaded_files = run_incremental_download(
        court_code_dl, start_date, end_date, max_workers
    )

    # Filter files to only include those from the specific bench we're processing
    bench_filtered_files = {"metadata": [], "data": []}

    # Filter metadata files
    for metadata_file in all_downloaded_files.get("metadata", []):
        file_bench = extract_bench_from_path(metadata_file)
        if file_bench == bench_name:
            bench_filtered_files["metadata"].append(metadata_file)

    # Filter data files
    for data_file in all_downloaded_files.get("data", []):
        file_bench = extract_bench_from_path(data_file)
        if file_bench == bench_name:
            bench_filtered_files["data"].append(data_file)

    print(
        f"    Filtered to {len(bench_filtered_files['metadata'])} metadata and {len(bench_filtered_files['data'])} data files for bench {bench_name}"
    )

    return bench_filtered_files


class Downloader:
    def __init__(self, task: CourtDateTask, existing_files_cache=None):
        self.task = task
        self.root_url = "https://judgments.ecourts.gov.in"
        self.search_url = f"{self.root_url}/pdfsearch/?p=pdf_search/home/"
        self.captcha_url = f"{self.root_url}/pdfsearch/vendor/securimage/securimage_show.php"  # not lint skip/
        self.captcha_token_url = f"{self.root_url}/pdfsearch/?p=pdf_search/checkCaptcha"
        self.pdf_link_url = f"{self.root_url}/pdfsearch/?p=pdf_search/openpdfcaptcha"
        self.pdf_link_url_wo_captcha = f"{root_url}/pdfsearch/?p=pdf_search/openpdf"

        self.court_code = task.court_code
        self.tracking_data = get_tracking_data()
        self.court_codes = get_court_codes()
        self.court_name = self.court_codes[self.court_code]
        self.court_tracking = self.tracking_data.get(self.court_code, {})
        self.session_cookie_name = "JUDGEMENTSSEARCH_SESSID"
        self.ecourts_token_cookie_name = "JSESSION"
        self.session_id = None
        self.ecourts_token = None
        self.app_token = "490a7e9b99e4553980213a8b86b3235abc51612b038dbdb1f9aa706b633bbd6c"  # not lint skip/

        # NEW: Cache of existing files in S3 to avoid re-downloading
        self.existing_files_cache = existing_files_cache or {}

    def _results_exist_in_search_response(self, res_dict):
        results_exist = (
            "reportrow" in res_dict
            and "aaData" in res_dict["reportrow"]
            and len(res_dict["reportrow"]["aaData"]) > 0
        )
        if results_exist:
            no_of_results = len(res_dict["reportrow"]["aaData"])
            logger.info(f"Found {no_of_results} results for task: {self.task}")
        return results_exist

    def _prepare_next_iteration(self, search_payload):
        search_payload["sEcho"] += 1
        search_payload["iDisplayStart"] += page_size
        logger.info(
            f"Next iteration: {search_payload['iDisplayStart']}, task: {self.task.id}"
        )
        return search_payload

    def process_court(self):
        last_date = self.court_tracking.get("last_date", START_DATE)
        from_date, to_date = get_new_date_range(last_date)
        if from_date is None:
            logger.info(f"No more data to download for: task: {self.task.id}")
            return
        search_payload = self.default_search_payload()
        search_payload["from_date"] = from_date
        search_payload["to_date"] = to_date
        self.init_user_session()
        search_payload["state_code"] = self.court_code
        search_payload["app_token"] = self.app_token
        results_available = True
        pdfs_downloaded = 0

        while results_available:
            try:
                response = self.request_api("POST", self.search_url, search_payload)
                res_dict = response.json()
                if self._results_exist_in_search_response(res_dict):
                    for idx, row in enumerate(res_dict["reportrow"]["aaData"]):
                        try:
                            is_pdf_downloaded = self.process_result_row(
                                row, row_pos=idx
                            )
                            if is_pdf_downloaded:
                                pdfs_downloaded += 1
                            else:
                                self.court_tracking["failed_dates"] = (
                                    self.court_tracking.get("failed_dates", [])
                                )
                                if from_date not in self.court_tracking["failed_dates"]:
                                    self.court_tracking["failed_dates"].append(
                                        from_date
                                    )
                            if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                # after 25 downloads, need to solve captcha for every pdf link request. Starting with a fresh session would be faster so that we get another 25 downloads without captcha
                                logger.info(
                                    f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task.id}"
                                )
                                break

                        except Exception as e:
                            logger.error(
                                f"Error processing row {row}: {e}, task: {self.task}"
                            )
                            traceback.print_exc()
                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        self.init_user_session()
                        search_payload["app_token"] = self.app_token
                        continue
                        # we are skipping the rest of the loop, meaning we fetch the 1000 results again for the same page, with a new session and process. Already downloaded pdfs will be skipped. This continues until we hve downloaded the whole page.
                    # prepare next iteration
                    search_payload = self._prepare_next_iteration(search_payload)
                else:
                    last_date = to_date
                    self.court_tracking["last_date"] = last_date
                    save_court_tracking_date(self.court_code, self.court_tracking)
                    from_date, to_date = get_new_date_range(to_date)
                    if from_date is None:
                        logger.info(f"No more data to download for: task: {self.task}")
                        results_available = False
                    else:
                        search_payload["from_date"] = from_date
                        search_payload["to_date"] = to_date
                        search_payload["sEcho"] = 1
                        search_payload["iDisplayStart"] = 0
                        search_payload["iDisplayLength"] = page_size
                        logger.info(f"Downloading data for task: {self.task}")

            except Exception as e:
                logger.error(f"Error processing task: {self.task}, Error: {e}")
                traceback.print_exc()
                self.court_tracking["failed_dates"] = self.court_tracking.get(
                    "failed_dates", []
                )
                if from_date not in self.court_tracking["failed_dates"]:
                    self.court_tracking["failed_dates"].append(
                        from_date
                    )  # TODO: should be all the dates from from_date to to_date in case step date > 1
                save_court_tracking_date(self.court_code, self.court_tracking)

    def process_result_row(self, row, row_pos):
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")
        # html_element = LH.fromstring(html)
        # why am I using both LH and BS4? idk.
        # title = html_element.xpath("./button/font/text()")[0]
        # description = html_element.xpath("./text()")[0]
        # case_details = html_element.xpath("./strong//text()")
        # check if button with onclick is present
        if not (soup.button and "onclick" in soup.button.attrs):
            logger.info(
                f"No button found, likely multi language judgment, task: {self.task}"
            )
            with open("html-parse-failures.txt", "a") as f:
                f.write(html + "\n")
            # TODO: requires special parsing
            return False
        pdf_fragment = self.extract_pdf_fragment(soup.button["onclick"])

        # **NEW: Check if file already exists in S3 (for sync_s3 optimization)**
        if self.existing_files_cache:
            file_bench = extract_bench_from_path(pdf_fragment)
            filename = Path(pdf_fragment).name

            # Check in cache for this bench
            if file_bench in self.existing_files_cache:
                bench_cache = self.existing_files_cache[file_bench]

                # Extract just the filename without extension for comparison
                base_filename = filename.replace(".pdf", "")

                # Check if this file already exists in S3
                if base_filename in bench_cache.get(
                    "metadata", set()
                ) or filename in bench_cache.get("data", set()):
                    logger.debug(
                        f"Skipping {filename} - already exists in S3 for bench {file_bench}"
                    )
                    return False

        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        is_pdf_present = self.is_pdf_downloaded(pdf_fragment)
        pdf_needs_download = not is_pdf_present
        if pdf_needs_download:
            is_fresh_download = self.download_pdf(pdf_fragment, row_pos)
        else:
            is_fresh_download = False
        metadata_output = pdf_output_path.with_suffix(".json")
        metadata = {
            "court_code": self.court_code,
            "court_name": self.court_name,
            "raw_html": html,
            # "title": title,
            # "description": description,
            # "case_details": case_details,
            "pdf_link": pdf_fragment,
            "downloaded": is_pdf_present or is_fresh_download,
        }
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_output, "w") as f:
            json.dump(metadata, f)
        return is_fresh_download

    def download_pdf(self, pdf_fragment, row_pos):
        # prepare temp pdf request
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        pdf_link_payload = self.default_pdf_link_payload()
        pdf_link_payload["path"] = pdf_fragment
        pdf_link_payload["val"] = row_pos
        pdf_link_payload["app_token"] = self.app_token
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )
        if "outputfile" not in pdf_link_response.json():
            logger.error(
                f"Error downloading pdf, task: {self.task}, Error: {pdf_link_response.json()}"
            )
            return False
        pdf_download_link = pdf_link_response.json()["outputfile"]

        # download pdf and save
        pdf_response = requests.request(
            "GET",
            root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
        )
        pdf_output_path.parent.mkdir(parents=True, exist_ok=True)
        # number of response butes
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0:
            logger.error(
                f"Empty pdf, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        if no_of_bytes == 315:
            logger.error(
                f"404 pdf response, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        with open(pdf_output_path, "wb") as f:
            f.write(pdf_response.content)
        logger.debug(
            f"Downloaded, task: {self.task}, output path: {pdf_output_path}, size: {no_of_bytes}"
        )
        return True

    def update_headers_with_new_session(self, headers):
        cookie = SimpleCookie()
        cookie.load(headers["Cookie"])
        cookie[self.session_cookie_name] = self.session_id
        headers["Cookie"] = cookie.output(header="", sep=";").strip()

    def extract_pdf_fragment(self, html_attribute):
        pattern = r"javascript:open_pdf\('.*?','.*?','(.*?)'\)"
        match = re.search(pattern, html_attribute)
        if match:
            return match.group(1).split("#")[0]
        return None

    def solve_math_expression(self, expression):
        # credits to: https://github.com/NoelShallum
        expression = expression.strip().replace(" ", "").replace(".", "")
        if "+" in expression:
            nums = expression.split("+")
            return str(int(nums[0]) + int(nums[1]))
        elif "-" in expression:
            nums = expression.split("-")
            return str(int(nums[0]) - int(nums[1]))
        elif (
            "*" in expression
            or "X" in expression
            or "x" in expression
            or "×" in expression
        ):
            expression = (
                expression.replace("x", "*").replace("×", "*").replace("X", "*")
            )
            nums = expression.split("*")
            return str(int(nums[0]) * int(nums[1]))
        elif "/" in expression or "÷" in expression:
            expression = expression.replace("÷", "/")
            nums = expression.split("/")
            return str(int(nums[0]) // int(nums[1]))
        else:
            raise ValueError(f"Unsupported mathematical expression: {expression}")

    def is_math_expression(self, expression):
        separators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        for separator in separators:
            if separator in expression:
                return True
        return False

    def solve_captcha(self, retries=0, captcha_url=None):
        logger.debug(f"Solving captcha, retries: {retries}, task: {self.task.id}")
        if retries > 10:
            raise ValueError("Failed to solve captcha")
        if captcha_url is None:
            captcha_url = self.captcha_url
        # download captcha image and save
        captcha_response = requests.get(
            captcha_url, headers={"Cookie": self.get_cookie()}, verify=False, timeout=30
        )
        # Generate a unique filename using UUID
        unique_id = uuid.uuid4().hex[:8]
        captcha_filename = Path(
            f"{captcha_tmp_dir}/captcha_{self.court_code}_{unique_id}.png"
        )
        with open(captcha_filename, "wb") as f:
            f.write(captcha_response.content)

        captcha_text = get_text(str(captcha_filename))

        captcha_text = captcha_text.strip()

        if MATH_CAPTCHA:
            if self.is_math_expression(captcha_text):
                try:
                    answer = self.solve_math_expression(captcha_text)
                    captcha_filename.unlink()
                    return answer
                except Exception as e:
                    logger.error(
                        f"Error solving math expression, task: {self.task.id}, retries: {retries}, captcha text: {captcha_text}, Error: {e}"
                    )
                    # move the captcha image to a new folder for debugging
                    new_filename = f"{uuid.uuid4().hex[:8]}_{captcha_filename.name}"
                    captcha_filename.rename(
                        Path(f"{captcha_failures_dir}/{new_filename}")
                    )
                    return self.solve_captcha(retries + 1, captcha_url)
            else:
                # If not a math expression, try again
                captcha_filename.unlink()  # Clean up the file
                return self.solve_captcha(retries + 1, captcha_url)
        else:
            captcha_text = captcha_text.strip()
            if len(captcha_text) != 6:
                if retries > 10:
                    raise Exception("Captcha not solved")
                return self.solve_captcha(retries + 1)
            return captcha_text

    def solve_pdf_download_captcha(self, response, pdf_link_payload, retries=0):
        html_str = response["filename"]
        html = LH.fromstring(html_str)
        img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
        img_src = root_url + img_src
        # download captch image and save
        captcha_text = self.solve_captcha(captcha_url=img_src)
        pdf_link_payload["captcha1"] = captcha_text
        pdf_link_payload["app_token"] = response["app_token"]
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url_wo_captcha, pdf_link_payload
        )
        res_json = pdf_link_response.json()
        if "message" in res_json and res_json["message"] == "Captcha not solved":
            logger.warning(
                f"Captcha not solved, task: {self.task.id}, retries: {retries}, Error: {pdf_link_response.json()}"
            )
            if retries == 2:
                return res_json
            logger.info(f"Retrying pdf captch solve, task: {self.task.id}")
            return self.solve_pdf_download_captcha(
                response, pdf_link_payload, retries + 1
            )
        return pdf_link_response

    def refresh_token(self, with_app_token=False):
        logger.debug(f"Current session id {self.session_id}, token {self.app_token}")
        answer = self.solve_captcha()
        captcha_check_payload = {
            "captcha": answer,
            "search_opt": "PHRASE",
            "ajax_req": "true",
        }
        if with_app_token:
            captcha_check_payload["app_token"] = self.app_token
        res = requests.request(
            "POST",
            self.captcha_token_url,
            headers=self.get_headers(),
            data=captcha_check_payload,
            verify=False,
            timeout=30,
        )
        res_json = res.json()
        self.app_token = res_json["app_token"]
        self.update_session_id(res)
        logger.debug("Refreshed token")

    def request_api(self, method, url, payload, **kwargs):
        headers = self.get_headers()
        logger.debug(
            f"api_request {self.session_id} {payload.get('app_token') if payload else None} {url}"
        )
        response = requests.request(
            method,
            url,
            headers=headers,
            data=payload,
            **kwargs,
            timeout=60,
            verify=False,
        )
        # if response is json
        try:
            response_dict = response.json()
        except Exception:
            response_dict = {}
        if "app_token" in response_dict:
            self.app_token = response_dict["app_token"]
        self.update_session_id(response)
        if url == self.captcha_token_url:
            return response

        if (
            "filename" in response_dict
            and "securimage_show" in response_dict["filename"]
        ):
            self.app_token = response_dict["app_token"]
            return self.solve_pdf_download_captcha(response_dict, payload)

        elif response_dict.get("session_expire") == "Y":
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        elif "errormsg" in response_dict:
            logger.error(f"Error {response_dict['errormsg']}")
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        return response

    def get_pdf_output_path(self, pdf_fragment):
        return output_dir / pdf_fragment.split("#")[0]

    def is_pdf_downloaded(self, pdf_fragment):
        pdf_metadata_path = self.get_pdf_output_path(pdf_fragment).with_suffix(".json")
        if pdf_metadata_path.exists():
            pdf_metadata = get_json_file(pdf_metadata_path)
            return pdf_metadata["downloaded"]
        return False

    def get_search_url(self):
        return f"{self.root_url}/pdfsearch/?p=pdf_search/home/"

    def default_search_payload(self):
        search_payload = urllib.parse.parse_qs(payload)
        search_payload = {k: v[0] for k, v in search_payload.items()}
        search_payload["sEcho"] = 1
        search_payload["iDisplayStart"] = 0
        search_payload["iDisplayLength"] = page_size
        return search_payload

    def default_pdf_link_payload(self):
        pdf_link_payload_o = urllib.parse.parse_qs(pdf_link_payload)
        pdf_link_payload_o = {k: v[0] for k, v in pdf_link_payload_o.items()}
        return pdf_link_payload_o

    def init_user_session(self):
        res = requests.request(
            "GET",
            f"{self.root_url}/pdfsearch/",
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            },
            timeout=30,
        )
        self.session_id = res.cookies.get(self.session_cookie_name)
        self.ecourts_token = res.cookies.get(self.ecourts_token_cookie_name)
        if self.ecourts_token is None:
            raise ValueError(
                "Failed to get session token, not expected to happen. This could happen if the IP might have been detected as spam"
            )

    def get_cookie(self):
        return f"{self.ecourts_token_cookie_name}={self.ecourts_token}; {self.session_cookie_name}={self.session_id}"

    def update_session_id(self, response):
        new_session_cookie = response.cookies.get(self.session_cookie_name)
        if new_session_cookie:
            self.session_id = new_session_cookie

    def get_headers(self):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.get_cookie(),
            "DNT": "1",
            "Origin": self.root_url,
            "Referer": self.root_url + "/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        return headers

    def download(self):
        """Process a specific date range for this court"""
        if self.task.from_date is None or self.task.to_date is None:
            logger.info(f"No more data to download for: task: {self.task}")
            return

        search_payload = self.default_search_payload()
        search_payload["from_date"] = self.task.from_date
        search_payload["to_date"] = self.task.to_date
        self.init_user_session()
        search_payload["state_code"] = self.court_code
        search_payload["app_token"] = self.app_token
        results_available = True
        pdfs_downloaded = 0

        logger.info(f"Downloading data for: task: {self.task}")

        while results_available:
            try:
                response = self.request_api("POST", self.search_url, search_payload)
                res_dict = response.json()
                if self._results_exist_in_search_response(res_dict):
                    results = res_dict["reportrow"]["aaData"]
                    num_results = len(results)

                    with tqdm(
                        total=num_results,
                        desc=f"Processing results for {self.task.court_code}",
                        unit="result",
                        leave=False,
                    ) as result_pbar:
                        for idx, row in enumerate(results):
                            try:
                                is_pdf_downloaded = self.process_result_row(
                                    row, row_pos=idx
                                )
                                if is_pdf_downloaded:
                                    pdfs_downloaded += 1
                                    result_pbar.set_postfix(downloaded=pdfs_downloaded)

                                result_pbar.update(1)

                                if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                    # after 25 downloads, need to solve captcha for every pdf link request
                                    logger.info(
                                        f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task}"
                                    )
                                    break
                            except Exception as e:
                                logger.error(
                                    f"Error processing row {row}: {e}, task: {self.task}"
                                )
                                traceback.print_exc()
                                result_pbar.update(1)

                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        logger.debug(
                            f"Resetting session after {NO_CAPTCHA_BATCH_SIZE} downloads, continuing with same search parameters"
                        )
                        self.init_user_session()
                        search_payload["app_token"] = self.app_token
                        continue

                    # prepare next iteration
                    search_payload = self._prepare_next_iteration(search_payload)
                else:
                    # No more results for this date range
                    results_available = False

            except Exception as e:
                logger.error(f"Error processing task: {self.task}, {e}")
                traceback.print_exc()
                # results_available = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""
Download judgments from Indian High Courts with intelligent S3 integration.

SMART DATE DETECTION:
- No dates: Automatically reads latest dates from S3 and downloads from next day
- With dates: Downloads specified range, skipping already-downloaded files

Examples:
  # Auto-detect dates and download for specific court
  python download.py --court_code 33_10
  
  # Download specific date range for a court
  python download.py --court_code 33_10 --start_date 2025-01-01 --end_date 2025-01-31
  
  # Download for all courts (auto-detect dates)
  python download.py
  
  # Just check what dates are in S3
  python download.py --fetch_dates
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--court_code",
        type=str,
        default=None,
        help="Single court code to process (accepts both 33_10 or 33~10 format)",
    )
    parser.add_argument(
        "--court_codes",
        type=str,
        default=[],
        help="Comma-separated court codes (e.g., 33_10,27_1 or 33~10,27~1)",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (omit to auto-detect from S3)",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (defaults to today)",
    )
    parser.add_argument(
        "--day_step", type=int, default=1, help="Number of days per chunk"
    )
    parser.add_argument(
        "--max_workers", type=int, default=2, help="Number of parallel workers"
    )
    parser.add_argument(
        "--fetch_dates",
        action="store_true",
        help="Just display latest dates from S3 index files without downloading",
    )
    parser.add_argument(
        "--s3_sync",
        action="store_true",
        default=False,
        help="Enable S3 integration (uploads to S3, checks existing files, syncs dates)",
    )
    args = parser.parse_args()

    if args.s3_sync:
        S3_ENABLED = True
        print("INFO: S3 integration enabled by --s3_sync flag")
    else:
        S3_ENABLED = False
        print("INFO: S3 integration disabled (use --s3_sync to enable)")

    # Handle different modes
    if args.fetch_dates:
        # Just print dates from S3 without downloading
        court_dates = get_court_dates_from_index_files()
        print(f"Found dates for {len(court_dates)} courts")
        print(json.dumps(court_dates, indent=2))
    else:
        # Main download mode (with intelligent date detection)
        if args.court_codes:
            assert args.court_code is None, (
                "court_code and court_codes cannot both be provided"
            )
            court_codes = args.court_codes.split(",")
        elif args.court_code:
            court_codes = [args.court_code]
        else:
            court_codes = None

        run(
            court_codes, args.start_date, args.end_date, args.day_step, args.max_workers
        )

"""
captcha prompt while downloading pdf seems to be different from session timeout
Every search API request returns a new app_token in response payload and new PHPSESSID in response cookies that need to be sent in the next request.
openpdfcaptcha request refreshes the app_token but not PHPSESSID

"""
