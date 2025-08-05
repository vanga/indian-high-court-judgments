from typing import Optional, Generator
from tqdm import tqdm
import argparse
from datetime import datetime, timedelta
import traceback
import re
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import lxml.html as LH
from http.cookies import SimpleCookie
import urllib
import easyocr
import logging
import threading
import concurrent.futures
import urllib3
import uuid
import os
import hashlib
import shutil
import tarfile

# S3 imports - only imported when needed
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# add a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

reader = easyocr.Reader(["en"])

root_url = "https://judgments.ecourts.gov.in"
output_dir = Path("./data")
START_DATE = "2008-01-01"


payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=27~1&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&int_fin_party_val=undefined&int_fin_case_val=undefined&int_fin_court_val=undefined&int_fin_decision_val=undefined&act=&sel_search_by=undefined&sections=undefined&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=2&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ajax_req=true&app_token=1fbc7fbb840eb95975c684565909fe6b3b82b8119472020ff10f40c0b1c901fe"


pdf_link_payload = "val=0&lang_flg=undefined&path=cnrorders/taphc/orders/2017/HBHC010262202017_1_2047-06-29.pdf#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined&ajax_req=true&app_token=c64944b84c687f501f9692e239e2a0ab007eabab497697f359a2f62e4fcd3d10"

page_size = 5000
MATH_CAPTCHA = False
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)

# ---- S3 SYNC CONFIG ----
S3_BUCKET = "indian-high-court-judgments"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
BENCH_CODES_FILE = "bench-codes.json"
OUTPUT_DIR = output_dir  # Reuse existing output_dir


def get_json_file(file_path) -> dict:
    with open(file_path) as f:
        return json.load(f)


def get_court_codes():
    court_codes = get_json_file("./court-codes.json")
    return court_codes


def get_tracking_data():
    tracking_data = get_json_file("./track.json")
    return tracking_data


def save_tracking_data(tracking_data):
    with open("./track.json", "w") as f:
        json.dump(tracking_data, f)


def save_court_tracking_date(court_code, court_tracking):
    # acquire a lock
    lock.acquire()
    tracking_data = get_tracking_data()
    tracking_data[court_code] = court_tracking
    save_tracking_data(tracking_data)
    # release the lock
    lock.release()


def get_new_date_range(
    last_date: str, day_step: int = 1
) -> tuple[str | None, str | None]:
    last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")
    new_from_date_dt = last_date_dt + timedelta(days=1)
    new_to_date_dt = new_from_date_dt + timedelta(days=day_step - 1)
    if new_from_date_dt.date() > datetime.now().date():
        return None, None

    if new_to_date_dt.date() > datetime.now().date():
        new_to_date_dt = datetime.now().date()
    new_from_date = new_from_date_dt.strftime("%Y-%m-%d")
    new_to_date = new_to_date_dt.strftime("%Y-%m-%d")
    return new_from_date, new_to_date


def get_date_ranges_to_process(court_code, start_date=None, end_date=None, day_step=1):
    """
    Generate date ranges to process for a given court.
    If start_date is provided but no end_date, use current date as end_date.
    If neither is provided, use tracking data to determine the next date range.
    """
    # If start_date is provided but end_date is not, use current date as end_date
    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date and end_date:
        # Convert string dates to datetime objects
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate date ranges with specified step
        current_date = start_date_dt
        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)
    else:
        # Use tracking data to get next date range
        tracking_data = get_tracking_data()
        court_tracking = tracking_data.get(court_code, {})
        last_date = court_tracking.get("last_date", START_DATE)

        # Process from last_date to current date in chunks
        current_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        end_date_dt = datetime.now()

        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)


class CourtDateTask:
    """A task representing a court and date range to process"""

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
        court_codes = {
            court_code: all_court_codes[court_code] for court_code in court_codes
        }

    for code in court_codes:
        for from_date, to_date in get_date_ranges_to_process(
            code, start_date, end_date, day_step
        ):
            yield CourtDateTask(code, from_date, to_date)


def process_task(task):
    """Process a single court-date task"""
    try:
        downloader = Downloader(task)
        downloader.download()
    except Exception as e:
        court_codes = get_court_codes()
        logger.error(
            f"Error processing court {task.court_code} {court_codes.get(task.court_code, 'Unknown')}: {e}"
        )
        traceback.print_exc()


def run(court_codes=None, start_date=None, end_date=None, day_step=1, max_workers=2):
    """
    Run the downloader with optional parameters using Python's multiprocessing
    with a generator that yields tasks on demand.
    """
    # Create a task generator and convert to list to get total count
    print("Generating tasks...")
    tasks = list(generate_tasks(court_codes, start_date, end_date, day_step))
    print(f"Generated {len(tasks)} tasks to process")
    
    if not tasks:
        logger.info("No tasks to process")
        return

    # Use ProcessPoolExecutor with map to process tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use tqdm to show progress
        with tqdm(total=len(tasks), desc="Processing tasks", unit="task") as pbar:
            for i, result in enumerate(executor.map(process_task, tasks)):
                # process_task doesn't return anything, so we're just tracking progress
                task = tasks[i]
                pbar.set_description(f"Processing {task.court_code} ({task.from_date} to {task.to_date})")
                pbar.update(1)

    logger.info("All tasks completed")


# ---- S3 SYNC FUNCTIONS ----
def get_bench_codes():
    """Load bench to court mappings from bench-codes.json"""
    try:
        with open(BENCH_CODES_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARN] {BENCH_CODES_FILE} not found, using empty mapping")
        return {}


def list_current_year_courts_and_benches(s3, year):
    """
    Returns dict: { court_code: { bench: [json_files...] } }
    """
    prefix = f"{S3_PREFIX}year={year}/"
    paginator = s3.get_paginator("list_objects_v2")
    result = {}

    print(f"Scanning S3 for files with prefix: {prefix}")
    total_keys = 0
    
    # Get all pages first to count total objects for progress bar
    pages = list(paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix))
    total_objects = sum(len(page.get("Contents", [])) for page in pages)
    
    with tqdm(total=total_objects, desc="Processing S3 objects", unit="file") as pbar:
        for page in pages:
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj["Key"]
                total_keys += 1
                pbar.update(1)
                
                # key example: metadata/json/year=2025/court=1_12/bench=kashmirhc/JKHC010046902008_1_2020-09-21.json
                # Parse using string operations instead of regex since we know the structure
                if key.startswith(prefix) and key.endswith('.json'):
                    # Remove prefix to get: court=1_12/bench=kashmirhc/JKHC010046902008_1_2020-09-21.json
                    remaining = key[len(prefix):]
                    parts = remaining.split('/')
                    if len(parts) >= 3:
                        # parts[0] = court=1_12, parts[1] = bench=kashmirhc, parts[2] = filename.json
                        court_part = parts[0]
                        bench_part = parts[1]
                        
                        if court_part.startswith('court=') and bench_part.startswith('bench='):
                            court_code = court_part[6:]  # Remove 'court=' prefix
                            bench = bench_part[6:]  # Remove 'bench=' prefix
                            result.setdefault(court_code, {}).setdefault(bench, []).append(key)
    
    print(f"Found {total_keys} total files, organized into {len(result)} courts")
    return result


def run_incremental_download(court_code_dl, start_date):
    """
    Run incremental download for a specific court and date using download.py functions directly
    """
    end_date = datetime.now().date()
    print(f"\nRunning incremental download for court={court_code_dl} from {start_date} to {end_date} ...")
    
    try:
        # Create a task for this court and date range
        task = CourtDateTask(
            court_code=court_code_dl,
            from_date=start_date.strftime("%Y-%m-%d"),
            to_date=end_date.strftime("%Y-%m-%d")
        )
        
        # Process the task directly
        process_task(task)
        
        print(f"[SUCCESS] Completed download for court={court_code_dl} from {start_date}")
        
    except Exception as e:
        print(f"[ERROR] Failed to download for court={court_code_dl}: {str(e)}")
        raise


def sync_to_s3():
    """
    Main S3 sync function - finds latest dates on S3 and downloads incremental data
    """
    if not S3_AVAILABLE:
        print("[ERROR] S3 dependencies not available. Please install: pip install boto3 pyarrow pandas")
        return
    
    year = datetime.now().year
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    # For listing data, we still use unsigned access to the main bucket
    s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    courts_and_benches = list_current_year_courts_and_benches(s3_unsigned, year)
    print(f"Found {len(courts_and_benches)} courts for year={year}")

    # Print the latest date for every court
    for court_code, bench_files in courts_and_benches.items():
        all_dates = set()
        for bench, files in bench_files.items():
            for key in files:
                # Extract date from key like: metadata/json/year=2025/court=1_12/bench=kashmirhc/JKHC010046902008_1_2020-09-21.json
                # Instead of regex, use string operations since we know the structure
                if key.endswith('.json'):
                    filename = key.split('/')[-1]  # Get just the filename
                    if '_' in filename and '-' in filename:
                        # Find date pattern in filename
                        parts = filename.split('_')
                        if len(parts) >= 2:
                            last_part = parts[-1]
                            if '-' in last_part:
                                date_part = last_part.replace('.json', '')  # Remove extension
                                try:
                                    d = datetime.strptime(date_part, "%Y-%m-%d").date()
                                    if d <= datetime.now().date():
                                        all_dates.add(d)
                                    else:
                                        print(f"[WARN] Skipping future date {d} in filename {key}")
                                except ValueError:
                                    continue
        if all_dates:
            latest = max(all_dates)
            print(f"[LATEST] Court {court_code}: {latest}")
        else:
            print(f"[LATEST] Court {court_code}: No dates found")

    # Process ALL courts instead of just the test court
    court_list = list(courts_and_benches.keys())
    with tqdm(total=len(court_list), desc="Processing courts", unit="court") as pbar:
        for court_code in court_list:
            try:
                pbar.set_description(f"Processing court {court_code}")
                bench_files = courts_and_benches[court_code]
                all_dates = set()
                for bench, files in bench_files.items():
                    for key in files:
                        # Extract date from key using string operations instead of regex
                        if key.endswith('.json'):
                            filename = key.split('/')[-1]  # Get just the filename
                            if '_' in filename and '-' in filename:
                                # Find date pattern in filename
                                parts = filename.split('_')
                                if len(parts) >= 2:
                                    last_part = parts[-1]
                                    if '-' in last_part:
                                        date_part = last_part.replace('.json', '')  # Remove extension
                                        try:
                                            d = datetime.strptime(date_part, "%Y-%m-%d").date()
                                            if d.year == year and d <= datetime.now().date():
                                                all_dates.add(d)
                                        except ValueError:
                                            continue
                
                if not all_dates:
                    print(f"[WARN] No valid dates found for court={court_code}")
                    pbar.update(1)
                    continue
                    
                court_code_dl = court_code.replace('_', '~')
                
                print(f"[DEBUG] All dates for court {court_code}: {sorted(all_dates)}")
                
                # Process all dates for this court
                sorted_dates = sorted(all_dates)
                with tqdm(total=len(sorted_dates), desc=f"Downloading dates for court {court_code}", unit="date", leave=False) as date_pbar:
                    for dt in sorted_dates:
                        date_pbar.set_description(f"Court {court_code}: {dt}")
                        run_incremental_download(court_code_dl, dt)
                        date_pbar.update(1)
                        
            except Exception as e:
                print(f"[ERROR] Failed to process court {court_code}: {str(e)}")
                continue  # Continue with next court even if one fails
            finally:
                pbar.update(1)

    print("\nS3 sync completed successfully!")


class Downloader:
    def __init__(self, task: CourtDateTask):
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
        result = reader.readtext(str(captcha_filename))
        if not result:
            logger.warning(
                f"No result from captcha, task: {self.task.id}, retries: {retries}"
            )
            return self.solve_captcha(retries + 1, captcha_url)
        captch_text = result[0][1].strip()

        if MATH_CAPTCHA:
            if self.is_math_expression(captch_text):
                try:
                    answer = self.solve_math_expression(captch_text)
                    captcha_filename.unlink()
                    return answer
                except Exception as e:
                    logger.error(
                        f"Error solving math expression, task: {self.task.id}, retries: {retries}, captcha text: {captch_text}, Error: {e}"
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
            captcha_text = "".join([c for c in captch_text if c.isnumeric()])
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
                    
                    with tqdm(total=num_results, desc=f"Processing results for {self.task.court_code}", unit="result", leave=False) as result_pbar:
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--court_code", type=str, default=None)
    parser.add_argument("--court_codes", type=str, default=[])
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step", type=int, default=1, help="Number of days per chunk"
    )
    parser.add_argument("--max_workers", type=int, default=2, help="Number of workers")
    parser.add_argument("--sync-s3", action="store_true", help="Run S3 sync to download incremental data and sync to S3")
    args = parser.parse_args()

    # Handle S3 sync mode
    if args.sync_s3:
        sync_to_s3()
    else:
        # Regular download mode
        if args.court_codes:
            assert (
                args.court_code is None
            ), "court_code and court_codes cannot both be provided"
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
