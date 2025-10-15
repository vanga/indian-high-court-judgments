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
import urllib.parse
import easyocr
import logging
import threading
import concurrent.futures
import urllib3
import uuid
import os
import tempfile
import tarfile

# S3 imports - only imported when needed
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
    S3_AVAILABLE = True
except ImportError as e:
    print(f"S3 dependencies not available: {e}")
    S3_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from process_metadata import MetadataProcessor
    PARQUET_AVAILABLE = True
except ImportError as e:
    print(f"Parquet dependencies not available: {e}")
    PARQUET_AVAILABLE = False

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
S3_BUCKET = "indian-high-court-judgments-test"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
BENCH_CODES_FILE = "bench-codes.json"
OUTPUT_DIR = output_dir  # Reuse existing output_dir

# Global cache for existing files in S3 (used during sync_s3 to avoid re-downloads)
_existing_files_cache = {}


def format_size(size_bytes):
    """Format bytes into human readable string"""
    if size_bytes == 0:
        return "0 B"
    
    size_units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024.0 and unit_index < len(size_units) - 1:
        size /= 1024.0
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {size_units[unit_index]}"
    else:
        return f"{size:.2f} {size_units[unit_index]}"
    

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
        # Use global cache if available (set during sync_s3)
        global _existing_files_cache
        downloader = Downloader(task, existing_files_cache=_existing_files_cache if _existing_files_cache else None)
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
        print(f"Warning: {BENCH_CODES_FILE} not found, using empty mapping")
        return {}


def get_court_dates_from_index_files():
    """Get updated_at dates from data index files"""
    # return {'20_7': {'jhar_pg': '2025-05-05T00:00:00'}}  # For testing, return fixed data
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return {}
    
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    year = datetime.now().year
    prefix = f"data/tar/year={year}/"
    
    print(f"Reading dates from data index files: {S3_BUCKET}/{prefix}")
    
    result = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("data.index.json"):
                    continue
                    
                # Extract court and bench from path
                court_code, bench = extract_court_bench_from_path(key)
                if not court_code or not bench:
                    continue
                
                # Get updated_at from index file
                updated_at = read_updated_at_from_index(s3, S3_BUCKET, key)
                if updated_at:
                    if court_code not in result:
                        result[court_code] = {}
                    result[court_code][bench] = updated_at
    
    except Exception as e:
        print(f"Failed to fetch dates: {e}")
        return {}
    
    print(f"Found dates for {len(result)} courts")
    return result


def extract_court_bench_from_path(key):
    """Extract court and bench from S3 key path"""
    parts = key.split('/')
    court_code = bench = None
    
    for part in parts:
        if part.startswith('court='):
            court_code = part[6:]  # Remove 'court=' prefix
        elif part.startswith('bench='):
            bench = part[6:]  # Remove 'bench=' prefix
    
    return court_code, bench


def read_updated_at_from_index(s3, bucket, key):
    """Get updated_at timestamp from index file"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        index_data = json.loads(response['Body'].read().decode('utf-8'))
        return index_data.get('updated_at')
    except Exception as e:
        print(f"Warning: Failed to read {key}: {e}")
        return None


def get_existing_files_from_s3(court_code, benches):
    """
    Fetch existing filenames from S3 index files for all benches of a court.
    Returns a dict: {bench_name: {'metadata': set(), 'data': set()}}
    """
    if not S3_AVAILABLE:
        return {}
    
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    year = datetime.now().year
    
    existing_files = {}
    
    for bench_name in benches.keys():
        existing_files[bench_name] = {'metadata': set(), 'data': set()}
        
        # Fetch metadata index
        metadata_key = f"metadata/tar/year={year}/court={court_code}/bench={bench_name}/metadata.index.json"
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=metadata_key)
            index_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Extract filenames (without .json extension for comparison)
            for file_info in index_data.get('files', []):
                filename = file_info.get('name', '')
                if filename:
                    # Store without extension for easier comparison
                    base_name = filename.replace('.json', '')
                    existing_files[bench_name]['metadata'].add(base_name)
        except Exception as e:
            # Index file might not exist yet, that's okay
            pass
        
        # Fetch data index
        data_key = f"data/tar/year={year}/court={court_code}/bench={bench_name}/data.index.json"
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=data_key)
            index_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Extract filenames
            for file_info in index_data.get('files', []):
                filename = file_info.get('name', '')
                if filename:
                    existing_files[bench_name]['data'].add(filename)
        except Exception as e:
            # Index file might not exist yet, that's okay
            pass
    
    # Print summary
    total_metadata = sum(len(bench['metadata']) for bench in existing_files.values())
    total_data = sum(len(bench['data']) for bench in existing_files.values())
    print(f"  Found {total_metadata} existing metadata files and {total_data} existing PDFs in S3")
    
    return existing_files


def update_index_files_after_download(court_code, bench, new_files, to_date=None):
    """Update both metadata and data index files with new download information"""
    print("TO_DATE from index.json updater : ", to_date)
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return
    
    s3_client = boto3.client('s3')
    s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    year = datetime.now().year
    # Use to_date if provided, otherwise use current time
    if to_date:
        if isinstance(to_date, str):
            # If to_date is a string, parse it and convert to ISO format
            to_date_dt = datetime.strptime(to_date, "%Y-%m-%d")
        else:
            # If to_date is already a datetime/date object
            to_date_dt = to_date if hasattr(to_date, 'hour') else datetime.combine(to_date, datetime.min.time())
        update_time = to_date_dt.isoformat()
    else:
        update_time = datetime.now().isoformat()
    
    # Update both metadata and data index files
    updates = [
        {
            'type': 'metadata',
            'key': f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.index.json",
            'tar_key': f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz",
            'files': new_files.get('metadata', [])
        },
        {
            'type': 'data', 
            'key': f"data/tar/year={year}/court={court_code}/bench={bench}/data.index.json",
            'tar_key': f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar",
            'files': new_files.get('data', [])
        }
    ]
    
    for update in updates:
        if not update['files']:
            continue
            
        try:
            # Read existing index or create new one
            try:
                response = s3_unsigned.get_object(Bucket=S3_BUCKET, Key=update['key'])
                index_data = json.loads(response['Body'].read().decode('utf-8'))
            except Exception:
                index_data = {"files": [], "file_count": 0, "updated_at": update_time}
            
            # Add new files (store only filenames, not full paths)
            existing_files = set(index_data.get('files', []))
            for new_file in update['files']:
                filename = os.path.basename(new_file)  # Extract just the filename
                if filename not in existing_files:
                    index_data['files'].append(filename)
            
            # Update metadata
            index_data['file_count'] = len(index_data['files'])
            index_data['updated_at'] = update_time
            
            # Get actual tar file size
            try:
                tar_response = s3_unsigned.head_object(Bucket=S3_BUCKET, Key=update['tar_key'])
                tar_size = tar_response['ContentLength']
                index_data['tar_size'] = tar_size
                index_data['tar_size_human'] = format_size(tar_size)
            except Exception as e:
                print(f"Warning: Could not get tar size for {update['tar_key']}: {e}")
                index_data['tar_size'] = 0
                index_data['tar_size_human'] = "0 B"
            
            # Write back to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=update['key'],
                Body=json.dumps(index_data, indent=2),
                ContentType='application/json'
            )
            print(f"Updated {update['type']} index with {len(update['files'])} files")
            
        except Exception as e:
            print(f"Failed to update {update['type']} index: {e}")


def run_incremental_download(court_code_dl, start_date, end_date=None, max_workers=4):
    """Run download for specific court and date range with multiprocessing support"""
    # If no end_date provided, make it same as start_date
    if end_date is None:
        end_date = start_date
    
    print(f"Downloading {court_code_dl}: {start_date} → {end_date}")
    
    try:
        # Use the same robust approach as regular downloads
        # Instead of FileTrackingDownloader, use the regular run() function with threading
        downloaded_files = {'metadata': [], 'data': []}
        
        # Use run() function for robust multi-threaded downloading
        print(f"Starting multi-threaded download with {max_workers} workers...")
        
        # Call run with the specific court and date range
        run(
            court_codes=[court_code_dl], 
            # start_date="2025-05-06",
            # end_date=start_date.strftime("%Y-%m-%d")+3, 
            start_date=start_date.strftime("%Y-%m-%d"), 
            end_date=end_date.strftime("%Y-%m-%d"), 

            day_step=1, 
            max_workers=max_workers
        )
        
        print(f"Multi-threaded download completed: {court_code_dl}")
        
        # Now collect all downloaded files for this court and date range
        # Scan the output directory for files created in this date range
        from pathlib import Path
        import glob
        import json
        
        # Convert court_code_dl back to get the bench directory name
        # court_code_dl is like "20~7", need to find corresponding bench
        court_code_underscore = court_code_dl.replace('~', '_')  # "20_7"
        
        print(f"Debug: court_code_dl = {court_code_dl}, court_code_underscore = {court_code_underscore}")
        
        # Load bench codes to find the bench name
        try:
            with open('bench-codes.json', 'r') as f:
                bench_codes = json.load(f)
            
            # Find ALL bench names for this court code (not just the first one)
            target_benches = []
            for bench_name, court_code in bench_codes.items():
                if court_code == court_code_underscore:
                    target_benches.append(bench_name)
            
            if target_benches:
                print(f"Targeting {len(target_benches)} bench directories for court {court_code_underscore}: {target_benches}")
                # Get files from ALL bench directories for this court
                for target_bench in target_benches:
                    bench_path = Path(f"./data/court/cnrorders/{target_bench}")
                    if bench_path.exists():
                        json_files = list(bench_path.glob("**/*.json"))
                        pdf_files = list(bench_path.glob("**/*.pdf"))
                        
                        # Add all files from this bench
                        for json_file in json_files:
                            downloaded_files['metadata'].append(str(json_file))
                        
                        for pdf_file in pdf_files:
                            downloaded_files['data'].append(str(pdf_file))
                        
                        print(f"Found {len(json_files)} JSON and {len(pdf_files)} PDF files in {target_bench}")
                    else:
                        print(f"Warning: Bench directory {bench_path} does not exist")
            else:
                print(f"Warning: Could not find any bench for court code {court_code_underscore}")
                print(f"Available court codes in bench-codes.json: {list(bench_codes.values())}")
                
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
                            downloaded_files['metadata'].append(str(json_file))
                        
                        for pdf_file in pdf_files:
                            downloaded_files['data'].append(str(pdf_file))
        
        print(f"Collected {len(downloaded_files['metadata'])} metadata files and {len(downloaded_files['data'])} PDF files")
        return downloaded_files
        
    except Exception as e:
        print(f"Download failed {court_code_dl}: {e}")
        traceback.print_exc()
        return {'metadata': [], 'data': []}

def sync_to_s3(max_workers=4):
    """Sync new data to S3 for all courts"""
    if not S3_AVAILABLE:
        print("ERROR: S3 dependencies not available")
        return
    
    print("Starting S3 sync for all courts")
    
    # Get current dates from S3 index files
    court_dates = get_court_dates_from_index_files()
    print(court_dates)
    if not court_dates:
        print("No existing data found in S3, exiting")
        return
    

    
    # Filter for testing - only process court 10_8 
    # court_dates = {k: v for k, v in court_dates.items() if k == '10_8'}
    
    # Count total benches across all courts  
    total_benches = sum(len(benches) for benches in court_dates.values())
    print(f"Found {len(court_dates)} courts with {total_benches} total benches to process")
    
    for s3_court_code, benches in court_dates.items():
        court_code = s3_court_code.replace('_', '~')
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
                bench_latest_date = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00')).date()
                bench_dates[bench_name] = bench_latest_date
                print(f"  Bench {bench_name}: latest date = {bench_latest_date}")
                
                if earliest_date is None or bench_latest_date < earliest_date:
                    earliest_date = bench_latest_date
            except Exception as e:
                print(f"  Warning: Invalid date {updated_at_str} for bench {bench_name}: {e}")
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
            print(f"  No data to download for court {court_code} (start_date {start_date} > today {today})")
            continue
        
        print(f"  Downloading court {court_code}: {start_date} to {today}")
        print(f"  (Files already in S3 will be automatically skipped)")
        all_downloaded_files = run_incremental_download(court_code, start_date, today, max_workers)
        
        if not all_downloaded_files or (not all_downloaded_files['metadata'] and not all_downloaded_files['data']):
            print(f"  No files downloaded for court {court_code}")
            continue
        
        print(f"  Downloaded {len(all_downloaded_files['metadata'])} metadata and {len(all_downloaded_files['data'])} data files")
        
        # Step 3: Split downloaded files by bench
        bench_files = {}
        for bench_name in bench_dates.keys():
            bench_files[bench_name] = {'metadata': [], 'data': []}
        
        # Distribute metadata files to benches
        for metadata_file in all_downloaded_files.get('metadata', []):
            file_bench = extract_bench_from_path(metadata_file)
            if file_bench and file_bench in bench_files:
                bench_files[file_bench]['metadata'].append(metadata_file)
        
        # Distribute data files to benches
        for data_file in all_downloaded_files.get('data', []):
            file_bench = extract_bench_from_path(data_file)
            if file_bench and file_bench in bench_files:
                bench_files[file_bench]['data'].append(data_file)
        
        # Show what we found for each bench
        for bench_name, files in bench_files.items():
            print(f"  Found {len(files['metadata'])} metadata and {len(files['data'])} data files for bench {bench_name}")
        
        # Step 4: Upload and update index for each bench that has files
        for bench_name, files in bench_files.items():
            if not files['metadata'] and not files['data']:
                print(f"  No new files for bench {bench_name}, skipping")
                continue
            
            print(f"  Uploading files for bench {bench_name}...")
            
            # Upload files to S3 - returns dict of bench -> success status
            bench_upload_status = upload_files_to_s3(court_code, files)
            
            # Update index files for successfully uploaded benches
            for uploaded_bench, upload_success in bench_upload_status.items():
                if upload_success:
                    print(f"    S3 upload successful, updating index files for bench {uploaded_bench}")
                    
                    # Use today as the end_date for index update (we downloaded up to today)
                    update_index_files_after_download(s3_court_code, uploaded_bench, files, today)
                    
                    print(f"    Index files updated for bench {uploaded_bench}")
                else:
                    print(f"    S3 upload failed for bench {uploaded_bench}, NOT updating index files")
        
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
        print(f"    Court {court_code}, bench {bench_name}: No data to download (start_date {start_date} > end_date {end_date})")
        return {'metadata': [], 'data': []}
    
    # Convert court code for download (replace _ with ~)
    court_code_dl = court_code.replace('_', '~')
    
    print(f"    Downloading: Court {court_code}, bench {bench_name}: {start_date} to {end_date}")
    
    # Run download with max_workers support (this downloads for entire court)
    all_downloaded_files = run_incremental_download(court_code_dl, start_date, end_date, max_workers)
    
    # Filter files to only include those from the specific bench we're processing
    bench_filtered_files = {'metadata': [], 'data': []}
    
    # Filter metadata files
    for metadata_file in all_downloaded_files.get('metadata', []):
        file_bench = extract_bench_from_path(metadata_file)
        if file_bench == bench_name:
            bench_filtered_files['metadata'].append(metadata_file)
    
    # Filter data files
    for data_file in all_downloaded_files.get('data', []):
        file_bench = extract_bench_from_path(data_file)
        if file_bench == bench_name:
            bench_filtered_files['data'].append(data_file)
    
    print(f"    Filtered to {len(bench_filtered_files['metadata'])} metadata and {len(bench_filtered_files['data'])} data files for bench {bench_name}")
    
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
            filename = os.path.basename(pdf_fragment)
            
            # Check in cache for this bench
            if file_bench in self.existing_files_cache:
                bench_cache = self.existing_files_cache[file_bench]
                
                # Extract just the filename without extension for comparison
                base_filename = filename.replace('.pdf', '')
                
                # Check if this file already exists in S3
                if base_filename in bench_cache.get('metadata', set()) or \
                   filename in bench_cache.get('data', set()):
                    logger.debug(f"Skipping {filename} - already exists in S3 for bench {file_bench}")
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
            captcha_text = "".join([c for c in captch_text if c.isalnum()])
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
                        logger.info(f"Resetting session after {NO_CAPTCHA_BATCH_SIZE} downloads, continuing with same search parameters")
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




def upload_files_to_s3(court_code, downloaded_files):
    """Upload downloaded files to S3 bucket with progress bars. Returns dict of bench -> success status"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available for upload")
        return {}
    
    if not downloaded_files['metadata'] and not downloaded_files['data']:
        print(f"No files to upload for court {court_code}")
        return {}  # Empty dict means no benches to upload
    
    s3_client = boto3.client('s3')
    current_time = datetime.now()
    year = current_time.year
    
    print(f"Starting S3 upload for court {court_code}")
    print(f"Files to upload: {len(downloaded_files['metadata'])} metadata, {len(downloaded_files['data'])} data files")
    
    # Extract bench from file paths and organize by bench
    bench_files = {}
    
    # Process metadata files
    failed_metadata = 0
    for metadata_file in downloaded_files['metadata']:
        bench = extract_bench_from_path(metadata_file)
        if bench:
            if bench not in bench_files:
                bench_files[bench] = {'metadata': [], 'data': []}
            bench_files[bench]['metadata'].append(metadata_file)
        else:
            failed_metadata += 1
            if failed_metadata <= 3:  # Show first few failures
                print(f"Failed to extract bench from metadata file: {metadata_file}")
    
    if failed_metadata > 0:
        print(f"Warning: {failed_metadata} metadata files failed bench extraction")
    
    # Process data files  
    failed_data = 0
    for data_file in downloaded_files['data']:
        bench = extract_bench_from_path(data_file)
        if bench:
            if bench not in bench_files:
                bench_files[bench] = {'metadata': [], 'data': []}
            bench_files[bench]['data'].append(data_file)
        else:
            failed_data += 1
            if failed_data <= 3:  # Show first few failures
                print(f"Failed to extract bench from data file: {data_file}")
    
    if failed_data > 0:
        print(f"Warning: {failed_data} data files failed bench extraction")
    
    # Show final counts by bench
    print(f"Organized files by bench:")
    for bench_name, files in bench_files.items():
        print(f"  {bench_name}: {len(files['metadata'])} metadata, {len(files['data'])} data")
    
    bench_upload_status = {}  # Track success per bench
    
    # Upload files by bench
    for bench, files in bench_files.items():
        print(f"Processing bench: {bench}")
        
        bench_success = True  # Track this bench's upload success
        
        # Convert court code to S3 format
        s3_court_code = court_code.replace('~', '_')
        
        # Upload metadata files with progress bar
        if files['metadata']:
            print(f"Uploading {len(files['metadata'])} JSON metadata files...")
            with tqdm(total=len(files['metadata']), desc="JSON files", unit="file") as pbar:
                for metadata_file in files['metadata']:
                    success = upload_single_file_to_s3(
                        s3_client, metadata_file, s3_court_code, bench, year, 'metadata'
                    )
                    if not success:
                        bench_success = False
                    pbar.update(1)
        
        # Upload data files with progress bar
        if files['data']:
            print(f"Uploading {len(files['data'])} PDF data files...")
            with tqdm(total=len(files['data']), desc="PDF files", unit="file") as pbar:
                for data_file in files['data']:
                    success = upload_single_file_to_s3(
                        s3_client, data_file, s3_court_code, bench, year, 'data'
                    )
                    if not success:
                        bench_success = False
                    pbar.update(1)
        
        print(f"Completed individual file upload for bench {bench}")
        
        # Create and upload tar files for this bench
        tar_success = create_and_upload_tar_files(s3_client, s3_court_code, bench, year, files)
        if not tar_success:
            bench_success = False
        
        # Create and upload parquet files for this bench
        parquet_success = create_and_upload_parquet_files(s3_client, s3_court_code, bench, year, files)
        if not parquet_success:
            bench_success = False
        
        # Store this bench's upload status
        bench_upload_status[bench] = bench_success
        
        if bench_success:
            print(f"  Successfully uploaded all files for bench {bench}")
        else:
            print(f"  Some files failed to upload for bench {bench}")
    
    # Print overall summary
    successful_benches = [b for b, success in bench_upload_status.items() if success]
    failed_benches = [b for b, success in bench_upload_status.items() if not success]
    
    if successful_benches:
        print(f"S3 upload completed successfully for benches: {', '.join(successful_benches)}")
    if failed_benches:
        print(f"S3 upload had failures for benches: {', '.join(failed_benches)}")
    
    return bench_upload_status


def upload_large_file_to_s3(s3_client, file_path, bucket, key, content_type, chunk_size=100*1024*1024):
    """
    Upload a large file to S3 using multipart upload if necessary.
    
    Args:
        s3_client: boto3 S3 client
        file_path: Local file path to upload
        bucket: S3 bucket name
        key: S3 object key
        content_type: MIME type of the file
        chunk_size: Size of each part in bytes (default 100MB)
    """
    import math
    
    file_size = os.path.getsize(file_path)
    print(f"  File size: {format_size(file_size)}")
    
    # AWS S3 limits: Single PUT max 5GB, Multipart parts can be 5MB-5GB
    STANDARD_UPLOAD_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5GB (AWS single PUT limit)
    LARGE_MULTIPART_CHUNK = 5 * 1024 * 1024 * 1024  # 5GB per part for very large files
    
    # Use regular upload for files smaller than 5GB (AWS limit for single PUT)
    if file_size < STANDARD_UPLOAD_THRESHOLD:
        print(f"  Using standard upload (file < 5GB)")
        try:
            with open(file_path, 'rb') as f:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=f,
                    ContentType=content_type
                )
            print(f"  Standard upload completed successfully")
            return True
        except Exception as e:
            print(f"  ERROR: Standard upload failed: {e}")
            return False
    
    # Use multipart upload for large files (>= 5GB)
    # For files >= 5GB, use 5GB chunks to minimize part count and API calls
    chunk_size = LARGE_MULTIPART_CHUNK
    print(f"  Using multipart upload with 5GB chunks (file >= 5GB)")
    
    try:
        # Initialize multipart upload
        mpu = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType=content_type
        )
        upload_id = mpu['UploadId']
        
        parts = []
        part_number = 1
        total_parts = math.ceil(file_size / chunk_size)
        
        print(f"  Uploading {total_parts} parts...")
        
        with open(file_path, 'rb') as f:
            with tqdm(total=total_parts, desc="  Uploading parts", unit="part") as pbar:
                while True:
                    # Read chunk
                    data = f.read(chunk_size)
                    if not data:
                        break
                    
                    # Upload part
                    part_response = s3_client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data
                    )
                    
                    # Store part info
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part_response['ETag']
                    })
                    
                    part_number += 1
                    pbar.update(1)
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"  Multipart upload completed successfully")
        return True
        
    except Exception as e:
        print(f"  ERROR: Multipart upload failed: {e}")
        
        # Try to abort the multipart upload to clean up
        try:
            s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            print(f"  Aborted incomplete multipart upload")
        except Exception as abort_error:
            print(f"  Warning: Failed to abort multipart upload: {abort_error}")
        
        return False


def create_and_upload_tar_files(s3_client, court_code, bench, year, files):
    """Download existing tar files, append new content, and upload back to S3"""
    
    print(f"Creating/updating tar files for bench {bench}")
    
    overall_success = True  # Track success for both metadata and data tar files
    
    # Handle metadata tar file
    if files['metadata']:
        metadata_tar_key = f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz"
        
        # Try to download existing tar file
        existing_files_set = set()
        temp_existing_tar = None
        
        try:
            # Get file size first
            head_response = s3_client.head_object(Bucket=S3_BUCKET, Key=metadata_tar_key)
            file_size = head_response['ContentLength']
            file_size_human = format_size(file_size)
            
            print(f"  Downloading existing metadata tar ({file_size_human})...")
            
            # Create temp file on disk
            temp_existing_tar = tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False)
            
            # For smaller files, can download directly, for larger ones, stream
            if file_size > 100 * 1024 * 1024:  # If larger than 100MB, stream it
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=metadata_tar_key)
                chunk_size = 64 * 1024 * 1024  # 64MB chunks
                downloaded = 0
                
                for chunk in iter(lambda: response['Body'].read(chunk_size), b''):
                    temp_existing_tar.write(chunk)
                    downloaded += len(chunk)
                    progress = (downloaded / file_size) * 100
                    print(f"\r  Progress: {progress:.1f}% ({format_size(downloaded)}/{file_size_human})", end='', flush=True)
                print()  # New line
            else:
                # For smaller files, download normally
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=metadata_tar_key)
                temp_existing_tar.write(response['Body'].read())
            
            temp_existing_tar.close()
            
            # Read existing files list to avoid duplicates
            with tarfile.open(temp_existing_tar.name, 'r:gz') as existing_tar:
                existing_files_set = set(existing_tar.getnames())
                print(f"  Found existing metadata tar with {len(existing_files_set)} files")
            
        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing metadata tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing metadata tar: {e}")
        
        # Create new tar file with both existing and new content
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_new_tar:
            with tarfile.open(temp_new_tar.name, 'w:gz') as new_tar:
                
                # First, add existing files if we have them
                if temp_existing_tar and os.path.exists(temp_existing_tar.name):
                    try:
                        print(f"  Merging existing metadata tar content...")
                        with tarfile.open(temp_existing_tar.name, 'r:gz') as existing_tar:
                            for member in existing_tar.getmembers():
                                file_obj = existing_tar.extractfile(member)
                                if file_obj:
                                    new_tar.addfile(member, file_obj)
                    except Exception as e:
                        print(f"  Warning: Could not read existing tar content: {e}")
                
                # Then add new files (skip duplicates)
                new_files_added = 0
                with tqdm(total=len(files['metadata']), desc="Adding to metadata tar", unit="file") as pbar:
                    for metadata_file in files['metadata']:
                        arcname = os.path.basename(metadata_file)
                        if arcname not in existing_files_set:
                            new_tar.add(metadata_file, arcname=arcname)
                            new_files_added += 1
                            pbar.set_postfix_str(f"Added {new_files_added}")
                        else:
                            pbar.set_postfix_str(f"Skipped duplicate")
                        pbar.update(1)
                
                print(f"  Added {new_files_added} new metadata files to tar")
        
        # Upload updated tar to S3 (with multipart support for large files)
        print(f"  Uploading updated metadata tar...")
        success = upload_large_file_to_s3(
            s3_client, 
            temp_new_tar.name, 
            S3_BUCKET, 
            metadata_tar_key, 
            'application/gzip'
        )
        
        # Clean up temp files
        os.unlink(temp_new_tar.name)
        if temp_existing_tar and os.path.exists(temp_existing_tar.name):
            os.unlink(temp_existing_tar.name)
        
        if success:
            print(f"  Successfully uploaded updated metadata tar")
        else:
            print(f"  Failed to upload metadata tar")
            overall_success = False
    
    # Handle data/PDF tar file
    if files['data']:
        data_tar_key = f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar"
        
        # Check existing tar file size first
        existing_files_set = set()
        temp_existing_tar = None
        
        try:
            # Get file metadata to check size
            head_response = s3_client.head_object(Bucket=S3_BUCKET, Key=data_tar_key)
            file_size = head_response['ContentLength']
            file_size_human = format_size(file_size)
            
            print(f"  Existing data tar size: {file_size_human}")
            print(f"  Downloading existing data tar to disk (streaming)...")
            
            # Create temp file for existing tar (on disk, not in RAM)
            temp_existing_tar = tempfile.NamedTemporaryFile(suffix='.tar', delete=False)
            
            # Stream download in chunks to avoid loading entire file into RAM
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=data_tar_key)
            chunk_size = 256 * 1024 * 1024  # 256MB chunks (optimized for faster downloads)
            downloaded = 0
            
            print(f"  Downloading {file_size_human} in {chunk_size // (1024*1024)}MB chunks...")
            for chunk in iter(lambda: response['Body'].read(chunk_size), b''):
                temp_existing_tar.write(chunk)
                downloaded += len(chunk)
                progress = (downloaded / file_size) * 100
                print(f"\r  Progress: {progress:.1f}% ({format_size(downloaded)}/{file_size_human})", end='', flush=True)
            
            print()  # New line after progress
            temp_existing_tar.close()
            
            # Now read the tar file list (this is fast, just reads the index)
            print(f"  Reading existing tar file index...")
            with tarfile.open(temp_existing_tar.name, 'r') as existing_tar:
                existing_files_set = set(existing_tar.getnames())
                print(f"  Found existing data tar with {len(existing_files_set)} files")
            
        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing data tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing data tar: {e}")
        
        # Use APPEND mode for existing tar or CREATE mode for new tar
        if temp_existing_tar and os.path.exists(temp_existing_tar.name):
            # APPEND MODE - only adds new files to end of existing tar
            print(f"  Using APPEND mode to add new files to existing tar...")
            with tarfile.open(temp_existing_tar.name, 'a') as tar:  # 'a' = APPEND mode
                # Get existing files list (fast - just reads index)
                existing_files = set(tar.getnames())
                print(f"  Existing tar contains {len(existing_files)} files")
                
                # Append only new files to the end
                new_files_added = 0
                with tqdm(total=len(files['data']), desc="Appending to data tar", unit="file") as pbar:
                    for data_file in files['data']:
                        arcname = os.path.basename(data_file)
                        if arcname not in existing_files:
                            tar.add(data_file, arcname=arcname)  # Appends to END of tar
                            new_files_added += 1
                            pbar.set_postfix_str(f"Appended {new_files_added}")
                        else:
                            pbar.set_postfix_str(f"Skipped duplicate")
                        pbar.update(1)
                
                print(f"  Appended {new_files_added} new files to existing tar")
            
            # Upload the modified tar (same file, with new data appended)
            print(f"  Uploading updated data tar...")
            success = upload_large_file_to_s3(
                s3_client, 
                temp_existing_tar.name,  # Upload the appended tar
                S3_BUCKET, 
                data_tar_key, 
                'application/x-tar'
            )
            
            # Clean up temp file
            os.unlink(temp_existing_tar.name)
            
        else:
            # CREATE MODE - no existing tar, create new one
            print(f"  Creating new data tar file...")
            with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as temp_new_tar:
                with tarfile.open(temp_new_tar.name, 'w') as tar:  # 'w' = CREATE mode
                    new_files_added = 0
                    with tqdm(total=len(files['data']), desc="Creating data tar", unit="file") as pbar:
                        for data_file in files['data']:
                            arcname = os.path.basename(data_file)
                            tar.add(data_file, arcname=arcname)
                            new_files_added += 1
                            pbar.set_postfix_str(f"Added {new_files_added}")
                            pbar.update(1)
                    
                    print(f"  Created new tar with {new_files_added} files")
            
            # Upload new tar to S3
            print(f"  Uploading new data tar...")
            success = upload_large_file_to_s3(
                s3_client, 
                temp_new_tar.name, 
                S3_BUCKET, 
                data_tar_key, 
                'application/x-tar'
            )
            
            # Clean up temp file
            os.unlink(temp_new_tar.name)
        
        if success:
            print(f"  Successfully uploaded updated data tar")
        else:
            print(f"  Failed to upload data tar")
            overall_success = False
    
    return overall_success



def create_and_upload_parquet_files(s3_client, court_code, bench, year, files):
    """Download existing parquet, append new data, and upload back to S3"""
    if not PARQUET_AVAILABLE:
        print("  Parquet libraries not available, skipping parquet creation")
        print("  Install with: pip install pyarrow")
        return True  # Consider this success since it's optional
        
    # Only process metadata files (JSON) for parquet conversion
    if not files['metadata']:
        print("  No metadata files to convert to parquet")
        return True
    
    try:
        print(f"  Creating/updating parquet files for bench {bench}")
        
        # Define S3 key for parquet file
        parquet_key = f"metadata/parquet/year={year}/court={court_code}/bench={bench}/metadata.parquet"
        
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            existing_parquet_path = temp_path / "existing.parquet"
            new_parquet_path = temp_path / "new.parquet"
            combined_parquet_path = temp_path / "combined.parquet"
            
            # Try to download existing parquet file
            existing_data = None
            try:
                print(f"  Downloading existing parquet file...")
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=parquet_key)
                
                # Save existing parquet to temp file
                with open(existing_parquet_path, 'wb') as f:
                    f.write(response['Body'].read())
                
                # Read existing parquet data
                import pandas as pd
                existing_data = pd.read_parquet(existing_parquet_path)
                print(f"  Found existing parquet with {len(existing_data)} records")
                
            except s3_client.exceptions.NoSuchKey:
                print(f"  No existing parquet found, will create new one")
            except Exception as e:
                print(f"  Warning: Could not download existing parquet: {e}")
            
            # Create temporary directory for new JSON files
            new_json_dir = temp_path / "new_json"
            new_json_dir.mkdir()
            
            # Copy new JSON files to temporary directory
            for json_file in files['metadata']:
                json_path = Path(json_file)
                dest_path = new_json_dir / json_path.name
                import shutil
                shutil.copy2(json_file, dest_path)
            
            # Create parquet from new JSON files
            print(f"  Processing {len(files['metadata'])} new JSON files...")
            mp = MetadataProcessor(new_json_dir, output_path=new_parquet_path)
            mp.process()
            
            # Read new parquet data
            import pandas as pd
            new_data = pd.read_parquet(new_parquet_path)
            print(f"  Created parquet with {len(new_data)} new records")
            
            # Combine existing and new data
            if existing_data is not None:
                # Concatenate and remove duplicates based on a unique column (e.g., pdf_link)
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)
                
                # Remove duplicates if pdf_link column exists
                if 'pdf_link' in combined_data.columns:
                    before_dedup = len(combined_data)
                    combined_data = combined_data.drop_duplicates(subset=['pdf_link'], keep='last')
                    after_dedup = len(combined_data)
                    duplicates_removed = before_dedup - after_dedup
                    if duplicates_removed > 0:
                        print(f"Removed {duplicates_removed} duplicate records")
                else:
                    print(f"No pdf_link column found, keeping all records")
                
                print(f"Combined parquet: {len(existing_data)} existing + {len(new_data)} new = {len(combined_data)} total records")
            else:
                combined_data = new_data
                print(f"New parquet file with {len(combined_data)} records")
            
            # Save combined data to final parquet file
            combined_data.to_parquet(combined_parquet_path, index=False)
            
            # Upload combined parquet file to S3
            print(f"  Uploading updated parquet file...")
            with open(combined_parquet_path, 'rb') as f:
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=parquet_key,
                    Body=f,
                    ContentType='application/octet-stream'
                )
            
            print(f"  Successfully uploaded updated parquet file")
            return True
            
    except Exception as e:
        print(f"  Failed to create/update parquet file: {e}")
        import traceback
        traceback.print_exc()
        return False


def extract_bench_from_path(file_path):
    """Extract bench name from file path like data/court/cnrorders/sikkimhc_pg/orders/..."""
    import os
    path_parts = file_path.split(os.sep)
    try:
        # Find the index of 'cnrorders' and get the next part as bench
        cnrorders_index = path_parts.index('cnrorders')
        if cnrorders_index + 1 < len(path_parts):
            return path_parts[cnrorders_index + 1]
    except (ValueError, IndexError):
        pass
    return None


def upload_single_file_to_s3(s3_client, local_file_path, court_code, bench, year, file_type):
    """Upload a single file to S3 (without print statements for use with progress bar)"""
    try:
        # Determine S3 key based on file type and your bucket structure
        filename = os.path.basename(local_file_path)
        
        if file_type == 'metadata':
            s3_key = f"metadata/json/year={year}/court={court_code}/bench={bench}/{filename}"
        else: 
            s3_key = f"data/pdf/year={year}/court={court_code}/bench={bench}/{filename}"
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=f,
                ContentType='application/json' if filename.endswith('.json') else 'application/pdf'
            )
        
        return True
        
    except Exception as e:
        print(f"Failed to upload {local_file_path}: {e}")
        return False


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
    parser.add_argument("--sync-s3", action="store_true", help="Run S3 sync for all courts")
    parser.add_argument("--fetch-dates", action="store_true", help="Fetch latest dates from tar index files")
    args = parser.parse_args()

    # Handle different modes
    if args.fetch_dates:
        court_dates = get_court_dates_from_index_files()
        print(f"Found dates for {len(court_dates)} courts")
    elif args.sync_s3:
        sync_to_s3(max_workers=args.max_workers)
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