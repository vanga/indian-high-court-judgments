import os
import re
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime, timedelta
from tqdm import tqdm
import subprocess
import hashlib
from pathlib import Path
import sys

# ---- CONFIG ----
S3_BUCKET = "indian-high-court-judgments"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
DOWNLOAD_SCRIPT = "./download.py"
TRACK_FILE = "track.json"  # Used by download.py

# ---- Upload/Merge CONFIG ----
OUTPUT_DIR = Path("./data")  # Change if needed, downloader output root
TEST_BUCKET = S3_BUCKET + "-test"

def list_current_year_courts_and_benches(s3, year):
    """
    Returns dict: { court_code: { bench: [json_files...] } }
    """
    prefix = f"{S3_PREFIX}year={year}/"
    paginator = s3.get_paginator("list_objects_v2")
    result = {}

    print(f"DEBUG: Paginating S3 with Prefix: {prefix}")
    total_keys = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        contents = page.get("Contents", [])
        print(f"DEBUG: Got {len(contents)} keys in one page")
        for obj in contents:
            key = obj["Key"]
            total_keys += 1
            # key example: metadata/json/year=2025/court=1_12/bench=kashmirhc/JKHC010046902008_1_2020-09-21.json
            m = re.match(
                rf"{re.escape(S3_PREFIX)}year={year}/court=([^/]+)/bench=([^/]+)/([^/]+\.json)$",
                key
            )
            if not m:
                continue
            court_code, bench, json_file = m.group(1), m.group(2), m.group(3)
            result.setdefault(court_code, {}).setdefault(bench, []).append(key)
    print(f"DEBUG: Total keys found under year={year}: {total_keys}")
    print(f"DEBUG: Courts/benches loaded: {result.keys()}")
    return result

def validate_and_correct_json(court_code_dl, date):
    track_file_path = Path(TRACK_FILE)
    if not track_file_path.exists():
        print(f"[WARN] track.json not found, skipping validation.")
        return

    with open(track_file_path, 'r') as f:
        track_data = json.load(f)

    court_tracking = track_data.get(court_code_dl)
    if not court_tracking:
        print(f"[WARN] No tracking data for {court_code_dl}, skipping validation.")
        return

    # This is a bit of a hack, we don't know the exact file, so we'll look for the most recent
    # json file in the court's metadata directory for that day.
    date_str = date.strftime('%Y-%m-%d')
    year = date.strftime('%Y')
    court_code_s3 = court_code_dl.replace('~', '_')
    
    # Construct the path based on the S3 structure, but locally
    # Example: ./local_hc_metadata/year=2025/court=1_12/bench=kashmirhc/
    # We don't know the bench, so we have to search for it.
    
    base_path = Path(LOCAL_DIR) / f"year={year}" / f"court={court_code_s3}"
    if not base_path.exists():
        print(f"[WARN] Directory not found, cannot validate: {base_path}")
        return

    # Find all json files for the given date.
    all_json_files = []
    for bench_dir in base_path.iterdir():
        if bench_dir.is_dir():
            all_json_files.extend(list(bench_dir.glob(f'*{date_str}.json')))

    if not all_json_files:
        print(f"[WARN] No JSON files found for {court_code_dl} on {date_str} to validate.")
        return

    today = datetime.now().date()
    
    for json_file_path in all_json_files:
        print(f"[INFO] Validating {json_file_path}")
        with open(json_file_path, 'r+') as f:
            try:
                data = json.load(f)
                original_count = len(data)
                
                # The json file is a list of dicts
                corrected_data = []
                for record in data:
                    needs_correction = False
                    
                    # Check and correct decision_date
                    decision_date_str = record.get('decision_date')
                    if decision_date_str:
                        try:
                            decision_date = datetime.strptime(decision_date_str, '%d-%m-%Y').date()
                            if decision_date > today:
                                print(f"[WARN] Found future decision_date {decision_date} in record.")
                                needs_correction = True
                        except ValueError:
                            print(f"[WARN] Could not parse date '{decision_date_str}', keeping record.")
                    
                    # Check and correct pdf_link if it contains a future date
                    pdf_link = record.get('pdf_link')
                    if pdf_link:
                        # Look for patterns like HCBM050020952024_1_2028-03-28.pdf
                        pdf_date_match = re.search(r'_(\d{4})-(\d{2})-(\d{2})\.pdf$', pdf_link)
                        if pdf_date_match:
                            pdf_year = int(pdf_date_match.group(1))
                            pdf_month = int(pdf_date_match.group(2))
                            pdf_day = int(pdf_date_match.group(3))
                            
                            try:
                                pdf_date = datetime(pdf_year, pdf_month, pdf_day).date()
                                if pdf_date > today:
                                    print(f"[WARN] Found future date in pdf_link: {pdf_link}")
                                    
                                    # If we have a valid decision_date, use it to correct the pdf_link
                                    if decision_date_str and not needs_correction:
                                        decision_date = datetime.strptime(decision_date_str, '%d-%m-%Y').date()
                                        # Replace the future date in pdf_link with decision_date
                                        corrected_pdf_link = re.sub(
                                            r'_\d{4}-\d{2}-\d{2}\.pdf$',
                                            f'_{decision_date.strftime("%Y-%m-%d")}.pdf',
                                            pdf_link
                                        )
                                        print(f"[INFO] Corrected pdf_link: {pdf_link} -> {corrected_pdf_link}")
                                        record['pdf_link'] = corrected_pdf_link
                                    else:
                                        # If decision_date is also problematic, mark for removal
                                        needs_correction = True
                            except (ValueError, OverflowError):
                                print(f"[WARN] Invalid date in pdf_link: {pdf_link}, keeping record.")
                    
                    # Add record to corrected data if it doesn't need correction or we fixed it
                    if not needs_correction:
                        corrected_data.append(record)
                    else:
                        print(f"[INFO] Removing record with future date")

                if len(corrected_data) < original_count:
                    f.seek(0)
                    f.truncate()
                    json.dump(corrected_data, f, indent=4)
                    print(f"[INFO] Corrected {json_file_path}, removed {original_count - len(corrected_data)} records.")

            except json.JSONDecodeError:
                print(f"[WARN] Could not decode JSON from {json_file_path}, skipping.")

def run_downloader(court_code_dl, start_date):
    # end_date = start_date + timedelta(days=1)
    # end_date = start_date
    end_date = datetime.now().date()
    print(f"\nRunning downloader for court={court_code_dl} from {start_date} to {end_date} ...")
    sys.stdout.flush()  # Force flush before subprocess
    
    cmd = [
        "python", DOWNLOAD_SCRIPT,
        "--court_code", court_code_dl,
        "--start_date", start_date.strftime("%Y-%m-%d"),
        "--end_date", end_date.strftime("%Y-%m-%d"),
        "--day_step", "2"
    ]
    print("Command:", " ".join(cmd))
    sys.stdout.flush()
    
    # Use subprocess with proper output handling
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        universal_newlines=True,
        bufsize=1
    )
    
    # Read output line by line
    for line in process.stdout:
        print(f"[DOWNLOADER] {line.rstrip()}")
        sys.stdout.flush()
    
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
        
    validate_and_correct_json(court_code_dl, start_date)

# --- UPLOAD/MERGE LOGIC ---
def file_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def s3_key_from_local(local_path):
    """
    Maps local file structure to desired S3 structure:
    - PDFs go to: data/pdf/year=YYYY/court=X_Y/bench=BENCH/filename.pdf
    - JSON go to: metadata/json/year=YYYY/court=X_Y/bench=BENCH/filename.json
    - PDF Tars go to: data/tar/year=YYYY/court=X_Y/bench=BENCH/filename.tar
    - Parquet go to: metadata/parquet/year=YYYY/court=X_Y/bench=BENCH/filename.parquet
    - JSON Tars go to: metadata/tar/year=YYYY/court=X_Y/bench=BENCH/metadata.tar.gz
    """
    relative_path = local_path.relative_to(OUTPUT_DIR)
    parts = relative_path.parts
    
    print(f"DEBUG: Processing {relative_path}")
    
    # Expected structure: YEAR/COURT_CODE/BENCH/FILENAME
    if len(parts) >= 3:
        year = parts[0]
        court_code = parts[1] 
        bench = parts[2]
        filename = parts[-1]
        
        # Validate year format
        if year.isdigit() and len(year) == 4:
            year_part = f"year={year}"
            court_part = f"court={court_code}"
            bench_part = f"bench={bench}"
            
            # Determine file type and create proper S3 key
            if filename.endswith('.pdf'):
                return f"data/pdf/{year_part}/{court_part}/{bench_part}/{filename}"
            elif filename.endswith('.json'):
                return f"metadata/json/{year_part}/{court_part}/{bench_part}/{filename}"
            elif filename == 'pdfs.tar':
                return f"data/tar/{year_part}/{court_part}/{bench_part}/{filename}"
            elif filename == 'metadata.tar.gz':
                return f"metadata/tar/{year_part}/{court_part}/{bench_part}/{filename}"
            elif filename.endswith('.parquet'):
                return f"metadata/parquet/{year_part}/{court_part}/{bench_part}/{filename}"
    
    # If structure doesn't match, skip this file
    print(f"WARNING: Skipping file with unexpected structure: {relative_path}")
    return None

def s3_object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def upload_file(s3, local_path, bucket, key):
    s3.upload_file(str(local_path), bucket, key)
    print(f"Uploaded: {key}")

def merge_json(local_path, s3, bucket, key):
    tmp_remote = "remote_tmp.json"
    try:
        s3.download_file(bucket, key, tmp_remote)
        with open(tmp_remote, "r") as f:
            remote_data = json.load(f)
        os.remove(tmp_remote)
    except Exception:
        remote_data = []
    with open(local_path, "r") as f:
        local_data = json.load(f)
    # Merge: assumes list of dicts
    if isinstance(remote_data, dict): remote_data = [remote_data]
    if isinstance(local_data, dict): local_data = [local_data]
    merged = {json.dumps(item, sort_keys=True): item for item in remote_data + local_data}
    merged_list = list(merged.values())
    with open("merged_tmp.json", "w") as f:
        json.dump(merged_list, f, indent=4)
    upload_file(s3, "merged_tmp.json", bucket, key)
    os.remove("merged_tmp.json")
    print(f"Merged JSON: {key}")

def merge_parquet(local_path, s3, bucket, key):
    import pyarrow as pa
    import pyarrow.parquet as pq
    tmp_remote = "remote_tmp.parquet"
    try:
        s3.download_file(bucket, key, tmp_remote)
        remote_table = pq.read_table(tmp_remote)
        os.remove(tmp_remote)
    except Exception:
        remote_table = None
    local_table = pq.read_table(local_path)
    if remote_table:
        merged_table = pa.concat_tables([remote_table, local_table])
    else:
        merged_table = local_table
    pq.write_table(merged_table, "merged_tmp.parquet")
    upload_file(s3, "merged_tmp.parquet", bucket, key)
    os.remove("merged_tmp.parquet")
    print(f"Merged Parquet: {key}")

def merge_tar_files(local_path, s3, bucket, key):
    """Merge tar files by combining contents"""
    import tarfile
    import tempfile
    
    tmp_remote = "remote_tmp.tar"
    tmp_merged = "merged_tmp.tar"
    
    try:
        # Download existing tar
        s3.download_file(bucket, key, tmp_remote)
        
        # Create merged tar
        with tarfile.open(tmp_merged, 'w') as merged_tar:
            # Add files from remote tar
            with tarfile.open(tmp_remote, 'r') as remote_tar:
                for member in remote_tar.getmembers():
                    merged_tar.addfile(member, remote_tar.extractfile(member))
            
            # Add files from local tar (will overwrite if same name)
            with tarfile.open(local_path, 'r') as local_tar:
                for member in local_tar.getmembers():
                    merged_tar.addfile(member, local_tar.extractfile(member))
        
        # Upload merged tar
        upload_file(s3, tmp_merged, bucket, key)
        
        # Cleanup
        os.remove(tmp_remote)
        os.remove(tmp_merged)
        print(f"Merged tar: {key}")
        
    except Exception as e:
        print(f"Error merging tar {key}: {e}")
        # Fallback to simple upload
        upload_file(s3, local_path, bucket, key)

def merge_tar_gz_files(local_path, s3, bucket, key):
    """Merge tar.gz files by combining contents"""
    import tarfile
    
    tmp_remote = "remote_tmp.tar.gz"
    tmp_merged = "merged_tmp.tar.gz"
    
    try:
        # Download existing tar.gz
        s3.download_file(bucket, key, tmp_remote)
        
        # Create merged tar.gz
        with tarfile.open(tmp_merged, 'w:gz') as merged_tar:
            # Add files from remote tar.gz
            with tarfile.open(tmp_remote, 'r:gz') as remote_tar:
                for member in remote_tar.getmembers():
                    merged_tar.addfile(member, remote_tar.extractfile(member))
            
            # Add files from local tar.gz (will overwrite if same name)
            with tarfile.open(local_path, 'r:gz') as local_tar:
                for member in local_tar.getmembers():
                    merged_tar.addfile(member, local_tar.extractfile(member))
        
        # Upload merged tar.gz
        upload_file(s3, tmp_merged, bucket, key)
        
        # Cleanup
        os.remove(tmp_remote)
        os.remove(tmp_merged)
        print(f"Merged tar.gz: {key}")
        
    except Exception as e:
        print(f"Error merging tar.gz {key}: {e}")
        # Fallback to simple upload
        upload_file(s3, local_path, bucket, key)

def upload_and_merge_all():
    # Use the specified AWS profile for authentication
    s3 = boto3.client("s3")
    
    for local_path in OUTPUT_DIR.rglob("*"):
        if local_path.is_dir(): 
            continue
            
        key = s3_key_from_local(local_path)
        if key is None:  # Skip files that don't match expected structure
            continue
            
        bucket = TEST_BUCKET
        print(f"Checking {key}...")
        exists = s3_object_exists(s3, bucket, key)
        
        # Merge/upload logic
        if local_path.suffix == ".json":
            if exists:
                merge_json(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.suffix == ".parquet":
            if exists:
                merge_parquet(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.suffix == ".tar":
            if exists:
                merge_tar_files(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.name.endswith(".tar.gz"):
            if exists:
                merge_tar_gz_files(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.suffix == ".pdf":
            # PDFs can still use hash comparison since individual PDFs shouldn't change
            if exists:
                tmp_remote = "remote_tmp.pdf"
                try:
                    s3.download_file(bucket, key, tmp_remote)
                    remote_hash = file_md5(tmp_remote)
                    local_hash = file_md5(local_path)
                    os.remove(tmp_remote)
                    if remote_hash == local_hash:
                        print(f"PDF duplicate, skipping: {key}")
                        continue
                except Exception:
                    pass
            upload_file(s3, local_path, bucket, key)
        else:
            if not exists:
                upload_file(s3, local_path, bucket, key)

def reorganize_downloaded_files():
    """
    Reorganize downloaded files from eCourts structure to S3 structure
    From: data/court/cnrorders/BENCH/orders/FILENAME
    To: data/YEAR/COURT_CODE/BENCH/FILENAME
    """
    print("Reorganizing downloaded files...")
    
    # Find all downloaded files
    court_dir = OUTPUT_DIR / "court"
    if not court_dir.exists():
        print("No court directory found, nothing to reorganize")
        return
    
    cnr_dir = court_dir / "cnrorders"
    if not cnr_dir.exists():
        print("No cnrorders directory found")
        return
    
    # Process each bench directory
    for bench_dir in cnr_dir.iterdir():
        if not bench_dir.is_dir():
            continue
            
        bench_name = bench_dir.name  # FIX: was "bench" instead of "bench_dir.name"
        orders_dir = bench_dir / "orders"
        
        if not orders_dir.exists():
            continue
            
        print(f"Processing bench: {bench_name}")
        
        # Process all files in the orders directory
        for file_path in orders_dir.rglob("*"):
            if not file_path.is_file():
                continue
                
            # Extract year from filename (e.g., JKHC020008342013_1_2025-03-04.json)
            year_match = re.search(r'_(\d{4})-\d{2}-\d{2}\.', file_path.name)
            if not year_match:
                print(f"Could not extract year from {file_path.name}")
                continue
                
            year = year_match.group(1)
            
            # Map bench to court code
            court_code = get_court_code_from_bench(bench_name)
            
            # Create new directory structure
            new_dir = OUTPUT_DIR / year / court_code / bench_name
            new_dir.mkdir(parents=True, exist_ok=True)
            
            # Move file to new location
            new_path = new_dir / file_path.name
            if not new_path.exists():
                file_path.rename(new_path)
                print(f"Moved: {file_path} -> {new_path}")
    
    # Remove the old court directory after reorganization
    import shutil
    shutil.rmtree(court_dir)
    print("Removed old court directory structure")

def get_court_code_from_bench(bench_name):
    """
    Map bench name to court code based on the codebase mappings
    """
    # Based on the ATHENA.md mappings
    bench_to_court = {
    # Existing mappings
    'jammuhc': '1_12',
    'kashmirhc': '1_12',
    'taphc': '36_29',
    'aphc': '28_2',
    'calcutta_circuit_bench_at_jalpaiguri': '19_16',
    'calcutta_original_side': '19_16',
    'calcutta_circuit_bench_at_port_blair': '19_16',
    'nlghccis': '18_6',
    'azghccis': '18_6',
    'arghccis': '18_6',
    'asghccis': '18_6',
    
    # Allahabad High Court
    'cisdb_16012018': '9_13',
    'cishclko': '9_13',
    
    # Bombay High Court
    'newos_spl': '27_1',
    'hcbgoa': '27_1',
    'testcase': '27_1',
    'hcaurdb': '27_1',
    'newos': '27_1',
    'newas': '27_1',
    
    # Calcutta High Court
    'calcutta_appellate_side': '19_16',
    
    # High Court of Chhattisgarh
    'cghccisdb': '22_18',
    
    # High Court of Delhi
    'dhcdb': '7_26',
    
    # High Court of Gujarat
    'gujarathc': '24_17',
    
    # High Court of Himachal Pradesh
    'cmis': '2_5',
    
    # High Court of Jharkhand
    'jhar_pg': '20_7',
    
    # High Court of Karnataka
    'karhcdharwad': '29_3',
    'karnataka_bng_old': '29_3',
    'karhckalaburagi': '29_3',
    
    # High Court of Kerala
    'highcourtofkerala': '32_4',
    
    # High Court of Madhya Pradesh
    'mphc_db_ind': '23_23',
    'mphc_db_jbp': '23_23',
    'mphc_db_gwl': '23_23',
    
    # High Court of Manipur
    'manipurhc_pg': '14_25',
    
    # High Court of Meghalaya
    'meghalaya': '17_21',
    
    # High Court of Orissa
    'cisnc': '21_11',
    
    # High Court of Punjab and Haryana
    'phhc': '3_22',
    
    # High Court of Rajasthan
    'rhcjodh240618': '8_9',
    'jaipur': '8_9',
    
    # High Court of Sikkim
    'sikkimhc_pg': '11_24',
    
    # High Court of Tripura
    'thcnc': '16_20',
    
    # High Court of Uttarakhand
    'ukhcucis_pg': '5_15',
    
    # Madras High Court
    'hc_cis_mas': '33_10',
    'mdubench': '33_10',
    
    # Patna High Court
    'patnahcucisdb94': '10_8',
}
    
    return bench_to_court.get(bench_name)  # Default to 1_12 if not found

def main():
    year = datetime.now().year
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    # For listing data, we still use unsigned access to the main bucket
    s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    # For operations that require credentials, use the profile
    s3_auth = boto3.client("s3")

    courts_and_benches = list_current_year_courts_and_benches(s3_unsigned, year)
    print(f"Found {len(courts_and_benches)} courts for year={year}")

    # Print the latest date for every court
    for court_code, bench_files in courts_and_benches.items():
        all_dates = set()
        for bench, files in bench_files.items():
            for key in files:
                m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
                if m:
                    try:
                        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                        if d <= datetime.now().date():
                            all_dates.add(d)
                        else:
                            print(f"[WARN] Skipping future date {d} in filename {key}")
                    except Exception:
                        continue
        if all_dates:
            latest = max(all_dates)
            print(f"[LATEST] Court {court_code}: {latest}")
        else:
            print(f"[LATEST] Court {court_code}: No dates found")

    # Process ALL courts instead of just the test court
    for court_code in courts_and_benches:
        try:
            bench_files = courts_and_benches[court_code]
            all_dates = set()
            for bench, files in bench_files.items():
                for key in files:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
                    if m:
                        try:
                            d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                            if d.year == year and d <= datetime.now().date():
                                all_dates.add(d)
                        except Exception:
                            continue
            
            if not all_dates:
                print(f"[WARN] No valid dates found for court={court_code}")
                continue
                
            court_code_dl = court_code.replace('_', '~')
            
            print(f"[DEBUG] All dates for court {court_code}: {sorted(all_dates)}")
            
            # Process all dates for this court
            for dt in sorted(all_dates):
                run_downloader(court_code_dl, dt)
                
        except Exception as e:
            print(f"[ERROR] Failed to process court {court_code}: {str(e)}")
            continue  # Continue with next court even if one fails

    print("\nAll done downloading. Now reorganizing files...")
    
    # ---- REORGANIZE DOWNLOADED FILES ----
    reorganize_downloaded_files()
    
    # ---- CREATE TAR AND PARQUET FILES ----
    create_tar_and_parquet_files()

    print("\nFile reorganization complete. Starting upload/merge to test bucket...")
    
    # ---- UPLOAD AND MERGE TO TEST BUCKET ----
    upload_and_merge_all()
    print("Upload/merge complete.")

def create_tar_and_parquet_files():
    """
    Create tar files for PDFs and parquet files for metadata
    Also create tar.gz files for JSON metadata
    """
    print("Creating tar and parquet files...")
    
    # Process each year/court/bench directory
    for year_dir in OUTPUT_DIR.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
            
        for court_dir in year_dir.iterdir():
            if not court_dir.is_dir():
                continue
                
            for bench_dir in court_dir.iterdir():
                if not bench_dir.is_dir():
                    continue
                    
                print(f"Processing {year_dir.name}/{court_dir.name}/{bench_dir.name}")
                
                # Create parquet from JSON files
                json_files = list(bench_dir.glob("*.json"))
                if json_files:
                    parquet_path = bench_dir / "metadata.parquet"
                    if not parquet_path.exists():
                        create_parquet_from_json(json_files, parquet_path)
                
                # Create tar.gz from JSON files (metadata tar)
                if json_files:
                    metadata_tar_path = bench_dir / "metadata.tar.gz"
                    if not metadata_tar_path.exists():
                        create_metadata_tar_gz(bench_dir, metadata_tar_path)
                
                # Create tar from PDF files
                pdf_files = list(bench_dir.glob("*.pdf"))
                if pdf_files:
                    tar_path = bench_dir / "pdfs.tar"
                    if not tar_path.exists():
                        create_tar_from_pdfs(pdf_files, tar_path)

def create_parquet_from_json(json_files, output_path):
    """Create parquet file from JSON metadata files"""
    try:
        import pandas as pd
        
        all_data = []
        for json_file in json_files:
            with open(json_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_parquet(output_path, index=False)
            print(f"Created parquet: {output_path}")
    except Exception as e:
        print(f"Error creating parquet {output_path}: {e}")

def create_tar_from_pdfs(pdf_files, output_path):
    """Create tar file from PDF files"""
    try:
        import tarfile
        
        with tarfile.open(output_path, 'w') as tar:
            for pdf_file in pdf_files:
                tar.add(pdf_file, arcname=pdf_file.name)
        print(f"Created tar: {output_path}")
    except Exception as e:
        print(f"Error creating tar {output_path}: {e}")

def create_metadata_tar_gz(bench_dir, output_path):
    """
    Create a tar.gz file containing metadata JSON files for a bench
    """
    try:
        import tarfile
        
        with tarfile.open(output_path, 'w:gz') as tar:
            for json_file in bench_dir.glob("*.json"):
                tar.add(json_file, arcname=json_file.name)
        print(f"Created metadata tar.gz: {output_path}")
    except Exception as e:
        print(f"Error creating metadata tar.gz {output_path}: {e}")

if __name__ == "__main__":
    main()