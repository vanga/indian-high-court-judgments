"""S3 utility functions for uploading, downloading, and managing index files."""

import logging
import os
import shutil
import tarfile
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import json

from src.models import IndexFileV2, IndexPart
from src.utils.shared_utils import format_size, utc_now_iso, generate_part_name, get_metadata_index_key, get_data_index_key, get_bench_partition_key

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore import UNSIGNED
    from botocore.client import Config

    S3_AVAILABLE = True
    s3_client = boto3.client("s3")
    s3_client_unsigned = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED))
except ImportError as e:
    print(f"S3 dependencies not available: {e}")
    S3_AVAILABLE = False

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from process_metadata import MetadataProcessor

    PARQUET_AVAILABLE = True
except ImportError as e:
    print(f"Parquet dependencies not available: {e}")
    PARQUET_AVAILABLE = False

# Import functions that will be used by the S3 functions
from src.utils.file_utils import group_files_by_year_and_bench, cleanup_uploaded_files
from tqdm import tqdm

S3_READ_BUCKET = os.environ.get("S3_READ_BUCKET", "indian-high-court-judgments")
S3_WRITE_BUCKET = os.environ.get("S3_WRITE_BUCKET", "indian-high-court-judgments")
logger = logging.getLogger(__name__)

# Maximum tar file size before splitting into a new part (1 GB)
MAX_TAR_SIZE_BYTES = 1 * 1024 * 1024 * 1024


def extract_court_bench_from_path(key):
    """Extract court and bench from S3 key path"""
    parts = key.split("/")
    court_code = bench = None

    for part in parts:
        if part.startswith("court="):
            court_code = part[6:]
        elif part.startswith("bench="):
            bench = part[6:]

    return court_code, bench


def read_updated_at_from_index(s3, bucket, key):
    """Get updated_at timestamp from index file (V2 only)."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        data = json.loads(body)
        return data.get("updated_at")
    except Exception as e:
        print(f"Warning: Failed to read {key}: {e}")
        return None


# Filename date pattern: e.g. "UPHC020313492024_1_2025-10-07.pdf" -> "2025-10-07"
import re as _re
_FILENAME_DATE_RE = _re.compile(r"_(\d{4}-\d{2}-\d{2})\.(?:pdf|json)$", _re.IGNORECASE)


def _extract_resume_cursor_from_index(s3, bucket: str, key: str, partition_year: int) -> Optional[str]:
    """Return a YYYY-MM-DD resume cursor for one (bench, year) data index.

    Preference order:
    1. `scraped_through_date` field if present (new, authoritative)
    2. Max filename date from all part files (legacy fallback, with sanity
       bounds to reject garbage years: must be in [partition_year-1, current year])
    3. None (caller should bootstrap from a sane default)
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
    except Exception as e:
        print(f"Warning: Failed to read {key}: {e}")
        return None

    explicit = data.get("scraped_through_date")
    if explicit:
        return explicit

    # Legacy fallback: scan filenames for the max embedded YYYY-MM-DD,
    # filtering out garbage (eCourts has filenames with typo years like
    # 2046, 2202, 2205 in the wild). Only accept dates in a plausible
    # window: [partition_year-1, today]. Dates before that are too old
    # to trust as a resume cursor; dates after today are clearly typos.
    today_str = datetime.now().strftime("%Y-%m-%d")
    min_year = max(2000, partition_year - 1)

    max_date: Optional[str] = None
    for part in data.get("parts", []) or []:
        for fname in part.get("files", []) or []:
            m = _FILENAME_DATE_RE.search(fname)
            if not m:
                continue
            d = m.group(1)
            try:
                y = int(d[:4])
            except ValueError:
                continue
            if y < min_year:
                continue
            if d > today_str:
                continue
            if max_date is None or d > max_date:
                max_date = d
    return max_date


def write_scraped_through_date(data_type: str, year: int, court_code: str, bench: str, scraped_through_date: str) -> bool:
    """Set/advance the scraped_through_date field on a bench+year index.

    Only moves forward — if the existing value is newer, no write occurs.
    Creates the index if missing (with zero files/parts).
    """
    index_key = (
        get_metadata_index_key(year, court_code, bench)
        if data_type == "metadata"
        else get_data_index_key(year, court_code, bench)
    )
    index_data = _load_index_v2(S3_READ_BUCKET, index_key, data_type, court_code, bench, year)
    existing = index_data.scraped_through_date
    if existing and existing >= scraped_through_date:
        return False
    index_data.scraped_through_date = scraped_through_date
    index_data.updated_at = utc_now_iso()
    s3_client.put_object(
        Bucket=S3_WRITE_BUCKET,
        Key=index_key,
        Body=index_data.model_dump_json(indent=2),
        ContentType="application/json",
    )
    return True


def _load_index_v2(
    read_bucket: str,
    index_key: str,
    file_type: str,
    court_code: str,
    bench: str,
    year: int,
) -> IndexFileV2:
    """Load index file (V2). If missing, return empty V2."""
    try:
        resp = s3_client_unsigned.get_object(Bucket=read_bucket, Key=index_key)
        raw = resp["Body"].read().decode("utf-8")
        data = json.loads(raw)
        return IndexFileV2.model_validate(data)
    except Exception:
        # No existing index -> create empty V2
        now_iso = utc_now_iso()
        return IndexFileV2(
            file_count=0,
            tar_size=0,
            tar_size_human="0 B",
            updated_at=now_iso,
            parts=[],
        )


def get_court_dates_from_index_files(year=None):
    """
    Get resume-cursor dates from data index files.

    For each (court, bench, year), returns the highest date that has been
    successfully scraped. Preference order inside each index file:
        1. scraped_through_date field (new, authoritative)
        2. Max filename date across all indexed parts (legacy fallback)

    Across year partitions, the MAX resume cursor per bench is kept (so the
    bench's most-recent coverage wins even if it lives in a prior year's
    partition).

    Args:
        year: Year to check. If None, scans current year and up to 2 prior years.

    Returns:
        Dict of {court_code: {bench_name: "YYYY-MM-DD"}} where the date is
        the resume cursor (the latest date the scraper has confirmed coverage of).
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    if year is not None:
        years_to_check = [year]
    else:
        current_year = datetime.now().year
        years_to_check = [current_year, current_year - 1, current_year - 2]

    result: Dict[str, Dict[str, str]] = {}
    for yr in years_to_check:
        prefix = f"data/tar/year={yr}/"
        print(f"Reading resume cursors from data index files: {S3_READ_BUCKET}/{prefix}")

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_READ_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("data.index.json"):
                    continue

                court_code, bench = extract_court_bench_from_path(key)
                if not court_code or not bench:
                    continue

                cursor = _extract_resume_cursor_from_index(s3, S3_READ_BUCKET, key, yr)
                if not cursor:
                    continue

                bench_map = result.setdefault(court_code, {})
                existing = bench_map.get(bench)
                # Keep the latest cursor (dates are ISO, lexicographic sort = chronological)
                if existing is None or cursor > existing:
                    bench_map[bench] = cursor

    print(f"Found resume cursors for {len(result)} courts (across {years_to_check})")
    return result


def get_existing_files_from_s3(file_type, court_code, benches, year=None):
    """
    Fetch existing filenames from S3 index files for all benches of a court.
    Returns a dict: {bench_name: {'metadata': set(), 'data': set()}}

    Args:
        court_code: Court code (e.g., '33_10')
        benches: Dict of bench names
        year: Year to check (defaults to current year)
    """
    if not S3_AVAILABLE:
        return {}

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    year = year or datetime.now().year

    existing_files = {}

    for bench_name in benches.keys():
        existing_files[bench_name] = {"metadata": set(), "data": set()}

        metadata_key = get_metadata_index_key(year, court_code, bench_name)

        response = s3.get_object(Bucket=S3_READ_BUCKET, Key=metadata_key)
        raw = response["Body"].read().decode("utf-8")
        data = json.loads(raw)
        for part in data.get("parts", []):
            existing_files[bench_name]["metadata"].update(
                part.get("files", []))

        data_key = get_data_index_key(year, court_code, bench_name)
        response = s3.get_object(Bucket=S3_READ_BUCKET, Key=data_key)
        raw = response["Body"].read().decode("utf-8")
        data = json.loads(raw)
        for part in data.get("parts", []):
            existing_files[bench_name]["data"].update(
                part.get("files", []))

    total_metadata = sum(len(bench["metadata"])
                         for bench in existing_files.values())
    total_data = sum(len(bench["data"]) for bench in existing_files.values())
    print(
        f"  Found {total_metadata} existing metadata files and {total_data} existing PDFs in S3"
    )

    return existing_files


def get_existing_files_from_s3_v2(data_type: str, year: int, court_code: str, bench: str) -> List[str]:
    """Get existing files from S3"""
    all_files: List[str] = []
    index_key = (
        get_metadata_index_key(year, court_code, bench)
        if data_type == "metadata"
        else get_data_index_key(year, court_code, bench)
    )
    try:
        response = s3_client.get_object(Bucket=S3_READ_BUCKET, Key=index_key)
        raw = response["Body"].read().decode("utf-8")
        data = json.loads(raw)
        for part in data.get("parts", []):
            all_files.extend(part.get("files", []))
    except s3_client.exceptions.NoSuchKey:
        # Index file doesn't exist yet, return empty list
        return []
    return all_files


def upload_large_file_to_s3(
    file_path: str,
    bucket: str,
    key: str,
    content_type: str = "application/octet-stream",
    multipart_threshold: int = 5 * 1024 * 1024 * 1024,  # 5GB
    multipart_chunksize: int = 100 * 1024 * 1024,  # 100MB
    max_retries: int = 3,
) -> bool:
    file_path_obj = Path(file_path)
    file_size = file_path_obj.stat().st_size
    print(f"  File size: {format_size(file_size)}")

    transfer_config = TransferConfig(
        multipart_threshold=multipart_threshold,
        multipart_chunksize=multipart_chunksize,
        use_threads=True,
        max_concurrency=10,
    )

    for attempt in range(1, max_retries + 1):
        try:
            s3_client.upload_file(
                file_path,
                bucket,
                key,
                ExtraArgs={"ContentType": content_type},
                Config=transfer_config,
            )
            print("  Upload successful")
            return True
        except Exception as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"  Upload attempt {attempt}/{max_retries} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Upload failed after {max_retries} attempts: {e}")
                return False


def upload_single_file_to_s3(
    data_type: str,
    year: int, court_code: str, bench: str, local_file_path: str | Path,
    max_retries: int = 3,
) -> bool:
    """Upload a single file to S3 (without print statements for use with progress bar)"""
    file_path_obj = Path(local_file_path)
    filename = file_path_obj.name

    if data_type == "metadata":
        s3_key = (
            f"metadata/json/year={year}/court={court_code}/bench={bench}/{filename}"
        )
    else:
        s3_key = f"data/pdf/year={year}/court={court_code}/bench={bench}/{filename}"

    for attempt in range(1, max_retries + 1):
        try:
            with open(file_path_obj, "rb") as f:
                s3_client.put_object(
                    Bucket=S3_WRITE_BUCKET,
                    Key=s3_key,
                    Body=f,
                    ContentType="application/json"
                    if filename.endswith(".json")
                    else "application/pdf",
                )
            return True
        except FileNotFoundError as e:
            logger.warning(
                "Skipping missing local file during upload: %s, remote path: %s (%s)",
                local_file_path,
                s3_key,
                e,
            )
            return False
        except Exception as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                time.sleep(wait)
            else:
                print(
                    f"Failed to upload {local_file_path}, remote path: {s3_key} "
                    f"after {max_retries} attempts: {e}")
                return False


def upload_files_to_s3_v2(data_type: str, year: int, court_code: str, bench: str, files: List[Path]) -> bool:
    """Upload files to S3"""

    if not files:
        print(
            f"No files to upload for {data_type} in {year}/{court_code}/{bench}")
        return True

    upload_candidates = []
    missing_files = []
    for file in files:
        path = Path(file)
        if path.exists():
            upload_candidates.append(path)
        else:
            missing_files.append(path)

    if missing_files:
        logger.warning(
            "Skipping %d missing local file(s) before upload for %s/%s/%s/%s",
            len(missing_files),
            data_type,
            year,
            court_code,
            bench,
        )

    if not upload_candidates:
        print(
            f"No existing files to upload for {data_type} in {year}/{court_code}/{bench}"
        )
        return False

    # upload individual files
    failed_files = []
    for file in upload_candidates:
        ok = upload_single_file_to_s3(data_type, year, court_code, bench, file)
        if not ok:
            failed_files.append(file)

    if failed_files:
        print(
            f"WARNING: {len(failed_files)} individual file upload(s) failed for "
            f"{data_type}/{year}/{court_code}/{bench}. "
            f"Tar archive will still be created for all files."
        )

    create_and_upload_tar_file(
        data_type, year, court_code, bench, upload_candidates
    )

    return len(failed_files) == 0


def _get_indexed_filenames(data_type: str, year: int, court_code: str, bench: str) -> set:
    """Return the set of filenames already present in the S3 index for this (bench, year)."""
    index_key = get_metadata_index_key(
        year, court_code, bench) if data_type == "metadata" else get_data_index_key(year, court_code, bench)
    index_data = _load_index_v2(
        S3_READ_BUCKET, index_key, data_type, court_code, bench, year)
    indexed = set()
    for part in index_data.parts:
        indexed.update(part.files)
    return indexed


def update_index_file(data_type: str, year: int, court_code: str, bench: str, files: List[Path], tar_file_name: str, tar_file_size: int) -> bool:
    """Update the S3 index file with a new tar part.

    WARNING: This performs a non-atomic read-modify-write on S3. If multiple
    processes update the same index concurrently, one write can overwrite the
    other, causing tar parts to be lost from the index. Do not run multiple
    download.py instances for the same (court, bench, year) simultaneously.

    Duplicate filenames (already present in a prior indexed part) are silently
    filtered out before appending. Callers should pre-filter when possible to
    avoid wasting tar upload bandwidth on duplicates — see _flush_part.
    """
    index_key = get_metadata_index_key(
        year, court_code, bench) if data_type == "metadata" else get_data_index_key(year, court_code, bench)
    index_data = _load_index_v2(
        S3_READ_BUCKET, index_key, data_type, court_code, bench, year)
    current_files = set()
    for part in index_data.parts:
        current_files.update(part.files)
    new_files = [Path(p).name for p in files]
    duplicates = set(new_files) & current_files
    if duplicates:
        logger.warning(
            "Filtering %d already-indexed file(s) from new part %s for %s/%s/%s/%s",
            len(duplicates), tar_file_name, data_type, year, court_code, bench,
        )
        new_files = [n for n in new_files if n not in current_files]
        if not new_files:
            logger.warning(
                "All files in part %s already indexed; skipping index update "
                "(tar still uploaded to S3 and will be an orphan — clean up via audit).",
                tar_file_name,
            )
            return False
    size_human = format_size(tar_file_size)
    new_part = IndexPart(
        name=tar_file_name,
        files=new_files,
        file_count=len(new_files),
        size=tar_file_size,
        size_human=size_human,
        created_at=utc_now_iso(),
    )
    index_data.parts.append(new_part)
    index_data.file_count = index_data.file_count + len(new_files)
    index_data.tar_size = index_data.tar_size + \
        tar_file_size
    index_data.tar_size_human = format_size(index_data.tar_size)
    index_data.updated_at = utc_now_iso()
    s3_client.put_object(
        Bucket=S3_WRITE_BUCKET,
        Key=index_key,
        Body=index_data.model_dump_json(indent=2),
        ContentType="application/json",
    )
    return True


def create_and_upload_tar_file(data_type: str, year: int, court_code: str, bench: str, files: List[Path]) -> bool:
    """Create tar file(s) from files and upload to S3, splitting when size limit is exceeded.

    When the cumulative size of source files exceeds MAX_TAR_SIZE_BYTES, the current
    tar is closed/uploaded and a new part is started. Each part gets its own index entry.
    """
    parts_prefix = get_bench_partition_key(
        data_type, 'tar', year, court_code, bench)
    suffix = ".tar.gz" if data_type == "metadata" else ".tar"
    tar_mode = "w:gz" if data_type == "metadata" else "w"
    content_type = "application/x-tar.gz" if data_type == "metadata" else "application/x-tar"

    # Sort files by size for more predictable splitting
    files_with_size = []
    for f in files:
        try:
            size = Path(f).stat().st_size
        except OSError as e:
            print(f"Warning: Failed to get size for file {f}: {e}. Treating size as 0.")
            size = 0
        files_with_size.append((f, size))

    # Track current part
    current_part_files = []
    current_cumulative_size = 0
    parts_uploaded = 0

    def _flush_part(part_files):
        """Close, upload, and index the current tar part.

        Pre-checks against the S3 index and filters out any filenames that are
        already indexed. If nothing is left after filtering, the tar is never
        built or uploaded (preventing orphan tar creation).
        """
        nonlocal parts_uploaded

        # Pre-filter against the S3 index to avoid building a tar for files
        # that are already indexed. This prevents orphan tar creation when
        # there's any overlap (from concurrent writers, re-uploads, etc.).
        already_indexed = _get_indexed_filenames(data_type, year, court_code, bench)
        new_part_files = [pf for pf in part_files if Path(pf).name not in already_indexed]
        skipped = len(part_files) - len(new_part_files)
        if skipped > 0:
            logger.warning(
                "Skipping %d already-indexed file(s) before tar build for %s/%s/%s/%s",
                skipped, data_type, year, court_code, bench,
            )
        if not new_part_files:
            logger.info(
                "All %d files in this part are already indexed; skipping tar build entirely.",
                len(part_files),
            )
            return

        part_files = new_part_files

        part_name = generate_part_name(utc_now_iso())
        # Append a sequence suffix if we've already uploaded a part in this run
        # to avoid timestamp collisions within the same second
        if parts_uploaded > 0:
            part_file_name = f"{part_name}-{parts_uploaded}{suffix}"
        else:
            part_file_name = f"{part_name}{suffix}"
        part_key = parts_prefix + part_file_name

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_tar:
            temp_path = temp_tar.name

        try:
            with tarfile.open(temp_path, tar_mode) as tar:
                for pf in tqdm(part_files, desc=f"Adding files to tar part {parts_uploaded + 1}"):
                    tar.add(pf, arcname=Path(pf).name)

            tar_size = Path(temp_path).stat().st_size
            print(f"  Uploading tar part {part_file_name} ({format_size(tar_size)}, {len(part_files)} files)")
            upload_ok = upload_large_file_to_s3(temp_path, S3_WRITE_BUCKET, part_key, content_type)
            if not upload_ok:
                raise RuntimeError(
                    f"Failed to upload tar part {part_file_name} to S3. "
                    f"Index will NOT be updated to avoid phantom entries."
                )
            update_index_file(data_type, year, court_code, bench,
                              part_files, part_file_name, tar_size)
            parts_uploaded += 1
        finally:
            Path(temp_path).unlink(missing_ok=True)
    for file_path, file_size in files_with_size:
        # Guard against a single file being larger than the maximum allowed tar size.
        # In this case we cannot create a valid part that respects MAX_TAR_SIZE_BYTES,
        # so fail fast and let the caller decide how to handle it.
        if file_size > MAX_TAR_SIZE_BYTES:
            raise ValueError(
                f"File {file_path} (size={format_size(file_size)}) exceeds the maximum "
                f"tar part size of {format_size(MAX_TAR_SIZE_BYTES)}"
            )
    for file_path, file_size in files_with_size:
        # If adding this file would exceed the limit AND we already have files,
        # flush the current part first
        if current_part_files and (current_cumulative_size + file_size) > MAX_TAR_SIZE_BYTES:
            _flush_part(current_part_files)
            current_part_files = []
            current_cumulative_size = 0

        current_part_files.append(file_path)
        current_cumulative_size += file_size

    # Flush remaining files
    if current_part_files:
        _flush_part(current_part_files)

    if parts_uploaded > 1:
        print(f"  Split into {parts_uploaded} tar parts due to size limit ({format_size(MAX_TAR_SIZE_BYTES)})")

    return True


def create_and_upload_parquet_files(
    year: int, court_code: str, bench: str, files: Dict[str, List[Path]]
) -> bool:
    """Download existing parquet, append new data, and upload back to S3"""
    if not PARQUET_AVAILABLE:
        print("  Parquet libraries not available, skipping parquet creation")
        print("  Install with: pip install pyarrow")
        return True  # Consider this success since it's optional

    # Only process metadata files (JSON) for parquet conversion
    if not files["metadata"]:
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
                response = s3_client.get_object(
                    Bucket=S3_READ_BUCKET, Key=parquet_key)

                # Save existing parquet to temp file
                with open(existing_parquet_path, "wb") as f:
                    f.write(response["Body"].read())

                existing_data = pd.read_parquet(existing_parquet_path)
                print(
                    f"  Found existing parquet with {len(existing_data)} records")

            except s3_client.exceptions.NoSuchKey:
                print(f"  No existing parquet found, will create new one")
            except Exception as e:
                print(f"  Warning: Could not download existing parquet: {e}")

            # Create temporary directory for new JSON files
            new_json_dir = temp_path / "new_json"
            new_json_dir.mkdir()

            # Copy new JSON files to temporary directory
            for json_file in files["metadata"]:
                json_path = Path(json_file)
                dest_path = new_json_dir / json_path.name

                shutil.copy2(json_file, dest_path)

            # Create parquet from new JSON files
            print(f"  Processing {len(files['metadata'])} new JSON files...")
            mp = MetadataProcessor(new_json_dir, output_path=new_parquet_path)
            mp.process()

            new_data = pd.read_parquet(new_parquet_path)
            print(f"  Created parquet with {len(new_data)} new records")

            # Combine existing and new data
            if existing_data is not None:
                # Concatenate and remove duplicates based on a unique column (e.g., pdf_link)
                combined_data = pd.concat(
                    [existing_data, new_data], ignore_index=True)

                # Remove duplicates if pdf_link column exists
                if "pdf_link" in combined_data.columns:
                    before_dedup = len(combined_data)
                    combined_data = combined_data.drop_duplicates(
                        subset=["pdf_link"], keep="last"
                    )
                    after_dedup = len(combined_data)
                    duplicates_removed = before_dedup - after_dedup
                    if duplicates_removed > 0:
                        print(
                            f"Removed {duplicates_removed} duplicate records")
                else:
                    print(f"No pdf_link column found, keeping all records")

                print(
                    f"Combined parquet: {len(existing_data)} existing + {len(new_data)} new = {len(combined_data)} total records"
                )
            else:
                combined_data = new_data
                print(f"New parquet file with {len(combined_data)} records")

            # Save combined data to final parquet file
            combined_data.to_parquet(combined_parquet_path, index=False)

            # Upload combined parquet file to S3
            print(f"  Uploading updated parquet file...")
            with open(combined_parquet_path, "rb") as f:
                s3_client.put_object(
                    Bucket=S3_WRITE_BUCKET,
                    Key=parquet_key,
                    Body=f,
                    ContentType="application/octet-stream",
                )

            print(f"  Successfully uploaded updated parquet file")
            return True

    except Exception as e:
        print(f"  Failed to create/update parquet file: {e}")
        import traceback

        traceback.print_exc()
        return False
