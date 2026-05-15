"""S3 utility functions for uploading, downloading, and managing index files."""

import logging
import io
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

    S3_AVAILABLE = True
    s3_client = boto3.client("s3")
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

S3_BUCKET = os.environ.get("S3_BUCKET", "indian-high-court-judgments")
logger = logging.getLogger(__name__)

# Maximum tar file size before splitting into a new part (1 GB)
MAX_TAR_SIZE_BYTES = 1 * 1024 * 1024 * 1024
# Parts at or below this size are eligible for in-place append on the next run.
# Anything bigger becomes immutable — re-downloading/re-uploading a 1 GB tar just
# to add a handful of small files wastes bandwidth. 100 MB keeps daily deltas
# cheap while still absorbing the vast majority of fragmentation on small benches.
APPEND_ELIGIBLE_SIZE_BYTES = 100 * 1024 * 1024


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


def _build_parquet_dedupe_key_frame(df):
    """Build a stable dedupe key for parquet rows.

    Prefer `cnr + decision_date` when present. CNR identifies the case, while
    decision_date keeps distinct dated judgment/order records for the same case.
    Fall back to `pdf_link` only when the logical key is incomplete.
    """
    if "cnr" in df.columns:
        cnr = df["cnr"].astype("string").fillna("").str.strip()
    else:
        cnr = None

    if "decision_date" in df.columns:
        decision_date = df["decision_date"].astype("string").fillna("").str.strip()
    else:
        decision_date = None

    if "pdf_link" in df.columns:
        pdf_link = df["pdf_link"].astype("string").fillna("").str.strip()
    else:
        pdf_link = None

    if cnr is None and pdf_link is None:
        return pd.Series([""] * len(df), index=df.index, dtype="string")

    if cnr is None or decision_date is None:
        return "pdf:" + pdf_link

    logical_key = "cnr:" + cnr + "|decision_date:" + decision_date
    has_logical_key = (cnr != "") & (decision_date != "")

    if pdf_link is None:
        return logical_key.where(has_logical_key, "")

    return logical_key.where(has_logical_key, "pdf:" + pdf_link)


def dedupe_parquet_records(df: "pd.DataFrame") -> "pd.DataFrame":
    """Drop duplicate logical rows from a parquet frame.

    The parquet writer historically deduped on `pdf_link`, which misses the
    repeated-CNR/same-decision-date cases we observed. This helper prefers
    `cnr + decision_date` and only falls back to `pdf_link` when needed.
    """
    if df.empty:
        return df

    key = _build_parquet_dedupe_key_frame(df)
    if "_dedupe_key" in df.columns:
        df = df.drop(columns=["_dedupe_key"])
    df = df.copy()
    df["_dedupe_key"] = key
    before = len(df)
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="last").drop(
        columns=["_dedupe_key"]
    )
    after = len(df)
    if after != before:
        print(f"  Removed {before - after} duplicate parquet records")
    return df


def filter_parquet_records_against_keys(
    df: "pd.DataFrame", existing_keys: set[str]
) -> "pd.DataFrame":
    """Remove records whose logical key already exists elsewhere in S3.

    This is used for court-year parquet regeneration so a repeated logical row
    survives in one bench parquet file, not in every bench partition.
    """
    if df.empty or not existing_keys:
        return df

    key = _build_parquet_dedupe_key_frame(df)
    mask = ~key.isin(existing_keys)
    removed = int((~mask).sum())
    if removed:
        print(f"  Removed {removed} parquet records already present in sibling benches")
    return df.loc[mask].copy()


def _load_existing_parquet_keys_for_court_year(
    year: int, court_code: str, exclude_bench: str
) -> set[str]:
    """Load logical keys from sibling bench parquet files for a court-year."""
    if not PANDAS_AVAILABLE:
        return set()

    existing_keys: set[str] = set()
    prefix = f"metadata/parquet/year={year}/court={court_code}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/metadata.parquet"):
                continue
            if f"/bench={exclude_bench}/" in key:
                continue
            try:
                body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                sibling = pd.read_parquet(
                    io.BytesIO(body), columns=["cnr", "decision_date", "pdf_link"]
                )
            except Exception as e:
                print(f"  Warning: Could not read sibling parquet {key}: {e}")
                continue
            existing_keys.update(_build_parquet_dedupe_key_frame(sibling).dropna().tolist())
    return existing_keys


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
    index_data = _load_index_v2(index_key)
    existing = index_data.scraped_through_date
    if existing and existing >= scraped_through_date:
        return False
    index_data.scraped_through_date = scraped_through_date
    index_data.updated_at = utc_now_iso()
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=index_key,
        Body=index_data.model_dump_json(indent=2),
        ContentType="application/json",
    )
    return True


def _load_index_v2(index_key: str) -> IndexFileV2:
    """Load index file (V2) from the canonical bucket. Returns empty V2 if missing."""
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=index_key)
        raw = resp["Body"].read().decode("utf-8")
        data = json.loads(raw)
        return IndexFileV2.model_validate(data)
    except Exception:
        return IndexFileV2(
            file_count=0,
            tar_size=0,
            tar_size_human="0 B",
            updated_at=utc_now_iso(),
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
    if year is not None:
        years_to_check = [year]
    else:
        current_year = datetime.now().year
        years_to_check = [current_year, current_year - 1, current_year - 2]

    result: Dict[str, Dict[str, str]] = {}
    for yr in years_to_check:
        prefix = f"data/tar/year={yr}/"
        print(f"Reading resume cursors from data index files: {S3_BUCKET}/{prefix}")

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("data.index.json"):
                    continue

                court_code, bench = extract_court_bench_from_path(key)
                if not court_code or not bench:
                    continue

                cursor = _extract_resume_cursor_from_index(s3_client, S3_BUCKET, key, yr)
                if not cursor:
                    continue

                bench_map = result.setdefault(court_code, {})
                existing = bench_map.get(bench)
                # Keep the latest cursor (dates are ISO, lexicographic sort = chronological)
                if existing is None or cursor > existing:
                    bench_map[bench] = cursor

    print(f"Found resume cursors for {len(result)} courts (across {years_to_check})")
    return result



def get_existing_files_from_s3_v2(data_type: str, year: int, court_code: str, bench: str) -> List[str]:
    """Get existing files from S3"""
    all_files: List[str] = []
    index_key = (
        get_metadata_index_key(year, court_code, bench)
        if data_type == "metadata"
        else get_data_index_key(year, court_code, bench)
    )
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=index_key)
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
                    Bucket=S3_BUCKET,
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
    index_data = _load_index_v2(index_key)
    indexed = set()
    for part in index_data.parts:
        indexed.update(part.files)
    return indexed


def update_index_file(
    data_type: str,
    year: int,
    court_code: str,
    bench: str,
    files: List[Path],
    tar_file_name: str,
    tar_file_size: int,
    replace_part_name: Optional[str] = None,
) -> bool:
    """Update the S3 index file with a new tar part.

    WARNING: This performs a non-atomic read-modify-write on S3. If multiple
    processes update the same index concurrently, one write can overwrite the
    other, causing tar parts to be lost from the index. Do not run multiple
    download.py instances for the same (court, bench, year) simultaneously.

    Duplicate filenames (already present in a prior indexed part) are silently
    filtered out before appending. Callers should pre-filter when possible to
    avoid wasting tar upload bandwidth on duplicates — see _flush_part.

    If ``replace_part_name`` is given, the part with that name is removed from
    the index and the new part is written in its place. The new part's ``files``
    list becomes the union of the replaced part's files and the incoming new
    files (deduped). This is used by the hybrid append path — callers upload a
    merged tar under a brand-new key, then swap the index entry in one PUT.
    """
    index_key = get_metadata_index_key(
        year, court_code, bench) if data_type == "metadata" else get_data_index_key(year, court_code, bench)
    index_data = _load_index_v2(index_key)

    old_part: Optional[IndexPart] = None
    if replace_part_name:
        for p in index_data.parts:
            if p.name == replace_part_name:
                old_part = p
                break
        if old_part is None:
            logger.warning(
                "replace_part_name=%s not found in index for %s/%s/%s/%s; "
                "proceeding as plain append.",
                replace_part_name, data_type, year, court_code, bench,
            )
        else:
            index_data.parts = [p for p in index_data.parts if p.name != replace_part_name]
            index_data.file_count -= old_part.file_count
            index_data.tar_size -= old_part.size

    # Dedup incoming files against everything still in the index (i.e. excluding
    # the part we just removed, whose files will be re-added via the merged part).
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

    if old_part is not None:
        # Merged part carries the old part's files plus the new unique ones.
        old_files_set = set(old_part.files)
        unique_new = [n for n in new_files if n not in old_files_set]
        part_files_list = list(old_part.files) + unique_new
    else:
        part_files_list = new_files

    if not part_files_list:
        logger.warning(
            "All files in part %s already indexed; skipping index update "
            "(tar still uploaded to S3 and will be an orphan — clean up via audit).",
            tar_file_name,
        )
        return False

    size_human = format_size(tar_file_size)
    new_part = IndexPart(
        name=tar_file_name,
        files=part_files_list,
        file_count=len(part_files_list),
        size=tar_file_size,
        size_human=size_human,
        created_at=utc_now_iso(),
    )
    index_data.parts.append(new_part)
    index_data.file_count = index_data.file_count + len(part_files_list)
    index_data.tar_size = index_data.tar_size + tar_file_size
    index_data.tar_size_human = format_size(index_data.tar_size)
    index_data.updated_at = utc_now_iso()
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=index_key,
        Body=index_data.model_dump_json(indent=2),
        ContentType="application/json",
    )
    return True


def _find_append_target(
    index_data: IndexFileV2, new_files_total_size: int
) -> Optional[IndexPart]:
    """Return the latest tar part eligible for in-place append, or None.

    Eligibility (all must hold):
      1. Part has the max ``created_at`` across all parts in the index.
      2. ``part.size <= APPEND_ELIGIBLE_SIZE_BYTES`` (cheap to re-upload).
      3. ``part.size + new_files_total_size <= MAX_TAR_SIZE_BYTES`` (stays
         under the split ceiling).

    Empty parts list, missing/malformed ``created_at``, or legacy oversized
    parts all return None — the caller falls through to new-part creation.
    """
    if not index_data.parts:
        return None
    parts_with_ts = [p for p in index_data.parts if p.created_at]
    if not parts_with_ts:
        return None
    try:
        latest = max(parts_with_ts, key=lambda p: p.created_at)
    except (TypeError, ValueError):
        return None
    if latest.size > APPEND_ELIGIBLE_SIZE_BYTES:
        return None
    if latest.size + new_files_total_size > MAX_TAR_SIZE_BYTES:
        return None
    return latest


def _append_to_existing_part(
    data_type: str,
    year: int,
    court_code: str,
    bench: str,
    part_files: List[Path],
    target: IndexPart,
    parts_prefix: str,
    suffix: str,
    tar_mode: str,
    content_type: str,
) -> bool:
    """Best-effort: merge ``part_files`` into ``target`` tar and swap the index.

    Returns True on full success. Returns False on any failure so the caller can
    fall through to the normal new-part path in the same run.

    Crash-safety ordering: upload merged tar under a brand-new key → swap index
    in one PUT → delete old part. The merged tar is never uploaded under the
    old part's key, so a crash between upload and index update leaves the old
    part authoritative (merged tar becomes an orphan for audit cleanup).
    """
    old_part_key = parts_prefix + target.name
    old_tar_temp: Optional[str] = None
    merged_tar_temp: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            old_tar_temp = tmp.name
        try:
            s3_client.download_file(S3_BUCKET, old_part_key, old_tar_temp)
        except Exception as e:
            logger.warning(
                "Append: failed to download old part s3://%s/%s (%s); falling through to new-part",
                S3_BUCKET, old_part_key, e,
            )
            return False

        read_mode = "r:gz" if data_type == "metadata" else "r"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            merged_tar_temp = tmp.name
        try:
            with tarfile.open(old_tar_temp, read_mode) as old_tar, \
                 tarfile.open(merged_tar_temp, tar_mode) as new_tar:
                for member in old_tar:
                    if member.isfile():
                        extracted = old_tar.extractfile(member)
                        new_tar.addfile(member, extracted)
                    else:
                        new_tar.addfile(member)
                for pf in tqdm(part_files, desc=f"Appending to {target.name}"):
                    new_tar.add(pf, arcname=Path(pf).name)
        except Exception as e:
            logger.warning(
                "Append: failed to build merged tar for %s (%s); falling through",
                old_part_key, e,
            )
            return False

        merged_size = Path(merged_tar_temp).stat().st_size
        if merged_size > MAX_TAR_SIZE_BYTES:
            logger.warning(
                "Append: merged tar size %s exceeds split ceiling %s; falling through",
                format_size(merged_size), format_size(MAX_TAR_SIZE_BYTES),
            )
            return False

        new_part_base = generate_part_name(utc_now_iso())
        new_part_file_name = f"{new_part_base}{suffix}"
        # Never reuse the old part's filename — protects against an S3 overwrite
        # racing with the index swap on sub-second collisions.
        if new_part_file_name == target.name:
            new_part_file_name = f"{new_part_base}-append{suffix}"
        new_part_key = parts_prefix + new_part_file_name

        print(
            f"  Appending to existing part {target.name} "
            f"({format_size(target.size)} + {len(part_files)} new files "
            f"→ {format_size(merged_size)}); uploading as {new_part_file_name}"
        )
        upload_ok = upload_large_file_to_s3(
            merged_tar_temp, S3_BUCKET, new_part_key, content_type
        )
        if not upload_ok:
            logger.warning(
                "Append: upload of merged tar %s failed; falling through to new-part",
                new_part_key,
            )
            return False

        try:
            update_index_file(
                data_type, year, court_code, bench,
                part_files, new_part_file_name, merged_size,
                replace_part_name=target.name,
            )
        except Exception as e:
            logger.error(
                "Append: index swap failed after merged-tar upload (%s). "
                "Merged tar s3://%s/%s is now an orphan; old part %s remains "
                "authoritative per the index.",
                e, S3_BUCKET, new_part_key, old_part_key,
            )
            return False

        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=old_part_key)
        except Exception as e:
            logger.warning(
                "Append: failed to delete old part s3://%s/%s after successful merge (%s). "
                "Orphan; clean up via audit.",
                S3_BUCKET, old_part_key, e,
            )

        return True
    finally:
        if old_tar_temp:
            Path(old_tar_temp).unlink(missing_ok=True)
        if merged_tar_temp:
            Path(merged_tar_temp).unlink(missing_ok=True)


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

        After dedup, the latest tar part may be eligible for an in-place append
        (small enough, and total merged size fits under the split ceiling). If
        so, we download it, stream-merge the new files into a freshly named
        tar, upload, swap the index, and delete the old part. Any failure in
        that path falls through to the normal new-part creation below.
        """
        nonlocal parts_uploaded

        # Pre-filter against the S3 index to avoid building a tar for files
        # that are already indexed. This prevents orphan tar creation when
        # there's any overlap (from concurrent writers, re-uploads, etc.).
        index_key = (
            get_metadata_index_key(year, court_code, bench)
            if data_type == "metadata"
            else get_data_index_key(year, court_code, bench)
        )
        index_data = _load_index_v2(index_key)
        already_indexed = set()
        for _p in index_data.parts:
            already_indexed.update(_p.files)

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

        # Try the hybrid append path: if the latest indexed part is small
        # enough and the merged size stays under the split ceiling, re-upload
        # a merged tar instead of fragmenting with yet another tiny part.
        new_files_total_size = 0
        for pf in part_files:
            try:
                new_files_total_size += Path(pf).stat().st_size
            except OSError:
                pass

        append_target = _find_append_target(index_data, new_files_total_size)
        if append_target is not None:
            appended = _append_to_existing_part(
                data_type, year, court_code, bench,
                part_files, append_target,
                parts_prefix, suffix, tar_mode, content_type,
            )
            if appended:
                parts_uploaded += 1
                return
            # else fall through to new-part creation

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
            upload_ok = upload_large_file_to_s3(temp_path, S3_BUCKET, part_key, content_type)
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
                    Bucket=S3_BUCKET, Key=parquet_key)

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
                combined_data = pd.concat(
                    [existing_data, new_data], ignore_index=True
                )
                combined_data = dedupe_parquet_records(combined_data)
                sibling_keys = _load_existing_parquet_keys_for_court_year(
                    year, court_code, bench
                )
                combined_data = filter_parquet_records_against_keys(
                    combined_data, sibling_keys
                )

                print(
                    f"Combined parquet: {len(existing_data)} existing + {len(new_data)} new = {len(combined_data)} total records"
                )
            else:
                combined_data = dedupe_parquet_records(new_data)
                sibling_keys = _load_existing_parquet_keys_for_court_year(
                    year, court_code, bench
                )
                combined_data = filter_parquet_records_against_keys(
                    combined_data, sibling_keys
                )
                print(f"New parquet file with {len(combined_data)} records")

            # Save combined data to final parquet file
            combined_data.to_parquet(combined_parquet_path, index=False)

            # Upload combined parquet file to S3
            print(f"  Uploading updated parquet file...")
            with open(combined_parquet_path, "rb") as f:
                s3_client.put_object(
                    Bucket=S3_BUCKET,
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
