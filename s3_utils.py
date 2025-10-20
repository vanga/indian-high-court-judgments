"""S3 utility functions for uploading, downloading, and managing index files."""

import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import json

from models import IndexFileV2, IndexPart

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore import UNSIGNED
    from botocore.client import Config

    S3_AVAILABLE = True
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
from file_utils import group_files_by_year_and_bench, cleanup_uploaded_files
from tqdm import tqdm

S3_READ_BUCKET = "indian-high-court-judgments"
S3_WRITE_BUCKET = "indian-high-court-judgments-test"


def is_gzipped_tar(file_path: str) -> bool:
    """Check if a tar file is gzipped/compressed by examining the file header."""
    try:
        with open(file_path, "rb") as f:
            # Read the first 2 bytes to check for gzip magic number
            header = f.read(2)
            # Gzip files start with 0x1f 0x8b
            return header == b"\x1f\x8b"
    except Exception:
        return False


def extract_gzipped_tar(gzipped_tar_path: str, output_tar_path: str) -> bool:
    """Extract a gzipped tar file to an uncompressed tar file."""
    try:
        import gzip

        with gzip.open(gzipped_tar_path, "rb") as gz_file:
            with open(output_tar_path, "wb") as tar_file:
                shutil.copyfileobj(gz_file, tar_file)
        return True
    except Exception as e:
        print(f"Error extracting gzipped tar: {e}")
        return False


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


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _generate_part_name(now_iso: str) -> str:
    # Use compact timestamp: YYYYMMDDThhmmssZ
    ts = datetime.fromisoformat(now_iso.replace("Z", "+00:00")).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    return f"part-{ts}.tar"


def _format_human_size_from_bytes(size_bytes: int) -> str:
    return format_size(size_bytes)


def _load_index_v2(
    s3_unsigned,
    read_bucket: str,
    index_key: str,
    file_type: str,
    court_code: str,
    bench: str,
    year: int,
) -> IndexFileV2:
    """Load index file (V2). If missing, return empty V2."""
    try:
        resp = s3_unsigned.get_object(Bucket=read_bucket, Key=index_key)
        raw = resp["Body"].read().decode("utf-8")
        data = json.loads(raw)
        return IndexFileV2.model_validate(data)
    except Exception:
        # No existing index -> create empty V2
        now_iso = _utc_now_iso()
        return IndexFileV2(
            file_count=0,
            tar_size=0,
            tar_size_human="0 B",
            updated_at=now_iso,
            parts=[],
        )


def get_court_dates_from_index_files(year=None):
    """
    Get updated_at dates from data index files

    Args:
        year: Year to check (defaults to current year)

    Returns:
        Dict of {court_code: {bench_name: updated_at_timestamp}}
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    year = year or datetime.now().year
    prefix = f"data/tar/year={year}/"

    print(f"Reading dates from data index files: {S3_READ_BUCKET}/{prefix}")

    result = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_READ_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("data.index.json"):
                continue

            court_code, bench = extract_court_bench_from_path(key)
            if not court_code or not bench:
                continue

            updated_at = read_updated_at_from_index(s3, S3_READ_BUCKET, key)
            if updated_at:
                if court_code not in result:
                    result[court_code] = {}
                result[court_code][bench] = updated_at

    print(f"Found dates for {len(result)} courts")
    return result


def get_existing_files_from_s3(court_code, benches, year=None):
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

        metadata_key = f"metadata/tar/year={year}/court={court_code}/bench={bench_name}/metadata.index.json"
        try:
            response = s3.get_object(Bucket=S3_READ_BUCKET, Key=metadata_key)
            raw = response["Body"].read().decode("utf-8")
            data = json.loads(raw)
            files_iter = []
            for part in data.get("parts", []):
                files_iter.extend(part.get("files", []))

            for filename in files_iter:
                if filename:
                    base_name = filename.replace(".json", "")
                    existing_files[bench_name]["metadata"].add(base_name)
        except Exception:
            pass

        data_key = f"data/tar/year={year}/court={court_code}/bench={bench_name}/data.index.json"
        try:
            response = s3.get_object(Bucket=S3_READ_BUCKET, Key=data_key)
            raw = response["Body"].read().decode("utf-8")
            data = json.loads(raw)
            files_iter = []
            for part in data.get("parts", []):
                files_iter.extend(part.get("files", []))

            for filename in files_iter:
                if filename:
                    existing_files[bench_name]["data"].add(filename)
        except Exception:
            pass

    total_metadata = sum(len(bench["metadata"]) for bench in existing_files.values())
    total_data = sum(len(bench["data"]) for bench in existing_files.values())
    print(
        f"  Found {total_metadata} existing metadata files and {total_data} existing PDFs in S3"
    )

    return existing_files


def update_index_files_after_download(court_code, bench, new_files, to_date=None):
    """Update index files after download.

    Parts-based (V2) index updates occur during tar part creation for all years.
    This function is a no-op and preserved for compatibility with call sites.
    """
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return

    s3_client = boto3.client("s3")
    s3_unsigned = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    return


def upload_large_file_to_s3(
    s3_client,
    file_path: str,
    bucket: str,
    key: str,
    content_type: str = "application/octet-stream",
    multipart_threshold: int = 5 * 1024 * 1024 * 1024,  # 5GB
    multipart_chunksize: int = 100 * 1024 * 1024,  # 100MB
) -> bool:
    """
    Upload a large file to S3 using boto3's built-in multipart handling.

    Args:
        s3_client: boto3 S3 client
        file_path: Local file path to upload
        bucket: S3 bucket name
        key: S3 object key
        content_type: MIME type of the file
        multipart_threshold: Size threshold for multipart upload (default 5GB)
        multipart_chunksize: Size of each part in bytes (default 100MB)
    """
    file_path_obj = Path(file_path)
    file_size = file_path_obj.stat().st_size
    print(f"  File size: {format_size(file_size)}")

    try:
        # Configure transfer settings
        transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            multipart_chunksize=multipart_chunksize,
            use_threads=True,  # Enable parallel uploads for better performance
            max_concurrency=10,  # Number of threads to use
        )

        # Use boto3's high-level upload_file method
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
        print(f"  Upload failed: {e}")
        return False


def upload_single_file_to_s3(
    s3_client,
    local_file_path: str | Path,
    court_code: str,
    bench: str,
    year: int,
    file_type: str,
) -> bool:
    """Upload a single file to S3 (without print statements for use with progress bar)"""
    try:
        # Convert to Path object for consistent handling
        file_path_obj = Path(local_file_path)
        filename = file_path_obj.name

        if file_type == "metadata":
            s3_key = (
                f"metadata/json/year={year}/court={court_code}/bench={bench}/{filename}"
            )
        else:
            s3_key = f"data/pdf/year={year}/court={court_code}/bench={bench}/{filename}"

        # Upload the file
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

    except Exception as e:
        print(f"Failed to upload {local_file_path}, remote path: {s3_key}: {e}")
        return False


def upload_files_to_s3(
    court_code: str, downloaded_files: Dict[str, List[Path]]
) -> Dict[str, bool]:
    """Upload downloaded files to S3 bucket with progress bars. Returns dict of bench -> success status"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available for upload")
        return {}

    if not downloaded_files["metadata"] and not downloaded_files["data"]:
        print(f"No files to upload for court {court_code}")
        return {}  # Empty dict means no benches to upload

    s3_client = boto3.client("s3")

    print(f"Starting S3 upload for court {court_code}")
    print(
        f"Total files: {len(downloaded_files['metadata'])} metadata, {len(downloaded_files['data'])} data files"
    )

    # Group files by year (from decision date) and bench
    files_by_year_bench = group_files_by_year_and_bench(downloaded_files)

    if not files_by_year_bench:
        print("Warning: No files could be grouped for upload")
        return {}

    # Show organization summary
    print(f"\nFiles organized into {len(files_by_year_bench)} year partition(s):")
    for year in sorted(files_by_year_bench.keys()):
        benches = files_by_year_bench[year]
        total_metadata = sum(len(b["metadata"]) for b in benches.values())
        total_data = sum(len(b["data"]) for b in benches.values())
        print(
            f"  Year {year}: {len(benches)} bench(es), {total_metadata} metadata, {total_data} data files"
        )
        for bench_name in sorted(benches.keys()):
            files = benches[bench_name]
            print(
                f"    - {bench_name}: {len(files['metadata'])} metadata, {len(files['data'])} data"
            )

    bench_upload_status = {}  # Track success per bench (across all years)

    # Convert court code to S3 format
    s3_court_code = court_code.replace("~", "_")

    # Process each year partition separately
    for year in sorted(files_by_year_bench.keys()):
        print(f"\n{'=' * 60}")
        print(f"Processing Year Partition: {year}")
        print(f"{'=' * 60}")

        bench_files = files_by_year_bench[year]

        # Upload files by bench within this year
        for bench, files in bench_files.items():
            print(f"\nProcessing {year}/{bench}")

            bench_success = True  # Track this bench's upload success

            # Upload metadata files with progress bar
            if files["metadata"]:
                print(f"Uploading {len(files['metadata'])} JSON metadata files...")
                with tqdm(
                    total=len(files["metadata"]), desc="JSON files", unit="file"
                ) as pbar:
                    for metadata_file in files["metadata"]:
                        success = upload_single_file_to_s3(
                            s3_client,
                            metadata_file,
                            s3_court_code,
                            bench,
                            year,
                            "metadata",
                        )
                        if not success:
                            bench_success = False
                        pbar.update(1)

            # Upload data files with progress bar
            if files["data"]:
                print(f"Uploading {len(files['data'])} PDF data files...")
                with tqdm(
                    total=len(files["data"]), desc="PDF files", unit="file"
                ) as pbar:
                    for data_file in files["data"]:
                        success = upload_single_file_to_s3(
                            s3_client, data_file, s3_court_code, bench, year, "data"
                        )
                        if not success:
                            bench_success = False
                        pbar.update(1)

            print(f"Completed individual file upload for {year}/{bench}")

            # Create and upload tar files for this bench/year
            tar_success = create_and_upload_tar_files(
                s3_client, s3_court_code, bench, year, files
            )
            if not tar_success:
                bench_success = False

            # Create and upload parquet files for this bench/year
            parquet_success = create_and_upload_parquet_files(
                s3_client, s3_court_code, bench, year, files
            )
            if not parquet_success:
                bench_success = False

            # Store this bench's upload status (use bench as key, will overwrite if same bench in multiple years)
            # Actually, let's track by year+bench to be more accurate
            bench_key = f"{year}/{bench}"
            bench_upload_status[bench_key] = bench_success

            if bench_success:
                print(f"  ✓ Successfully uploaded all files for {year}/{bench}")

                # Clean up local files after successful upload
                print(f"  Cleaning up local files for {year}/{bench}...")
                cleanup_uploaded_files(files)
            else:
                print(f"  ✗ Some files failed to upload for {year}/{bench}")
                print(f"  Keeping local files due to upload failures")

    # Print overall summary
    successful_benches = [b for b, success in bench_upload_status.items() if success]
    failed_benches = [b for b, success in bench_upload_status.items() if not success]

    if successful_benches:
        print(
            f"S3 upload completed successfully for benches: {', '.join(successful_benches)}"
        )
    if failed_benches:
        print(f"S3 upload had failures for benches: {', '.join(failed_benches)}")

    return bench_upload_status


def create_and_upload_tar_files(
    s3_client, court_code: str, bench: str, year: int, files: Dict[str, List[Path]]
) -> bool:
    """Create/upload tar part files for all years and update V2 indexes."""

    print(f"Creating/updating tar files for bench {bench}")

    overall_success = True  # Track success for both metadata and data tar files

    now_iso = _utc_now_iso()
    s3_unsigned = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # PARTS MODE (all years)
    # Handle metadata part
    if files["metadata"]:
        parts_prefix = f"metadata/tar/year={year}/court={court_code}/bench={bench}/"
        part_name = _generate_part_name(now_iso)
        part_key = parts_prefix + part_name

        print(f"  Creating metadata part: {part_name}")
        with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as temp_tar:
            with tarfile.open(temp_tar.name, "w") as tar:
                added = 0
                seen = set()
                with tqdm(
                    total=len(files["metadata"]),
                    desc="Creating metadata part",
                    unit="file",
                ) as pbar:
                    for metadata_file in files["metadata"]:
                        arcname = Path(metadata_file).name
                        if arcname in seen:
                            pbar.update(1)
                            continue
                        seen.add(arcname)
                        tar.add(metadata_file, arcname=arcname)
                        added += 1
                        pbar.set_postfix_str(f"Added {added}")
                        pbar.update(1)

        # Upload part
        print(f"  Uploading metadata part {part_name}...")
        success = upload_large_file_to_s3(
            s3_client,
            temp_tar.name,
            S3_WRITE_BUCKET,
            part_key,
            "application/x-tar",
        )
        size_bytes = Path(temp_tar.name).stat().st_size
        Path(temp_tar.name).unlink()
        if not success:
            print("  Failed to upload metadata part")
            overall_success = False
        else:
            print("  Uploaded metadata part")
            # Update index V2
            index_key = f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.index.json"
            index_v2 = _load_index_v2(
                s3_unsigned,
                S3_READ_BUCKET,
                index_key,
                "metadata",
                court_code,
                bench,
                year,
            )
            new_part = IndexPart(
                name=part_name,
                files=[Path(p).name for p in files["metadata"]],
                size=size_bytes,
                size_human=_format_human_size_from_bytes(size_bytes),
                created_at=now_iso,
            )
            index_v2.parts.append(new_part)
            # Recompute aggregates
            index_v2.file_count = sum(p.file_count for p in index_v2.parts)
            index_v2.tar_size = sum(p.size for p in index_v2.parts)
            index_v2.tar_size_human = _format_human_size_from_bytes(index_v2.tar_size)
            index_v2.updated_at = now_iso

            s3_client.put_object(
                Bucket=S3_WRITE_BUCKET,
                Key=index_key,
                Body=index_v2.model_dump_json(indent=2),
                ContentType="application/json",
            )

    # Handle data part
    if files["data"]:
        parts_prefix = f"data/tar/year={year}/court={court_code}/bench={bench}/"
        part_name = _generate_part_name(now_iso)
        part_key = parts_prefix + part_name

        print(f"  Creating data part: {part_name}")
        with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as temp_tar:
            with tarfile.open(temp_tar.name, "w") as tar:
                added = 0
                seen = set()
                with tqdm(
                    total=len(files["data"]), desc="Creating data part", unit="file"
                ) as pbar:
                    for data_file in files["data"]:
                        arcname = Path(data_file).name
                        if arcname in seen:
                            pbar.update(1)
                            continue
                        seen.add(arcname)
                        tar.add(data_file, arcname=arcname)
                        added += 1
                        pbar.set_postfix_str(f"Added {added}")
                        pbar.update(1)

        # Upload part
        print(f"  Uploading data part {part_name}...")
        success = upload_large_file_to_s3(
            s3_client,
            temp_tar.name,
            S3_WRITE_BUCKET,
            part_key,
            "application/x-tar",
        )
        size_bytes = Path(temp_tar.name).stat().st_size
        Path(temp_tar.name).unlink()
        if not success:
            print("  Failed to upload data part")
            overall_success = False
        else:
            print("  Uploaded data part")
            # Update index V2
            index_key = (
                f"data/tar/year={year}/court={court_code}/bench={bench}/data.index.json"
            )
            index_v2 = _load_index_v2(
                s3_unsigned,
                S3_READ_BUCKET,
                index_key,
                "data",
                court_code,
                bench,
                year,
            )
            new_part = IndexPart(
                name=part_name,
                files=[Path(p).name for p in files["data"]],
                size=size_bytes,
                size_human=_format_human_size_from_bytes(size_bytes),
                created_at=now_iso,
            )
            index_v2.parts.append(new_part)
            # Recompute aggregates
            index_v2.file_count = sum(p.file_count for p in index_v2.parts)
            index_v2.tar_size = sum(p.size for p in index_v2.parts)
            index_v2.tar_size_human = _format_human_size_from_bytes(index_v2.tar_size)
            index_v2.updated_at = now_iso

            s3_client.put_object(
                Bucket=S3_WRITE_BUCKET,
                Key=index_key,
                Body=index_v2.model_dump_json(indent=2),
                ContentType="application/json",
            )

    return overall_success


def create_and_upload_parquet_files(
    s3_client, court_code: str, bench: str, year: int, files: Dict[str, List[Path]]
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
                response = s3_client.get_object(Bucket=S3_READ_BUCKET, Key=parquet_key)

                # Save existing parquet to temp file
                with open(existing_parquet_path, "wb") as f:
                    f.write(response["Body"].read())

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
                combined_data = pd.concat([existing_data, new_data], ignore_index=True)

                # Remove duplicates if pdf_link column exists
                if "pdf_link" in combined_data.columns:
                    before_dedup = len(combined_data)
                    combined_data = combined_data.drop_duplicates(
                        subset=["pdf_link"], keep="last"
                    )
                    after_dedup = len(combined_data)
                    duplicates_removed = before_dedup - after_dedup
                    if duplicates_removed > 0:
                        print(f"Removed {duplicates_removed} duplicate records")
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
