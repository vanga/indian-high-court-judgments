"""S3 utility functions for uploading, downloading, and managing index files."""

import shutil
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from models import IndexFile

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
    """Get updated_at timestamp from index file"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        index_file = IndexFile.model_validate_json(
            response["Body"].read().decode("utf-8")
        )
        return index_file.updated_at
    except Exception as e:
        print(f"Warning: Failed to read {key}: {e}")
        return None


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
            index_file = IndexFile.model_validate_json(
                response["Body"].read().decode("utf-8")
            )

            for filename in index_file.files:
                if filename:
                    base_name = filename.replace(".json", "")
                    existing_files[bench_name]["metadata"].add(base_name)
        except Exception:
            pass

        data_key = f"data/tar/year={year}/court={court_code}/bench={bench_name}/data.index.json"
        try:
            response = s3.get_object(Bucket=S3_READ_BUCKET, Key=data_key)
            index_file = IndexFile.model_validate_json(
                response["Body"].read().decode("utf-8")
            )

            for filename in index_file.files:
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
    """Update both metadata and data index files with new download information"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return

    s3_client = boto3.client("s3")
    s3_unsigned = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    if to_date:
        if isinstance(to_date, str):
            to_date_dt = datetime.strptime(to_date, "%Y-%m-%d")
        else:
            to_date_dt = (
                to_date
                if hasattr(to_date, "hour")
                else datetime.combine(to_date, datetime.min.time())
            )
        update_time = to_date_dt.isoformat()
        year = to_date_dt.year
    else:
        current_time = datetime.now()
        update_time = current_time.isoformat()
        year = current_time.year

    updates = [
        {
            "type": "metadata",
            "key": f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.index.json",
            "tar_key": f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz",
            "files": new_files.get("metadata", []),
        },
        {
            "type": "data",
            "key": f"data/tar/year={year}/court={court_code}/bench={bench}/data.index.json",
            "tar_key": f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar",
            "files": new_files.get("data", []),
        },
    ]

    for update in updates:
        if not update["files"]:
            continue

        try:
            try:
                response = s3_unsigned.get_object(
                    Bucket=S3_READ_BUCKET, Key=update["key"]
                )
                index_file = IndexFile.model_validate_json(
                    response["Body"].read().decode("utf-8")
                )
            except Exception:
                index_file = IndexFile(
                    files=[],
                    file_count=0,
                    tar_size=0,
                    tar_size_human="0 B",
                    updated_at=update_time,
                )

            existing_files = set(index_file.files)
            for new_file in update["files"]:
                filename = Path(new_file).name
                if filename not in existing_files:
                    index_file.files.append(filename)

            index_file.file_count = len(index_file.files)
            index_file.updated_at = update_time

            try:
                tar_response = s3_unsigned.head_object(
                    Bucket=S3_READ_BUCKET, Key=update["tar_key"]
                )
                tar_size = tar_response["ContentLength"]
                index_file.tar_size = tar_size
                index_file.tar_size_human = format_size(tar_size)
            except Exception as e:
                print(f"Warning: Could not get tar size for {update['tar_key']}: {e}")
                index_file.tar_size = 0
                index_file.tar_size_human = "0 B"

            s3_client.put_object(
                Bucket=S3_WRITE_BUCKET,
                Key=update["key"],
                Body=index_file.model_dump_json(indent=2),
                ContentType="application/json",
            )
            print(f"Updated {update['type']} index with {len(update['files'])} files")

        except Exception as e:
            print(f"Failed to update {update['type']} index: {e}")


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
    """Download existing tar files, append new content, and upload back to S3"""

    print(f"Creating/updating tar files for bench {bench}")

    overall_success = True  # Track success for both metadata and data tar files

    # Handle metadata tar file
    if files["metadata"]:
        metadata_tar_key = (
            f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz"
        )

        # Try to download existing tar file
        existing_files_set = set()
        temp_existing_tar = None

        try:
            # Get file size first
            head_response = s3_client.head_object(
                Bucket=S3_READ_BUCKET, Key=metadata_tar_key
            )
            file_size = head_response["ContentLength"]
            file_size_human = format_size(file_size)

            print(f"  Downloading existing metadata tar ({file_size_human})...")

            # Create temp file on disk
            temp_existing_tar = tempfile.NamedTemporaryFile(
                suffix=".tar.gz", delete=False
            )

            # For smaller files, can download directly, for larger ones, stream
            if file_size > 100 * 1024 * 1024:  # If larger than 100MB, stream it
                response = s3_client.get_object(
                    Bucket=S3_READ_BUCKET, Key=metadata_tar_key
                )
                chunk_size = 64 * 1024 * 1024  # 64MB chunks
                downloaded = 0

                for chunk in iter(lambda: response["Body"].read(chunk_size), b""):
                    temp_existing_tar.write(chunk)
                    downloaded += len(chunk)
                    progress = (downloaded / file_size) * 100
                    print(
                        f"\r  Progress: {progress:.1f}% ({format_size(downloaded)}/{file_size_human})",
                        end="",
                        flush=True,
                    )
                print()  # New line
            else:
                # For smaller files, download normally
                response = s3_client.get_object(
                    Bucket=S3_READ_BUCKET, Key=metadata_tar_key
                )
                temp_existing_tar.write(response["Body"].read())

            temp_existing_tar.close()

            # Read existing files list to avoid duplicates
            with tarfile.open(temp_existing_tar.name, "r:gz") as existing_tar:
                existing_files_set = set(existing_tar.getnames())
                print(
                    f"  Found existing metadata tar with {len(existing_files_set)} files"
                )

        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing metadata tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing metadata tar: {e}")

        # Create new tar file with both existing and new content
        with tempfile.NamedTemporaryFile(
            suffix=".tar.gz", delete=False
        ) as temp_new_tar:
            with tarfile.open(temp_new_tar.name, "w:gz") as new_tar:
                # First, add existing files if we have them
                if temp_existing_tar and Path(temp_existing_tar.name).exists():
                    try:
                        print(f"  Merging existing metadata tar content...")
                        with tarfile.open(
                            temp_existing_tar.name, "r:gz"
                        ) as existing_tar:
                            for member in existing_tar.getmembers():
                                file_obj = existing_tar.extractfile(member)
                                if file_obj:
                                    new_tar.addfile(member, file_obj)
                    except Exception as e:
                        print(f"  Warning: Could not read existing tar content: {e}")

                # Then add new files (skip duplicates)
                new_files_added = 0
                with tqdm(
                    total=len(files["metadata"]),
                    desc="Adding to metadata tar",
                    unit="file",
                ) as pbar:
                    for metadata_file in files["metadata"]:
                        arcname = Path(metadata_file).name
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
            S3_WRITE_BUCKET,
            metadata_tar_key,
            "application/gzip",
        )

        # Clean up temp files
        Path(temp_new_tar.name).unlink()
        if temp_existing_tar and Path(temp_existing_tar.name).exists():
            Path(temp_existing_tar.name).unlink()

        if success:
            print(f"  Successfully uploaded updated metadata tar")
        else:
            print(f"  Failed to upload metadata tar")
            overall_success = False

    # Handle data/PDF tar file
    if files["data"]:
        data_tar_key = f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar"

        # Check existing tar file size first
        existing_files_set = set()
        temp_existing_tar = None

        try:
            # Get file metadata to check size
            head_response = s3_client.head_object(
                Bucket=S3_READ_BUCKET, Key=data_tar_key
            )
            file_size = head_response["ContentLength"]
            file_size_human = format_size(file_size)

            print(f"  Existing data tar size: {file_size_human}")
            print(f"  Downloading existing data tar to disk (streaming)...")

            # Create temp file for existing tar (on disk, not in RAM)
            temp_existing_tar = tempfile.NamedTemporaryFile(suffix=".tar", delete=False)

            # Stream download in chunks to avoid loading entire file into RAM
            response = s3_client.get_object(Bucket=S3_READ_BUCKET, Key=data_tar_key)
            chunk_size = (
                256 * 1024 * 1024
            )  # 256MB chunks (optimized for faster downloads)
            downloaded = 0

            print(
                f"  Downloading {file_size_human} in {chunk_size // (1024 * 1024)}MB chunks..."
            )
            for chunk in iter(lambda: response["Body"].read(chunk_size), b""):
                temp_existing_tar.write(chunk)
                downloaded += len(chunk)
                progress = (downloaded / file_size) * 100
                print(
                    f"\r  Progress: {progress:.1f}% ({format_size(downloaded)}/{file_size_human})",
                    end="",
                    flush=True,
                )

            print()  # New line after progress
            temp_existing_tar.close()

            # Check if the downloaded tar is gzipped
            if is_gzipped_tar(temp_existing_tar.name):
                print(
                    f"  Detected gzipped tar file, extracting to uncompressed format..."
                )
                # Create a new temp file for the uncompressed tar
                temp_uncompressed_tar = tempfile.NamedTemporaryFile(
                    suffix=".tar", delete=False
                )
                temp_uncompressed_tar.close()

                if extract_gzipped_tar(
                    temp_existing_tar.name, temp_uncompressed_tar.name
                ):
                    # Replace the gzipped file with the uncompressed one
                    Path(temp_existing_tar.name).unlink()
                    shutil.move(temp_uncompressed_tar.name, temp_existing_tar.name)
                    print(
                        f"  Successfully extracted gzipped tar to uncompressed format"
                    )
                else:
                    print(f"  Failed to extract gzipped tar, will create new tar")
                    Path(temp_existing_tar.name).unlink()
                    temp_existing_tar = None
            else:
                print(f"  Tar file is not gzipped, proceeding with append mode")

            # Now read the tar file list (this is fast, just reads the index)
            if temp_existing_tar and Path(temp_existing_tar.name).exists():
                print(f"  Reading existing tar file index...")
                with tarfile.open(temp_existing_tar.name, "r") as existing_tar:
                    existing_files_set = set(existing_tar.getnames())
                    print(
                        f"  Found existing data tar with {len(existing_files_set)} files"
                    )

        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing data tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing data tar: {e}")

        # Use APPEND mode for existing tar or CREATE mode for new tar
        if temp_existing_tar and Path(temp_existing_tar.name).exists():
            # APPEND MODE - only adds new files to end of existing tar
            print(
                f"  Using APPEND mode to add new files to existing tar, temp file: {temp_existing_tar}"
            )
            with tarfile.open(temp_existing_tar.name, "a") as tar:  # 'a' = APPEND mode
                # Get existing files list (fast - just reads index)
                existing_files = set(tar.getnames())
                print(f"  Existing tar contains {len(existing_files)} files")

                # Append only new files to the end
                new_files_added = 0
                with tqdm(
                    total=len(files["data"]), desc="Appending to data tar", unit="file"
                ) as pbar:
                    for data_file in files["data"]:
                        arcname = Path(data_file).name
                        if arcname not in existing_files:
                            tar.add(data_file, arcname=arcname)  # Appends to END of tar
                            new_files_added += 1
                            pbar.set_postfix_str(f"Appended {new_files_added}")
                        else:
                            pbar.set_postfix_str(f"Skipped duplicate")
                        pbar.update(1)

                print(f"  Appended {new_files_added} new files to existing tar")

            # Upload the modified tar (same file, with new data appended)
            print(f"  Uploading updated data tar (uncompressed)...")
            success = upload_large_file_to_s3(
                s3_client,
                temp_existing_tar.name,  # Upload the appended tar
                S3_WRITE_BUCKET,
                data_tar_key,
                "application/x-tar",  # Uncompressed tar content type
            )

            # Clean up temp file
            Path(temp_existing_tar.name).unlink()

        else:
            # CREATE MODE - no existing tar, create new one
            print(f"  Creating new data tar file...")
            with tempfile.NamedTemporaryFile(
                suffix=".tar", delete=False
            ) as temp_new_tar:
                with tarfile.open(temp_new_tar.name, "w") as tar:  # 'w' = CREATE mode
                    new_files_added = 0
                    with tqdm(
                        total=len(files["data"]), desc="Creating data tar", unit="file"
                    ) as pbar:
                        for data_file in files["data"]:
                            arcname = Path(data_file).name
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
                S3_WRITE_BUCKET,
                data_tar_key,
                "application/x-tar",
            )

            # Clean up temp file
            Path(temp_new_tar.name).unlink()

        if success:
            print(f"  Successfully uploaded updated data tar")
        else:
            print(f"  Failed to upload data tar")
            overall_success = False

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
