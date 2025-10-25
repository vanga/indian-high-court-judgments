"""
Shared utility functions to eliminate code duplication across the codebase.

This module consolidates common functions that were duplicated across multiple files.
"""

import gzip
import shutil
from datetime import datetime, timezone
from pathlib import Path


def format_size(size_bytes: int) -> str:
    """
    Format bytes into human readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human readable size string (e.g., "1.5 GB", "500 KB")
    """
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


def utc_now_iso() -> str:
    """
    Get current UTC time in ISO format.

    Returns:
        ISO format datetime string with Z suffix (e.g., "2025-01-27T10:30:45Z")
    """
    return (
        datetime.now(timezone.utc)
        .replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def is_gzipped_tar(file_path: Path) -> bool:
    """
    Check if a tar file is gzipped/compressed by examining the file header.

    Args:
        file_path: Path to the tar file

    Returns:
        True if the file is gzipped, False otherwise
    """
    try:
        with open(file_path, "rb") as f:
            # Read the first 2 bytes to check for gzip magic number
            header = f.read(2)
            # Gzip files start with 0x1f 0x8b
            return header == b"\x1f\x8b"
    except Exception:
        return False


def extract_gzipped_tar(gzipped_tar_path: Path, output_tar_path: Path) -> bool:
    """
    Extract a gzipped tar file to an uncompressed tar file.

    Args:
        gzipped_tar_path: Path to the gzipped tar file
        output_tar_path: Path where the uncompressed tar file should be written

    Returns:
        True if extraction was successful, False otherwise
    """
    try:
        with gzip.open(gzipped_tar_path, "rb") as gz_file:
            with open(output_tar_path, "wb") as tar_file:
                shutil.copyfileobj(gz_file, tar_file)
        return True
    except Exception as e:
        print(f"Error extracting gzipped tar: {e}")
        return False


def generate_part_name(now_iso: str) -> str:
    # Use compact timestamp: YYYYMMDDThhmmssZ
    ts = datetime.fromisoformat(now_iso.replace("Z", "+00:00")).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    return f"part-{ts}"


def get_bench_partition_key(data_type: str, storage_type: str, year: int, court_code: str, bench: str) -> str:
    return f"{data_type}/{storage_type}/year={year}/court={court_code}/bench={bench}/"


def get_metadata_index_key(year: int, court_code: str, bench: str) -> str:
    return f"{get_bench_partition_key('metadata', 'tar', year, court_code, bench)}metadata.index.json"


def get_data_index_key(year: int, court_code: str, bench: str) -> str:
    return f"{get_bench_partition_key('data', 'tar', year, court_code, bench)}data.index.json"
