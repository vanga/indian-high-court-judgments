"""
PDF compression utility using Ghostscript.
"""

import traceback
import shutil
import os
import sys
import argparse
from pathlib import Path
import concurrent.futures
from tqdm import tqdm
import traceback
import csv
from datetime import datetime
import tarfile
import tempfile

INPUT_DIR = Path("/home/ubuntu/data")
OUTPUT_DIR = Path("/home/ubuntu/data/data/tar-compressed")
ERROR_LOG = OUTPUT_DIR / "error_log.txt"
HIGH_COURTS_CSV = Path(
    "/home/ubuntu/indian-high-court-judgments/opendata/docs/high_courts.csv")


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
error_file = open(ERROR_LOG, "a")


def compress_pdf(input_path, output_path, compression_level="screen"):
    try:
        """
            Compress a PDF file using Ghostscript.

        Args:
            input_path: Path to the input PDF file
            output_path: Path where the compressed PDF will be saved
            compression_level: Compression level (screen, ebook, printer, prepress, or default)

        Returns:
            tuple: (success, message)
        """
        # Validate compression level
        valid_levels = ["screen", "ebook", "printer", "prepress", "default"]
        if compression_level not in valid_levels:
            return (
                False,
                f"Invalid compression level. Choose from: {', '.join(valid_levels)}",
            )

        # Use full path to Ghostscript
        gs_path = "/usr/bin/gs"  # Update this path if needed

        # Construct Ghostscript command
        gs_command = (
            f"{gs_path} -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 "
            f"-dPDFSETTINGS=/{compression_level} -dNOPAUSE -dBATCH -dQUIET "
            f"-sOutputFile='{output_path}' '{input_path}'"
        )

        # Execute command first
        exit_code = os.system(gs_command)
        if exit_code != 0:
            error_msg = (
                f"Ghostscript failed with exit code {exit_code}\nCommand: {gs_command}"
            )
            log_error(input_path)
            return False, f"Ghostscript failed with exit code {exit_code}"

        # Now check file sizes after compression is done
        if os.path.exists(output_path):
            input_size = get_file_size_kb(input_path)
            output_size = get_file_size_kb(output_path)
            reduction = (1 - output_size / input_size) * \
                100 if input_size > 0 else 0
            if reduction <= 0:
                os.remove(output_path)
                shutil.copy(input_path, output_path)
                return True, "No reduction achieved, keeping original"
            return True, f"Compression successful ({reduction:.2f}% reduction)"
        # else:
        #     error_msg = "Output file was not created after Ghostscript execution"
        #     log_error(input_path)
        #     return False, "Output file was not created"

    except Exception as e:
        log_error(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)
        return False, f"Error during compression: {str(e)}"


def get_file_size_kb(file_path):
    """Return file size in KB."""
    return os.path.getsize(file_path) / 1024


def log_error(pdf_file):
    error_file.write(f"{pdf_file}\n")
    error_file.flush()  # Ensure the error is written immediately


def load_court_data():
    """Load court and bench data from high_courts.csv"""
    courts_data = []
    with open(HIGH_COURTS_CSV, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            courts_data.append({
                'court_name': row['court_name'],
                'court_code': row['court_code'],
                'bench_name': row['bench_name']
            })
    return courts_data


def get_year_range():
    """Get the range of years from 1950 to current year"""
    current_year = datetime.now().year
    return list(range(2025, current_year + 1))


def generate_processing_combinations():
    """Generate all combinations of year, court, and bench for processing"""
    courts_data = load_court_data()
    years = get_year_range()

    combinations = []
    for year in years:
        for court in courts_data:
            combinations.append({
                'year': year,
                'court_name': court['court_name'],
                'court_code': court['court_code'],
                'bench_name': court['bench_name']
            })

    return combinations


def compress_pdf_wrapper(combination):
    """Wrapper function for compress_pdf to use with concurrent.futures."""
    year = combination['year']
    court_name = combination['court_name']
    court_code = combination['court_code']
    bench_name = combination['bench_name']

    # Construct the expected directory path based on S3 partitioning structure
    # Structure: INPUT_DIR/data/tar/year=YYYY/court=XX_X/bench=name/
    data_tar_dir = INPUT_DIR / "data" / "tar" / \
        f"year={year}" / f"court={court_code}" / f"bench={bench_name}"

    if not data_tar_dir.exists():
        return  # Skip if directory doesn't exist

    # Find TAR files in the directory
    tar_files = list(data_tar_dir.glob("*.tar"))
    if not tar_files:
        return  # Skip if no TAR files found

    print(
        f"Processing {len(tar_files)} TAR files for {year}/{court_code}/{bench_name}")

    for tar_file in tar_files:
        try:
            process_tar_file(tar_file, year, court_name,
                             court_code, bench_name)
        except Exception as e:
            error_message = f"Error processing TAR {tar_file}: {str(e)}\n{traceback.format_exc()}"
            log_error(tar_file)
            print(f"Error processing TAR {tar_file}: {e}")


def process_tar_file(tar_file_path, year, court_name, court_code, bench_name):
    """Process a single TAR file: extract, compress PDFs, recreate TAR."""
    print(f"Processing TAR: {tar_file_path.name}")

    # Create temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        extract_dir = temp_path / "extracted"
        extract_dir.mkdir()

        # Extract TAR file
        print(f"  Extracting {tar_file_path.name}...")
        original_tar_size = tar_file_path.stat().st_size
        print(f"  Original TAR size: {original_tar_size:,} bytes")

        # Detect if original TAR is compressed
        is_compressed = tar_file_path.name.endswith('.gz')
        print(
            f"  TAR compression: {'gzip' if is_compressed else 'uncompressed'}")

        with tarfile.open(tar_file_path, 'r') as tar:
            tar.extractall(extract_dir)
            original_members = len(tar.getmembers())
            print(f"  Original TAR contains: {original_members} files")

        # Find all PDF files in extracted directory
        pdf_files = extract_dir.glob("**/*.pdf")

        # Process each PDF file
        compressed_count = 0
        total_original_size = 0
        total_compressed_size = 0

        for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
            try:
                original_size = pdf_file.stat().st_size
                total_original_size += original_size

                # Create temporary file for compressed version
                compressed_file = pdf_file.parent / \
                    f"{pdf_file.stem}_compressed.pdf"

                # Compress the PDF
                success, message = compress_pdf(pdf_file, compressed_file)

                if success and compressed_file.exists():
                    compressed_size = compressed_file.stat().st_size

                    # Only replace if compressed version is smaller
                    if compressed_size < original_size:
                        # Replace original with compressed version
                        pdf_file.unlink()  # Remove original
                        # Rename compressed to original name
                        compressed_file.rename(pdf_file)
                        compressed_count += 1
                        total_compressed_size += compressed_size
                        print(
                            f"    ✓ Compressed {pdf_file.name}: {original_size} → {compressed_size} bytes")
                    else:
                        # Keep original, remove compressed version
                        compressed_file.unlink()
                        total_compressed_size += original_size
                        print(
                            f"    - Skipped {pdf_file.name}: no size reduction")
                else:
                    # Compression failed, keep original
                    if compressed_file.exists():
                        compressed_file.unlink()
                    total_compressed_size += original_size
                    print(
                        f"    ✗ Failed to compress {pdf_file.name}: {message}")

            except Exception as e:
                print(f"    ✗ Error processing {pdf_file.name}: {e}")
                total_compressed_size += original_size

        # Recreate TAR file with updated PDFs
        if compressed_count > 0:
            print(
                f"  Recreating TAR with {compressed_count} compressed PDFs...")
            create_updated_tar(extract_dir, tar_file_path, is_compressed)

            # Calculate savings
            savings = total_original_size - total_compressed_size
            savings_percent = (savings / total_original_size *
                               100) if total_original_size > 0 else 0
            print(
                f"  ✓ TAR updated: {compressed_count}/{len(pdf_files)} PDFs compressed")
            print(
                f"  ✓ Size reduction: {savings:,} bytes ({savings_percent:.1f}%)")
        else:
            print(f"  No PDFs were compressed, keeping original TAR")


def create_updated_tar(extract_dir, original_tar_path, is_compressed=False):
    """Create a new TAR file with the updated PDFs."""
    # Create backup of original
    backup_path = original_tar_path.with_suffix('.tar.backup')
    shutil.copy2(original_tar_path, backup_path)

    # Determine TAR mode based on original compression
    tar_mode = 'w:gz' if is_compressed else 'w'

    # Create new TAR file with same compression as original
    with tarfile.open(original_tar_path, tar_mode) as tar:
        added_files = 0
        for file_path in extract_dir.rglob('*'):
            if file_path.is_file():
                # Add file to TAR with relative path
                arcname = file_path.relative_to(extract_dir)
                tar.add(file_path, arcname=str(arcname))
                added_files += 1
        print(f"    Added {added_files} files to new TAR")

    # Get sizes for comparison
    new_size = original_tar_path.stat().st_size
    backup_size = backup_path.stat().st_size

    # Always keep the new version if we compressed PDFs
    # The TAR might be larger due to compression overhead, but PDFs inside are smaller
    backup_path.unlink()
    print(f"    ✓ TAR file updated: {backup_size} → {new_size} bytes")

    if new_size < backup_size:
        print(
            f"    ✓ TAR file size reduced by {backup_size - new_size:,} bytes")
    elif new_size > backup_size:
        print(
            f"    ℹ️  TAR file size increased by {new_size - backup_size:,} bytes (but PDFs inside are compressed)")


def batch_compress_pdfs(
    compression_level="screen",
    max_workers=None,
):
    """
    Compress all PDF files using parallel processing across years, courts, and benches.

    Args:
        compression_level: Compression level to use
        max_workers: Maximum number of worker processes (None = CPU count)

    Returns:
        int: Number of successfully compressed files
    """
    output_folder = OUTPUT_DIR
    output_folder.mkdir(parents=True, exist_ok=True)

    # Generate all combinations of year, court, and bench
    combinations = generate_processing_combinations()

    print(f"Processing {len(combinations)} combinations of year/court/bench")
    print(f"Years: 2025-{datetime.now().year}")
    print(f"Courts and benches loaded from: {HIGH_COURTS_CSV}")
    print(
        f"Input directory structure: {INPUT_DIR}/data/tar/year=YYYY/court=XX_X/bench=name/")
    print(f"Processing TAR files containing PDFs - will compress PDFs and update TAR files in-place")
    print(f"Only PDFs that compress to smaller size will be replaced")

    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all compression tasks
        futures = [executor.submit(compress_pdf_wrapper, combination)
                   for combination in combinations]

        # Process results as they complete
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            desc="Compressing PDFs",
            total=len(futures)
        ):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing combination: {e}")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Compress PDF files using Ghostscript")
    parser.add_argument(
        "-o",
        "--output",
        help="Path for the compressed output file or folder",
    )
    parser.add_argument(
        "-l",
        "--level",
        choices=["screen", "ebook", "printer", "prepress", "default"],
        default="screen",
        help="Compression level (default: screen - highest compression)",
    )
    parser.add_argument(
        "-b",
        "--batch",
        default=True,
        action="store_true",
        help="Process input as a folder containing multiple PDF files",
    )

    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=10,
        help="Number of parallel compression jobs (default: CPU count)",
    )
    args = parser.parse_args()

    # Check Ghostscript availability
    print("Checking Ghostscript version:")
    gs_path = "/usr/bin/gs"  # Update this path if needed
    if os.system(f"{gs_path} --version") != 0:
        print("Error: Ghostscript not found. Please install Ghostscript.")
        return 1

    batch_compress_pdfs(args.level, args.jobs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
