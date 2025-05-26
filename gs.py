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

INPUT_DIR = Path("/home/ubuntu/data/court/cnrorders")
OUTPUT_DIR = Path("/home/ubuntu/compressed/court/cnrorders")
ERROR_LOG = OUTPUT_DIR / "error_log.txt"


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
            reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0
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


def compress_pdf_wrapper(court: Path):
    """Wrapper function for compress_pdf to use with concurrent.futures."""
    pdf_files = court.glob("**/*.pdf")
    for pdf_file in pdf_files:
        try:
            input_path = pdf_file
            output_path = OUTPUT_DIR / pdf_file.relative_to(INPUT_DIR)
            if output_path.exists():
                continue
            print(f"Compressing {pdf_file}")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            success, message = compress_pdf(input_path, output_path)

            # If compression failed, ensure no empty file is left behind
            if not success and output_path.exists():
                os.remove(output_path)

        except Exception as e:
            error_message = f"Error in wrapper: {str(e)}\n{traceback.format_exc()}"
            log_error(pdf_file)
            # Clean up any partial output file
            if output_path.exists():
                os.remove(output_path)


def batch_compress_pdfs(
    compression_level="screen",
    max_workers=None,
):
    """
    Compress all PDF files in a folder using parallel processing.

    Args:
        input_folder: Path to folder containing PDF files
        output_folder: Path where compressed PDFs will be saved
        compression_level: Compression level to use
        recursive: Whether to process subdirectories recursively
        max_workers: Maximum number of worker processes (None = CPU count)

    Returns:
        int: Number of successfully compressed files
    """
    input_folder = INPUT_DIR
    output_folder = OUTPUT_DIR
    output_folder.mkdir(parents=True, exist_ok=True)

    courts = INPUT_DIR.iterdir()

    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all compression tasks
        futures = [executor.submit(compress_pdf_wrapper, court) for court in courts]

        # Process results as they complete
        # for future in tqdm(
        #     concurrent.futures.as_completed(futures),
        #     desc="Compressing PDFs",
        # ):
        #     success, message = future.result()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Compress PDF files using Ghostscript")
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
