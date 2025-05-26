import os
import shutil
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# Configuration
INPUT_DIR = Path("/home/ubuntu/data/court/cnrorders")
OUTPUT_DIR = Path("/home/ubuntu/compressed/court/cnrorders")
GHOSTSCRIPT_CMD = [
    "gs",
    "-sDEVICE=pdfwrite",
    "-dPDFSETTINGS=/screen",
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
]


def process_pdf(input_pdf: Path):
    # Define output file
    relative_path = input_pdf.relative_to(INPUT_DIR)
    output_pdf = OUTPUT_DIR / relative_path
    if output_pdf.exists():
        return
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    # Build command
    cmd = GHOSTSCRIPT_CMD + [f"-sOutputFile={output_pdf}", str(input_pdf)]

    try:
        subprocess.run(cmd, check=True)
        old_size = input_pdf.stat().st_size
        new_size = output_pdf.stat().st_size
        if new_size > old_size:
            os.remove(output_pdf)
            shutil.copy(input_pdf, output_pdf)
    except subprocess.CalledProcessError as e:
        print(f"Failed: {input_pdf} with error {e}")


def process_directory(directory: Path):
    """Process all PDF files in a given directory."""
    pdf_files = directory.rglob("*.pdf")
    count = 0
    for pdf_file in pdf_files:
        process_pdf(pdf_file)
        count += 1
    print(f"Processed {count} PDFs in {directory}")
    return count


def main():
    # Get all immediate subdirectories of the input directory
    subdirectories = [d for d in INPUT_DIR.iterdir() if d.is_dir()]

    if not subdirectories:
        # If no subdirectories, process the main directory
        print(f"No subdirectories found. Processing all PDFs in {INPUT_DIR}")
        process_directory(INPUT_DIR)
    else:
        print(f"Found {len(subdirectories)} subdirectories to process")

        # Use multi-threading with one worker per subdirectory
        with ProcessPoolExecutor(max_workers=min(2, len(subdirectories))) as executor:
            results = list(executor.map(process_directory, subdirectories))

        total_processed = sum(results)
        print(f"Total PDFs processed: {total_processed}")


if __name__ == "__main__":
    main()
