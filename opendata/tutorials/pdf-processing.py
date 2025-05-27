import os
import boto3
import tarfile
import tempfile
from pathlib import Path
from typing import Optional
import PyPDF2
import logging
import gzip
import shutil
import io
import argparse


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
SOURCE_BUCKET_NAME = "indian-high-court-judgments"


class PDFProcessor:
    def __init__(
        self,
        output_bucket: str,
        year: Optional[str] = None,
        court: Optional[str] = None,
        bench: Optional[str] = None,
    ):
        self.s3_client = boto3.client("s3")

        self.source_bucket_name = SOURCE_BUCKET_NAME
        self.output_bucket_name = output_bucket
        self.year = year
        self.court = court
        self.bench = bench

        # Build the search prefix based on provided arguments
        self.s3_search_prefix = self._build_search_prefix()

    def _build_search_prefix(self) -> str:
        """Build S3 prefix for searching tar files based on provided arguments."""
        prefix_parts = ["data/tar"]

        if self.year:
            prefix_parts.append(f"year={self.year}")
            if self.court:
                prefix_parts.append(f"court={self.court}")
                if self.bench:
                    prefix_parts.append(f"bench={self.bench}")

        return "/".join(prefix_parts) + "/"

    def find_tar_files(self) -> list:
        """Find all tar files matching the search criteria."""
        try:
            tar_files = []
            paginator = self.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(
                Bucket=self.source_bucket_name, Prefix=self.s3_search_prefix
            ):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if obj["Key"].endswith("pdfs.tar"):
                            tar_files.append(obj["Key"])

            logger.info(f"Found {len(tar_files)} tar files matching criteria")
            return tar_files

        except Exception as e:
            logger.error(f"Failed to find tar files: {e}")
            return []

    def download_tar_file(self, tar_key: str, local_path: str) -> bool:
        try:
            self.s3_client.download_file(self.source_bucket_name, tar_key, local_path)
            return True
        except Exception as e:
            logger.error(f"Failed to download tar file {tar_key}: {e}")
            return False

    def extract_tar_file(self, tar_path: str, extract_dir: str) -> bool:
        try:
            with tarfile.open(tar_path, "r") as tar:
                tar.extractall(path=extract_dir)
            return True
        except Exception:
            return False

    def pdf_to_text(self, pdf_path: str) -> Optional[list]:
        try:
            pages_content = []
            with open(pdf_path, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    pages_content.append(page_text)
            return pages_content
        except Exception:
            return None

    def _build_output_prefix_for_tar(self, tar_key: str) -> str:
        """Build S3 prefix for output files based on the tar file's parent path."""
        # Extract parent directory from tar file path
        # e.g., "data/tar/year=2023/court=SC/bench=1/pdfs.tar" -> "data/tar/year=2023/court=SC/bench=1/"
        parent_path = "/".join(tar_key.split("/")[:-1])

        # Replace "data/tar" with "data/text-tars"
        output_path = parent_path.replace("data/tar", "data/text-tars", 1)

        return output_path + "/"

    def process_pdfs_in_directory(self, directory: str, output_prefix: str) -> int:
        """Process PDFs and create a tar.gz archive directly."""
        pdf_files = list(Path(directory).rglob("*.pdf"))
        if not pdf_files:
            return 0

        processed_count = 0

        # Create tar.gz archive directly
        archive_key = f"{output_prefix}texts.tar.gz"

        try:
            with tempfile.NamedTemporaryFile(
                suffix=".tar.gz", delete=False
            ) as temp_archive:
                with tarfile.open(temp_archive.name, "w:gz") as tar:
                    for pdf_file in pdf_files:
                        pages_content = self.pdf_to_text(str(pdf_file))
                        if pages_content is None:
                            continue

                        # Create text content in the same JSON-like format
                        text_content = "[\n"
                        for i, page_content in enumerate(pages_content):
                            stripped_content = page_content.strip()
                            escaped_content = stripped_content.replace(
                                '"', '\\"'
                            ).replace("\n", "\\n")
                            text_content += f'  "{escaped_content}"'
                            if i < len(pages_content) - 1:
                                text_content += ","
                            text_content += "\n"
                        text_content += "]\n"

                        # Add to tar archive
                        text_filename = f"{Path(pdf_file).stem}.txt"
                        tarinfo = tarfile.TarInfo(name=text_filename)
                        text_bytes = text_content.encode("utf-8")
                        tarinfo.size = len(text_bytes)
                        tar.addfile(tarinfo, fileobj=io.BytesIO(text_bytes))

                        processed_count += 1

                # Upload the archive to S3
                if processed_count > 0:
                    with open(temp_archive.name, "rb") as archive_file:
                        self.s3_client.put_object(
                            Bucket=self.output_bucket_name,
                            Key=archive_key,
                            Body=archive_file,
                            ContentType="application/gzip",
                        )

                    logger.info(
                        f"Uploaded archive to s3://{self.output_bucket_name}/{archive_key}"
                    )

                # Clean up temporary file
                os.unlink(temp_archive.name)

        except Exception as e:
            logger.error(f"Failed to create archive for {output_prefix}: {e}")
            return 0

        return processed_count

    def run(self):
        """Process all matching tar files."""
        tar_files = self.find_tar_files()

        if not tar_files:
            logger.warning("No tar files found matching the criteria")
            return False

        total_processed = 0

        for tar_key in tar_files:
            logger.info(f"Processing tar file: {tar_key}")

            # Build output prefix for this specific tar file
            output_prefix = self._build_output_prefix_for_tar(tar_key)

            with tempfile.TemporaryDirectory() as temp_dir:
                tar_path = os.path.join(temp_dir, "pdfs.tar")
                if not self.download_tar_file(tar_key, tar_path):
                    logger.error(f"Skipping {tar_key} due to download failure")
                    continue

                extract_dir = os.path.join(temp_dir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)

                if not self.extract_tar_file(tar_path, extract_dir):
                    logger.error(f"Skipping {tar_key} due to extraction failure")
                    continue

                processed_count = self.process_pdfs_in_directory(
                    extract_dir, output_prefix
                )
                total_processed += processed_count
                logger.info(f"Processed {processed_count} files from {tar_key}")

        return total_processed > 0


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process PDF files from S3 and extract text content"
    )

    # Mandatory arguments
    parser.add_argument(
        "--output-bucket",
        required=True,
        help="Output S3 bucket name for processed text files",
    )

    # Optional arguments
    parser.add_argument("--year", help="Year for organizing files (optional)")
    parser.add_argument(
        "--court", help="Court identifier for organizing files (optional)"
    )
    parser.add_argument(
        "--bench", help="Bench identifier for organizing files (optional)"
    )

    return parser.parse_args()


def main():
    try:
        args = parse_arguments()
        processor = PDFProcessor(
            output_bucket=args.output_bucket,
            year=args.year,
            court=args.court,
            bench=args.bench,
        )
        success = processor.run()
        if not success:
            exit(1)
    except Exception:
        exit(1)


if __name__ == "__main__":
    main()
