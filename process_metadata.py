from pathlib import Path
import json
import lxml.html as LH
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import exiftool
import concurrent.futures
import os
import shutil

# Import shared HTML parsing utilities
from src.utils.html_utils import parse_case_details_from_html

ADD_PDF_METADATA = False

src = Path("./data")


class MetadataProcessor:
    def __init__(self, src, batch_size=5000, output_path="processed_metadata.parquet"):
        self.src = src
        self.without_rh = 0
        self.output_path = output_path
        self.record_count = 0
        self.batch_size = batch_size

        # Buffer to hold records before writing
        self.record_buffer = []

        # Schema will be initialized from the first valid record
        self.schema = None
        self.writer = None

        # Define all fields to ensure consistency
        self.all_fields = [
            "court_code",
            "title",
            "description",
            "judge",
            "pdf_link",
            "cnr",
            "date_of_registration",
            "decision_date",
            "disposal_nature",
            "court",
            "raw_html",
            "pdf_exists",
        ]

        if ADD_PDF_METADATA:
            self.all_fields.extend(
                [
                    "size",
                    "file_type",
                    "mime_type",
                    "pdf_version",
                    "pdf_linearized",
                    "pdf_pages",
                    "pdf_producer",
                    "pdf_language",
                ]
            )

    def get_metadata_files(self):
        for file in self.src.glob("**/*.json"):
            yield file

    def _add_pdf_metadata(self, processed, pdf_path: Path):
        import exiftool
        if not pdf_path.exists():
            print(f"Skipping {pdf_path} because it has no pdf")
            return
        pdf_metadata = exiftool.get_metadata(pdf_path)
        if len(pdf_metadata) != 1:
            print(
                f"Error processing {pdf_path} for exif metadata, count: {len(pdf_metadata)}"
            )
        else:
            pdf_metadata = pdf_metadata[0]
            processed["size"] = pdf_metadata.get("File:FileSize", None)
            processed["file_type"] = pdf_metadata.get("File:FileType", None)
            processed["mime_type"] = pdf_metadata.get("File:MIMEType", None)
            processed["pdf_version"] = pdf_metadata.get("PDF:PDFVersion", None)
            processed["pdf_linearized"] = pdf_metadata.get(
                "PDF:Linearized", None)
            processed["pdf_pages"] = pdf_metadata.get("PDF:PageCount", None)
            processed["pdf_producer"] = pdf_metadata.get("PDF:Producer", None)
            processed["pdf_language"] = pdf_metadata.get("PDF:Language", None)
            processed["pdf_producer"] = pdf_metadata.get("PDF:Producer", None)

    def process(self):
        try:
            if ADD_PDF_METADATA:
                with exiftool.ExifToolHelper() as et:
                    for file in tqdm(self.get_metadata_files()):
                        try:
                            metadata = self.load_metadata(file)
                            processed = self.process_metadata(metadata)
                            if not processed:
                                print(
                                    f"Skipping {file} because it has no raw_html")
                                continue
                        except Exception as e:
                            print(f"Error processing {file}: {e}")
                            continue

                        pdf_path = file.with_suffix(".pdf")
                        if not pdf_path.exists():
                            processed["pdf_exists"] = False
                        else:
                            processed["pdf_exists"] = True
                        try:
                            self._add_pdf_metadata(processed, pdf_path)
                        except Exception as e:
                            print(f"Error processing {file}: {e}")
                        finally:
                            self.add_record(processed)
            else:
                for file in tqdm(self.get_metadata_files()):
                    try:
                        metadata = self.load_metadata(file)
                        processed = self.process_metadata(metadata)
                        if not processed:
                            print(
                                f"Skipping {file} because it has no raw_html")
                            continue

                        pdf_path = file.with_suffix(".pdf")
                        if not pdf_path.exists():
                            processed["pdf_exists"] = False
                        else:
                            processed["pdf_exists"] = True

                        self.add_record(processed)
                    except Exception as e:
                        print(f"Error processing {file}: {e}")
        except Exception as e:
            print(f"Error processing {file}: {e}")
        finally:
            if self.record_buffer:
                self.write_batch()
            if hasattr(self, "writer") and self.writer is not None:
                self.writer.close()
                print(
                    f"Wrote {self.record_count} records to {self.output_path}")

    def process_metadata(self, metadata: dict) -> dict:
        if "raw_html" not in metadata:
            self.without_rh += 1
            return None

        html_s = metadata["raw_html"]
        html_element = LH.fromstring(html_s)

        # Add try-except to handle missing title
        try:
            title = html_element.xpath("./button//text()")[0].strip()
        except (IndexError, KeyError):
            title = ""

        description_elem = html_element.xpath("./text()")
        judge_txt = html_element.xpath("./strong/text()")
        judge_name = judge_txt[0].split(":")[1].strip() if judge_txt else ""
        description = (
            description_elem[0].strip() if description_elem else ""
        )  # Empty string instead of None

        case_details = {
            "court_code": metadata["court_code"],
            "title": title,
            "description": description,
            "judge": judge_name,
            "pdf_link": metadata["pdf_link"],
            "raw_html": metadata["raw_html"],
        }

        # Use shared HTML parsing utility to extract case details
        parsed_details = parse_case_details_from_html(metadata["raw_html"])
        case_details.update(parsed_details)

        return case_details

    def add_record(self, record):
        """Add a record to the buffer and write if the buffer is full."""
        self.record_buffer.append(record)

        # If buffer reaches batch size, write the batch
        if len(self.record_buffer) >= self.batch_size:
            self.write_batch()

    def write_batch(self):
        """Write the current buffer as a batch to the parquet file."""
        if not self.record_buffer:
            return

        # Ensure all records have all fields (with None for missing values)
        for record in self.record_buffer:
            for field in self.all_fields:
                if field not in record:
                    record[field] = None

        # Convert buffer to pandas DataFrame
        df = pd.DataFrame(self.record_buffer)

        # Ensure DataFrame has all expected columns in the right order
        for field in self.all_fields:
            if field not in df.columns:
                df[field] = None

        # Reorder columns to match expected schema
        df = df[self.all_fields]

        # Define explicit dtypes to ensure consistency across batches
        dtypes = {
            "court_code": "string",
            "title": "string",
            "description": "string",
            "judge": "string",
            "pdf_link": "string",
            "cnr": "string",
            "date_of_registration": "string",  # some dates are malformed
            "decision_date": "date32[day][pyarrow]",
            "disposal_nature": "string",
            "court": "string",
            "raw_html": "string",
            "pdf_exists": "boolean",
            "size": "Int64",  # Nullable integer
            "file_type": "string",
            "mime_type": "string",
            "pdf_version": "float64",
            "pdf_linearized": "boolean",
            "pdf_pages": "Int64",  # Explicitly use Int64 (nullable integer)
            "pdf_producer": "string",
            "pdf_language": "string",
        }

        # Handle date conversion with proper format
        if "decision_date" in df.columns:
            # Convert decision_date with dayfirst=True for DD-MM-YYYY format
            df["decision_date"] = pd.to_datetime(
                df["decision_date"], format="mixed", dayfirst=True, errors="coerce"
            )

        # Apply the dtypes
        for col, dtype in dtypes.items():
            if (
                col in df.columns and col != "decision_date"
            ):  # Skip decision_date as we handled it above
                df[col] = df[col].astype(dtype)

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(
            df,
        )

        # Initialize writer if needed
        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(
                self.output_path,
                self.schema,
                compression="snappy",
            )

        # Write the batch
        self.writer.write_table(table)

        # Update record count
        self.record_count += len(self.record_buffer)

        # Clear the buffer
        self.record_buffer = []

    def load_metadata(self, file: Path | str) -> dict:
        with open(file) as f:
            return json.load(f)

    def process_court_dir(self, court_dir):
        """Process a single court directory and return the output file path."""
        court_name = court_dir.name
        output_file = self.output_dir / f"{court_name}_metadata.parquet"

        processor = MetadataProcessor(court_dir, batch_size=self.batch_size)
        processor.output_path = output_file
        processor.process()

        return output_file, processor.record_count, processor.without_rh

    def process_parallel(self, max_workers=None):
        """Process all court directories in parallel."""
        court_dirs = [d for d in self.src.glob(
            "court/cnrorders/*") if d.is_dir()]

        if not court_dirs:
            print("No court directories found!")
            return

        print(f"Found {len(court_dirs)} court directories to process")

        total_records = 0
        total_without_rh = 0
        output_files = []

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:
            futures = {
                executor.submit(self.process_court_dir, court_dir): court_dir.name
                for court_dir in court_dirs
            }

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing courts",
            ):
                court_name = futures[future]
                try:
                    output_file, record_count, without_rh = future.result()
                    output_files.append(output_file)
                    total_records += record_count
                    total_without_rh += without_rh
                    print(
                        f"Completed {court_name}: {record_count} records, {without_rh} without raw_html"
                    )
                except Exception as e:
                    print(f"Error processing {court_name}: {e}")

        self.combine_parquet_files(output_files)

        print(f"Total records processed: {total_records}")
        print(f"Total records without raw_html: {total_without_rh}")

    def combine_parquet_files(self, file_paths):
        """Combine multiple parquet files into a single file."""
        if not file_paths:
            print("No files to combine")
            return

        print(f"Combining {len(file_paths)} parquet files...")

        # Read and combine all parquet files
        dfs = []
        for file_path in file_paths:
            if file_path.exists() and file_path.stat().st_size > 0:
                try:
                    df = pd.read_parquet(file_path)
                    dfs.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

        if not dfs:
            print("No valid parquet files to combine")
            return

        combined_df = pd.concat(dfs, ignore_index=True)

        combined_df.to_parquet(self.output_path, compression="snappy")

        print(
            f"Combined {len(dfs)} files with {len(combined_df)} total records to {self.output_path}"
        )


if __name__ == "__main__":
    processor = MetadataProcessor(
        src, batch_size=1000, output_dir="processed_data")

    processor.process_parallel(max_workers=32)
