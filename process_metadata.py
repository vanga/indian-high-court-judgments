from pathlib import Path
import json
import lxml.html as LH
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import exiftool

src = Path("./data")


class MetadataProcessor:
    def __init__(self, src, batch_size=5000):
        self.src = src
        self.without_rh = 0
        self.output_path = "processed_metadata.parquet"
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
            "size",
            "file_type",
            "mime_type",
            "pdf_version",
            "pdf_linearized",
            "pdf_pages",
            "pdf_producer",
            "pdf_language",
        ]

    def get_metadata_files(self):
        for file in self.src.glob("**/*.json"):
            yield file

    def process(self):
        try:
            count = 0
            with exiftool.ExifToolHelper() as et:
                for file in tqdm(self.get_metadata_files()):
                    count +=1
                    if count > 1000000:
                        break
                    try:
                        metadata = self.load_metadata(file)
                        processed = self.process_metadata(metadata)
                        if not processed:
                            print(f"Skipping {file} because it has no raw_html")
                            continue
                        pdf_path = file.with_suffix(".pdf")
                        if not pdf_path.exists():
                            print(f"Skipping {file} because it has no pdf")
                            continue
                        pdf_metadata = et.get_metadata(pdf_path)
                        if len(pdf_metadata) != 1:
                            print(
                                f"Error processing {file} for exif metadata, count: {len(pdf_metadata)}"
                            )
                        else:
                            pdf_metadata = pdf_metadata[0]
                            processed["size"] = pdf_metadata.get("File:FileSize", None)
                            processed["file_type"] = pdf_metadata.get(
                                "File:FileType", None
                            )
                            processed["mime_type"] = pdf_metadata.get(
                                "File:MIMEType", None
                            )
                            processed["pdf_version"] = pdf_metadata.get(
                                "PDF:PDFVersion", None
                            )
                            processed["pdf_linearized"] = pdf_metadata.get(
                                "PDF:Linearized", None
                            )
                            processed["pdf_pages"] = pdf_metadata.get(
                                "PDF:PageCount", None
                            )
                            processed["pdf_producer"] = pdf_metadata.get(
                                "PDF:Producer", None
                            )
                            processed["pdf_language"] = pdf_metadata.get(
                                "PDF:Language", None
                            )
                            processed["pdf_producer"] = pdf_metadata.get(
                                "PDF:Producer", None
                            )

                        self.add_record(processed)
                    except Exception as e:
                        print(f"Error processing {file}: {e}")
                        continue
        except Exception as e:
            print(f"Error processing {file}: {e}")

            # Write any remaining records in the buffer
            if self.record_buffer:
                self.write_batch()

        finally:
            # Close the writer if it exists
            if hasattr(self, "writer") and self.writer is not None:
                self.writer.close()
                print(f"Wrote {self.record_count} records to {self.output_path}")

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
        }

        # Wrap XPath queries in try-except to handle missing elements
        try:
            case_details_elements = html_element.xpath(
                '//strong[@class="caseDetailsTD"]'
            )[0]

            # Handle potential missing fields with default empty strings
            try:
                case_details["cnr"] = case_details_elements.xpath(
                    './/span[contains(text(), "CNR")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                case_details["cnr"] = ""

            try:
                case_details["date_of_registration"] = case_details_elements.xpath(
                    './/span[contains(text(), "Date of registration")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                case_details["date_of_registration"] = ""

            try:
                case_details["decision_date"] = case_details_elements.xpath(
                    './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                case_details["decision_date"] = ""

            try:
                case_details["disposal_nature"] = case_details_elements.xpath(
                    './/span[contains(text(), "Disposal Nature")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                case_details["disposal_nature"] = ""

            try:
                case_details["court"] = (
                    case_details_elements.xpath(
                        './/span[contains(text(), "Court")]/text()'
                    )[0]
                    .split(":")[1]
                    .strip()
                )
            except (IndexError, KeyError):
                case_details["court"] = ""

        except (IndexError, KeyError):
            # If we can't find the case details element, set all fields to empty strings
            case_details["cnr"] = ""
            case_details["date_of_registration"] = ""
            case_details["decision_date"] = ""
            case_details["disposal_nature"] = ""
            case_details["court"] = ""

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
            "date_of_registration": "string",
            "decision_date": "string",
            "disposal_nature": "string",
            "court": "string",
            "size": "Int64",  # Nullable integer
            "file_type": "string",
            "mime_type": "string",
            "pdf_version": "float64",
            "pdf_linearized": "boolean",
            "pdf_pages": "Int64",  # Explicitly use Int64 (nullable integer)
            "pdf_producer": "string",
            "pdf_language": "string",
        }

        # Apply the dtypes
        for col, dtype in dtypes.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

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


# You can adjust the batch size based on your data characteristics and memory availability
processor = MetadataProcessor(src, batch_size=1000)
processor.process()

print(f"Records without raw_html: {processor.without_rh}")
