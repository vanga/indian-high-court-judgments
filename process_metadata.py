from pathlib import Path
import json
import lxml.html as LH
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

    def get_metadata_files(self):
        for file in self.src.glob("**/*.json"):
            yield file

    def process(self):
        try:
            for file in tqdm(self.get_metadata_files()):
                try:
                    metadata = self.load_metadata(file)
                    processed = self.process_metadata(metadata)
                    if processed:  # Only add if we got valid data
                        self.add_record(processed)
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
        description = (
            description_elem[0].strip() if description_elem else ""
        )  # Empty string instead of None

        case_details = {
            "court_code": metadata["court_code"],
            "title": title,
            "description": description,
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
                )[0]
            except (IndexError, KeyError):
                case_details["cnr"] = ""

            try:
                case_details["date_of_registration"] = case_details_elements.xpath(
                    './/span[contains(text(), "Date of registration")]/following-sibling::font/text()'
                )[0]
            except (IndexError, KeyError):
                case_details["date_of_registration"] = ""

            try:
                case_details["decision_date"] = case_details_elements.xpath(
                    './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
                )[0]
            except (IndexError, KeyError):
                case_details["decision_date"] = ""

            try:
                case_details["disposal_nature"] = case_details_elements.xpath(
                    './/span[contains(text(), "Disposal Nature")]/following-sibling::font/text()'
                )[0]
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

        # Convert buffer to pandas DataFrame
        df = pd.DataFrame(self.record_buffer)

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Initialize writer if needed
        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(
                self.output_path,
                self.schema,
                compression="snappy",  # Added efficient compression
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
