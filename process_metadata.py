from pathlib import Path
import json
import lxml.html as LH
from tqdm import tqdm

src = Path("./data")


class MetadataProcessor:
    def __init__(self, src):
        self.src = src
        self.without_rh = 0

    def get_metadata_files(self):
        for file in src.glob("**/*.json"):
            yield file

    def process(self):
        for file in tqdm(self.get_metadata_files()):
            metadata = self.load_metadata(file)
            self.process_metadata(metadata)

    def process_metadata(self, metadata: dict) -> dict:
        if "raw_html" not in metadata:
            self.without_rh += 1
            return
        html_s = metadata["raw_html"]
        html_element = LH.fromstring(html_s)
        title = html_element.xpath("./button//text()")[0].strip()
        description_elem = html_element.xpath("./text()")
        description = description_elem[0].strip() if description_elem else None
        case_details = {
            "court_code": metadata["court_code"],
            "court_number": metadata["court_number"],
            "title": title,
            "description": description,
        }
        case_details_elements = html_element.xpath('//strong[@class="caseDetailsTD"]')[
            0
        ]
        case_details["CNR"] = case_details_elements.xpath(
            './/span[contains(text(), "CNR")]/following-sibling::font/text()'
        )[0]
        case_details["Date of registration"] = case_details_elements.xpath(
            './/span[contains(text(), "Date of registration")]/following-sibling::font/text()'
        )[0]
        case_details["Decision Date"] = case_details_elements.xpath(
            './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
        )[0]
        case_details["Disposal Nature"] = case_details_elements.xpath(
            './/span[contains(text(), "Disposal Nature")]/following-sibling::font/text()'
        )[0]
        case_details["Court"] = (
            case_details_elements.xpath('.//span[contains(text(), "Court")]/text()')[0]
            .split(":")[1]
            .strip()
        )

    def load_metadata(self, file: Path | str) -> dict:
        with open(file) as f:
            return json.load(f)


processor = MetadataProcessor(src)
processor.process()

print(processor.without_rh)
