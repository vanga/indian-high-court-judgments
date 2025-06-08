from typing import Optional, Generator
from tqdm import tqdm
import argparse
from datetime import datetime, timedelta
import traceback
import re
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import lxml.html as LH
from http.cookies import SimpleCookie
import urllib
import easyocr
import logging
import threading
import concurrent.futures
import urllib3
import uuid

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# add a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

reader = easyocr.Reader(["en"])

root_url = "https://scr.sci.gov.in"
output_dir = Path("./sc_data")
START_DATE = "2024-01-10"

# Updated payload for Supreme Court based on curl request
payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=10&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&act=&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=3&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ncn=&bool_opt=&sort_flg=undefined&ajax_req=true"

# Updated PDF payload for Supreme Court
pdf_link_payload = "val=0&lang_flg=undefined&path=2025_5_275_330&citation_year=2025&fcourt_type=3&nc_display=2025INSC555&ajax_req=true"

page_size = 5000
MATH_CAPTCHA = False
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)


def get_json_file(file_path) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def get_tracking_data():
    try:
        tracking_data = get_json_file("./sc_track.json")
    except FileNotFoundError:
        tracking_data = {}
    return tracking_data


def save_tracking_data(tracking_data):
    with open("./sc_track.json", "w") as f:
        json.dump(tracking_data, f)


def save_tracking_date(tracking_data):
    # acquire a lock
    lock.acquire()
    save_tracking_data(tracking_data)
    # release the lock
    lock.release()


def get_new_date_range(
    last_date: str, day_step: int = 30
) -> tuple[str | None, str | None]:
    last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")
    new_from_date_dt = last_date_dt + timedelta(days=1)
    new_to_date_dt = new_from_date_dt + timedelta(days=day_step - 1)
    if new_from_date_dt.date() > datetime.now().date():
        return None, None

    if new_to_date_dt.date() > datetime.now().date():
        new_to_date_dt = datetime.now().date()
    new_from_date = new_from_date_dt.strftime("%Y-%m-%d")
    new_to_date = new_to_date_dt.strftime("%Y-%m-%d")
    return new_from_date, new_to_date


def get_date_ranges_to_process(start_date=None, end_date=None, day_step=30):
    """
    Generate date ranges to process for Supreme Court.
    If start_date is provided but no end_date, use current date as end_date.
    If neither is provided, use tracking data to determine the next date range.
    """
    # If start_date is provided but end_date is not, use current date as end_date
    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date and end_date:
        # Convert string dates to datetime objects
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate date ranges with specified step
        current_date = start_date_dt
        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)
    else:
        # Use tracking data to get next date range
        tracking_data = get_tracking_data()
        last_date = tracking_data.get("last_date", START_DATE)

        # Process from last_date to current date in chunks
        current_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        end_date_dt = datetime.now()

        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)


class SCDateTask:
    """A task representing a date range to process for Supreme Court"""

    def __init__(self, from_date, to_date):
        self.id = str(uuid.uuid4())
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self):
        return f"SCDateTask(id={self.id}, from_date={self.from_date}, to_date={self.to_date})"


def generate_tasks(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    day_step: int = 1,
) -> Generator[SCDateTask, None, None]:
    """Generate tasks for processing Supreme Court date ranges as a generator"""
    for from_date, to_date in get_date_ranges_to_process(
        start_date, end_date, day_step
    ):
        yield SCDateTask(from_date, to_date)


def process_task(task):
    """Process a single date task"""
    try:
        downloader = Downloader(task)
        downloader.download()
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        traceback.print_exc()


def run(start_date=None, end_date=None, day_step=1, max_workers=1):
    """
    Run the downloader with optional parameters using Python's multiprocessing
    with a generator that yields tasks on demand.
    """
    # Create a task generator
    tasks = generate_tasks(start_date, end_date, day_step)

    # Use ProcessPoolExecutor with map to process tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # map automatically consumes the iterator and processes tasks in parallel
        # it returns results in the same order as the input iterator
        for i, result in enumerate(executor.map(process_task, tasks)):
            # process_task doesn't return anything, so we're just tracking progress
            logger.info(f"Completed task {i+1}")

    logger.info("All tasks completed")


class Downloader:
    def __init__(self, task: SCDateTask):
        self.task = task
        self.root_url = "https://scr.sci.gov.in"
        self.search_url = f"{self.root_url}/scrsearch/?p=pdf_search/home/"
        self.captcha_url = (
            f"{self.root_url}/scrsearch/vendor/securimage/securimage_show.php"
        )
        self.captcha_token_url = f"{self.root_url}/scrsearch/?p=pdf_search/checkCaptcha"
        self.pdf_link_url = f"{self.root_url}/scrsearch/?p=pdf_search/openpdfcaptcha"
        self.pdf_link_url_wo_captcha = (
            f"{self.root_url}/scrsearch/?p=pdf_search/openpdf"
        )

        self.tracking_data = get_tracking_data()
        self.session_cookie_name = "SCR_SESSID"
        self.ecourts_token_cookie_name = "JSESSION"
        self.session_id = None
        self.ecourts_token = None

    def _results_exist_in_search_response(self, res_dict):
        results_exist = (
            "reportrow" in res_dict
            and "aaData" in res_dict["reportrow"]
            and len(res_dict["reportrow"]["aaData"]) > 0
        )
        if results_exist:
            no_of_results = len(res_dict["reportrow"]["aaData"])
            logger.info(f"Found {no_of_results} results for task: {self.task}")
        return results_exist

    def _prepare_next_iteration(self, search_payload):
        search_payload["sEcho"] += 1
        search_payload["iDisplayStart"] += page_size
        logger.info(
            f"Next iteration: {search_payload['iDisplayStart']}, task: {self.task.id}"
        )
        return search_payload

    def process_result_row(self, row, row_pos):
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")

        # First check for direct PDF button (single language)
        if soup.button and "onclick" in soup.button.attrs:
            pdf_info = self.extract_pdf_fragment(soup.button["onclick"])
            if pdf_info:
                return self._process_pdf_download(pdf_info, row_pos, html)

        # Check for multi-language selector
        select_element = soup.find("select", {"name": "language"})
        if select_element and "onchange" in select_element.attrs:
            pdf_info = self.extract_pdf_fragment_from_select(select_element["onchange"])
            if pdf_info:
                return self._process_pdf_download(
                    pdf_info, row_pos, html, is_multilang=True
                )

        # If neither button nor select found
        logger.info(f"No PDF button or language selector found, task: {self.task}")
        with open("html-parse-failures.txt", "a") as f:
            f.write(html + "\n")
        return False

    def extract_pdf_fragment(self, html_attribute):
        """Extract PDF fragment from button onclick attribute"""
        # Pattern: javascript:open_pdf('0','2008','2008_2_95_100','2008INSC111');
        pattern = r"javascript:open_pdf\('(.*?)','(.*?)','(.*?)','(.*?)'\)"
        match = re.search(pattern, html_attribute)
        if match:
            val = match.group(1)
            citation_year = match.group(2)
            path = match.group(3).split("#")[0]
            nc_display = match.group(4)
            return {
                "val": val,
                "citation_year": citation_year,
                "path": path,
                "nc_display": nc_display,
            }
        return None

    def extract_pdf_fragment_from_select(self, onchange_attr):
        """Extract PDF fragment and metadata from multi-language select onchange attribute"""
        # Pattern: javascript:get_pdf_lang('0','2008_2_95_100','2008');
        pattern = r"javascript:get_pdf_lang\('(.*?)','(.*?)','(.*?)'\)"
        match = re.search(pattern, onchange_attr)
        if match:
            val = match.group(1)
            path = match.group(2).split("#")[0]
            citation_year = match.group(3)
            return {
                "val": val,
                "path": path,
                "citation_year": citation_year,
                "nc_display": "",  # Will be extracted from HTML
            }
        return None

    def extract_language_options(self, select_element):
        """Extract available language options from select element"""
        languages = []
        options = select_element.find_all("option")
        for option in options:
            value = option.get("value", "")
            text = option.get_text(strip=True)
            languages.append({"value": value, "text": text})
        return languages

    def _process_pdf_download(self, pdf_info, row_pos, html, is_multilang=False):
        """Common method to process PDF download for both single and multi-language cases"""
        if not pdf_info or not pdf_info.get("path"):
            logger.error(f"Invalid PDF info extracted: {pdf_info}")
            return False

        path = pdf_info["path"]
        citation_year = pdf_info.get("citation_year", "")
        nc_display = pdf_info.get("nc_display", "")
        val = pdf_info.get("val", str(row_pos))

        # If nc_display is empty (from multi-lang), try to extract from HTML
        if not nc_display:
            nc_display = self.extract_nc_display(html)

        pdf_output_path = self.get_pdf_output_path(path)
        is_pdf_present = self.is_pdf_downloaded(path)
        pdf_needs_download = not is_pdf_present

        # Extract language options if multilingual
        available_languages = []
        if is_multilang:
            soup = BeautifulSoup(html, "html.parser")
            select_element = soup.find("select", {"name": "language"})
            if select_element:
                available_languages = self.extract_language_options(select_element)

        if pdf_needs_download:
            if is_multilang:
                # For multi-language, pass all extracted parameters
                is_fresh_download = self.download_pdf_multilang(
                    path, val, citation_year, nc_display, lang_code=""
                )
            else:
                is_fresh_download = self.download_pdf(
                    path, val, citation_year, nc_display
                )
        else:
            is_fresh_download = False

        metadata_output = pdf_output_path.with_suffix(".json")
        metadata = {
            "raw_html": html,
            "pdf_link": path,
            "citation_year": citation_year,
            "nc_display": nc_display,
            "val": val,
            "downloaded": is_pdf_present or is_fresh_download,
            "is_multilang": is_multilang,
            "available_languages": available_languages,
        }
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_output, "w") as f:
            json.dump(metadata, f)
        return is_fresh_download

    def download_pdf_multilang(
        self, pdf_fragment, val, citation_year, nc_display, lang_code=""
    ):
        """Download PDF for multi-language judgment with specific language"""
        # prepare temp pdf request
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        pdf_link_payload = self.default_pdf_link_payload()

        # Set all required parameters matching the curl request
        pdf_link_payload["val"] = val
        pdf_link_payload["path"] = pdf_fragment
        pdf_link_payload["citation_year"] = citation_year
        pdf_link_payload["nc_display"] = nc_display
        pdf_link_payload["fcourt_type"] = "3"
        pdf_link_payload["ajax_req"] = "true"

        # Set language flag for multi-language judgments
        if lang_code:
            pdf_link_payload["lang_flg"] = lang_code
        else:
            # For English (default), set to empty string
            pdf_link_payload["lang_flg"] = ""

        logger.debug(
            f"Downloading multilang PDF with lang_flg='{pdf_link_payload['lang_flg']}', path: {pdf_fragment}, citation_year: {citation_year}, nc_display: {nc_display}"
        )

        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )

        if "outputfile" not in pdf_link_response.json():
            logger.error(
                f"Error downloading multilang pdf, task: {self.task}, Error: {pdf_link_response.json()}"
            )
            return False

        pdf_download_link = pdf_link_response.json()["outputfile"]

        # download pdf and save
        pdf_response = requests.request(
            "GET",
            self.root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
        )
        pdf_output_path.parent.mkdir(parents=True, exist_ok=True)

        # number of response bytes
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0:
            logger.error(
                f"Empty multilang pdf, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        if no_of_bytes == 315:
            logger.error(
                f"404 multilang pdf response, task: {self.task}, output path: {pdf_output_path}"
            )
            return False

        with open(pdf_output_path, "wb") as f:
            f.write(pdf_response.content)
        logger.debug(
            f"Downloaded multilang PDF, task: {self.task}, output path: {pdf_output_path}, size: {no_of_bytes}, lang: {lang_code or 'English'}"
        )
        return True

    def extract_nc_display(self, html):
        """Extract nc_display (neutral citation) from HTML"""
        soup = BeautifulSoup(html, "html.parser")

        # Look for neutral citation patterns like "2008INSC111"
        # This might be in various places in the HTML
        text = soup.get_text()

        # Pattern for neutral citation: YEAR + INSC/SC + number
        nc_pattern = r"(\d{4}(?:INSC|SC)\d+)"
        match = re.search(nc_pattern, text)
        if match:
            return match.group(1)

        # Alternative: look for citation in specific elements
        # Sometimes it might be in a span, div, or other element
        for element in soup.find_all(["span", "div", "strong", "b"]):
            element_text = element.get_text(strip=True)
            match = re.search(nc_pattern, element_text)
            if match:
                return match.group(1)

        return ""

    def solve_math_expression(self, expression):
        # credits to: https://github.com/NoelShallum
        expression = expression.strip().replace(" ", "").replace(".", "")
        if "+" in expression:
            nums = expression.split("+")
            return str(int(nums[0]) + int(nums[1]))
        elif "-" in expression:
            nums = expression.split("-")
            return str(int(nums[0]) - int(nums[1]))
        elif (
            "*" in expression
            or "X" in expression
            or "x" in expression
            or "×" in expression
        ):
            expression = (
                expression.replace("x", "*").replace("×", "*").replace("X", "*")
            )
            nums = expression.split("*")
            return str(int(nums[0]) * int(nums[1]))
        elif "/" in expression or "÷" in expression:
            expression = expression.replace("÷", "/")
            nums = expression.split("/")
            return str(int(nums[0]) // int(nums[1]))
        else:
            raise ValueError(f"Unsupported mathematical expression: {expression}")

    def is_math_expression(self, expression):
        separators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        for separator in separators:
            if separator in expression:
                return True
        return False

    def solve_captcha(self, retries=0, captcha_url=None):
        logger.debug(f"Solving captcha, retries: {retries}, task: {self.task.id}")
        if retries > 10:
            raise ValueError("Failed to solve captcha")
        if captcha_url is None:
            captcha_url = self.captcha_url

        # download captcha image and save
        captcha_response = requests.get(
            captcha_url, headers={"Cookie": self.get_cookie()}, verify=False, timeout=30
        )

        # Generate a unique filename using UUID
        unique_id = uuid.uuid4().hex[:8]
        captcha_filename = Path(f"{captcha_tmp_dir}/captcha_sc_{unique_id}.png")
        with open(captcha_filename, "wb") as f:
            f.write(captcha_response.content)

        result = reader.readtext(str(captcha_filename))
        if not result:
            logger.warning(
                f"No result from captcha, task: {self.task.id}, retries: {retries}"
            )
            return self.solve_captcha(retries + 1, captcha_url)

        captcha_text = result[0][1].strip()

        if MATH_CAPTCHA:
            if self.is_math_expression(captcha_text):
                try:
                    answer = self.solve_math_expression(captcha_text)
                    captcha_filename.unlink()
                    return answer
                except Exception as e:
                    logger.error(
                        f"Error solving math expression, task: {self.task.id}, retries: {retries}, captcha text: {captcha_text}, Error: {e}"
                    )
                    # move the captcha image to a new folder for debugging
                    new_filename = f"{uuid.uuid4().hex[:8]}_{captcha_filename.name}"
                    captcha_filename.rename(
                        Path(f"{captcha_failures_dir}/{new_filename}")
                    )
                    return self.solve_captcha(retries + 1, captcha_url)
            else:
                # If not a math expression, try again
                captcha_filename.unlink()  # Clean up the file
                return self.solve_captcha(retries + 1, captcha_url)
        else:
            captcha_text = captcha_text.strip()
            if len(captcha_text) != 6:
                if retries > 10:
                    raise Exception("Captcha not solved")
                return self.solve_captcha(retries + 1)
            return captcha_text

    def solve_pdf_download_captcha(self, response, pdf_link_payload, retries=0):
        html_str = response["filename"]
        html = LH.fromstring(html_str)
        img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
        img_src = self.root_url + img_src

        # download captcha image and save
        captcha_text = self.solve_captcha(captcha_url=img_src)
        pdf_link_payload["captcha1"] = captcha_text

        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url_wo_captcha, pdf_link_payload
        )
        res_json = pdf_link_response.json()

        if "message" in res_json and res_json["message"] == "Captcha not solved":
            logger.warning(
                f"Captcha not solved, task: {self.task.id}, retries: {retries}, Error: {pdf_link_response.json()}"
            )
            if retries == 2:
                return res_json
            logger.info(f"Retrying pdf captcha solve, task: {self.task.id}")
            return self.solve_pdf_download_captcha(
                response, pdf_link_payload, retries + 1
            )
        return pdf_link_response

    def refresh_token(self, with_app_token=False):
        logger.debug(f"Current session id {self.session_id}")
        answer = self.solve_captcha()
        captcha_check_payload = {
            "captcha": answer,
            "search_opt": "PHRASE",
            "ajax_req": "true",
        }

        res = requests.request(
            "POST",
            self.captcha_token_url,
            headers=self.get_headers(),
            data=captcha_check_payload,
            verify=False,
            timeout=30,
        )
        self.update_session_id(res)
        logger.debug("Refreshed token")

    def request_api(self, method, url, payload, **kwargs):
        headers = self.get_headers()
        logger.debug(f"api_request {self.session_id} {url}")
        response = requests.request(
            method,
            url,
            headers=headers,
            data=payload,
            **kwargs,
            timeout=60,
            verify=False,
        )

        # if response is json
        try:
            response_dict = response.json()
        except Exception as e:
            response_dict = {}

        self.update_session_id(response)

        if url == self.captcha_token_url:
            return response

        if (
            "filename" in response_dict
            and "securimage_show" in response_dict["filename"]
        ):
            return self.solve_pdf_download_captcha(response_dict, payload)

        elif response_dict.get("session_expire") == "Y":
            self.refresh_token()
            return self.request_api(method, url, payload, **kwargs)

        elif "errormsg" in response_dict:
            logger.error(f"Error {response_dict['errormsg']}")
            self.refresh_token()
            return self.request_api(method, url, payload, **kwargs)

        return response

    def get_pdf_output_path(self, pdf_fragment):
        return output_dir / (pdf_fragment.split("#")[0] + ".pdf")

    def is_pdf_downloaded(self, pdf_fragment):
        pdf_metadata_path = self.get_pdf_output_path(pdf_fragment).with_suffix(".json")
        if pdf_metadata_path.exists():
            pdf_metadata = get_json_file(pdf_metadata_path)
            return pdf_metadata["downloaded"]
        return False

    def default_search_payload(self):
        search_payload = urllib.parse.parse_qs(payload)
        search_payload = {k: v[0] for k, v in search_payload.items()}
        search_payload["sEcho"] = 1
        search_payload["iDisplayStart"] = 0
        search_payload["iDisplayLength"] = page_size
        return search_payload

    def default_pdf_link_payload(self):
        pdf_link_payload_o = urllib.parse.parse_qs(pdf_link_payload)
        pdf_link_payload_o = {k: v[0] for k, v in pdf_link_payload_o.items()}
        return pdf_link_payload_o

    def init_user_session(self):
        res = requests.request(
            "GET",
            f"{self.root_url}/scrsearch/",
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            },
            timeout=30,
        )
        self.session_id = res.cookies.get(self.session_cookie_name)
        self.ecourts_token = res.cookies.get(self.ecourts_token_cookie_name)
        if self.ecourts_token is None:
            raise ValueError(
                "Failed to get session token, not expected to happen. This could happen if the IP might have been detected as spam"
            )

    def get_cookie(self):
        return f"{self.ecourts_token_cookie_name}={self.ecourts_token}; {self.session_cookie_name}={self.session_id}"

    def update_session_id(self, response):
        new_session_cookie = response.cookies.get(self.session_cookie_name)
        if new_session_cookie:
            self.session_id = new_session_cookie

    def get_headers(self):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.get_cookie(),
            "DNT": "1",
            "Origin": self.root_url,
            "Pragma": "no-cache",
            "Referer": self.root_url + "/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        return headers

    def download(self):
        """Process a specific date range for Supreme Court"""
        if self.task.from_date is None or self.task.to_date is None:
            logger.info(f"No more data to download for: task: {self.task}")
            return

        search_payload = self.default_search_payload()
        search_payload["from_date"] = self.task.from_date
        search_payload["to_date"] = self.task.to_date
        self.init_user_session()
        results_available = True
        pdfs_downloaded = 0

        logger.info(f"Downloading data for: task: {self.task}")

        while results_available:
            try:
                response = self.request_api("POST", self.search_url, search_payload)
                res_dict = response.json()
                if self._results_exist_in_search_response(res_dict):
                    for idx, row in enumerate(res_dict["reportrow"]["aaData"]):
                        try:
                            is_pdf_downloaded = self.process_result_row(
                                row, row_pos=idx
                            )
                            if is_pdf_downloaded:
                                pdfs_downloaded += 1
                            if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                # after 25 downloads, need to solve captcha for every pdf link request
                                logger.info(
                                    f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task}"
                                )
                                break
                        except Exception as e:
                            logger.error(
                                f"Error processing row {row}: {e}, task: {self.task}"
                            )
                            traceback.print_exc()

                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        self.init_user_session()
                        continue

                    # prepare next iteration
                    search_payload = self._prepare_next_iteration(search_payload)
                else:
                    # No more results for this date range
                    results_available = False
                    # Update tracking data
                    self.tracking_data["last_date"] = self.task.to_date
                    save_tracking_date(self.tracking_data)

            except Exception as e:
                logger.error(f"Error processing task: {self.task}, {e}")
                traceback.print_exc()
                results_available = False

    def download_pdf(self, pdf_fragment, val, citation_year, nc_display):
        """Download PDF for single-language judgment"""
        # prepare temp pdf request
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        pdf_link_payload = self.default_pdf_link_payload()

        # Set all required parameters matching the curl request
        pdf_link_payload["val"] = val
        pdf_link_payload["path"] = pdf_fragment
        pdf_link_payload["citation_year"] = citation_year
        pdf_link_payload["nc_display"] = nc_display
        pdf_link_payload["fcourt_type"] = "3"
        pdf_link_payload["ajax_req"] = "true"

        # For single language judgments, lang_flg might not be needed or empty
        pdf_link_payload["lang_flg"] = ""

        logger.debug(
            f"Downloading PDF, path: {pdf_fragment}, citation_year: {citation_year}, nc_display: {nc_display}"
        )

        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )

        if "outputfile" not in pdf_link_response.json():
            logger.error(
                f"Error downloading pdf, task: {self.task}, Error: {pdf_link_response.json()}"
            )
            return False

        pdf_download_link = pdf_link_response.json()["outputfile"]

        # download pdf and save
        pdf_response = requests.request(
            "GET",
            self.root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
        )
        pdf_output_path.parent.mkdir(parents=True, exist_ok=True)

        # number of response bytes
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0:
            logger.error(
                f"Empty pdf, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        if no_of_bytes == 315:
            logger.error(
                f"404 pdf response, task: {self.task}, output path: {pdf_output_path}"
            )
            return False

        with open(pdf_output_path, "wb") as f:
            f.write(pdf_response.content)
        logger.debug(
            f"Downloaded, task: {self.task}, output path: {pdf_output_path}, size: {no_of_bytes}"
        )
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step", type=int, default=30, help="Number of days per chunk"
    )
    parser.add_argument("--max_workers", type=int, default=1, help="Number of workers")
    args = parser.parse_args()

    run(args.start_date, args.end_date, args.day_step, args.max_workers)

"""
Supreme Court judgment scraper for scr.sci.gov.in
Based on the ecourts scraper but simplified for single court structure
"""
