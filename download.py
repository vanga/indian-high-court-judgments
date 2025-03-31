from tqdm import tqdm
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

import threading
import concurrent.futures
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

reader = easyocr.Reader(["en"])

root_url = "https://judgments.ecourts.gov.in"
output_dir = Path("./data")

payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=27~1&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&int_fin_party_val=undefined&int_fin_case_val=undefined&int_fin_court_val=undefined&int_fin_decision_val=undefined&act=&sel_search_by=undefined&sections=undefined&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=2&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ajax_req=true&app_token=1fbc7fbb840eb95975c684565909fe6b3b82b8119472020ff10f40c0b1c901fe"


pdf_link_payload = "val=0&lang_flg=undefined&path=cnrorders/taphc/orders/2017/HBHC010262202017_1_2047-06-29.pdf#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined&ajax_req=true&app_token=c64944b84c687f501f9692e239e2a0ab007eabab497697f359a2f62e4fcd3d10"

page_size = 1000
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()
MAX_WORKERS = 1


def get_json_file(file_path) -> dict:
    with open(file_path) as f:
        return json.load(f)


def get_court_codes():
    court_codes = get_json_file("./court-codes.json")
    return court_codes


def get_tracking_data():
    tracking_data = get_json_file("./track.json")
    return tracking_data


def save_tracking_data(tracking_data):
    with open("./track.json", "w") as f:
        json.dump(tracking_data, f)


def save_court_tracking_date(court_code, court_tracking):
    # acquire a lock
    lock.acquire()
    tracking_data = get_tracking_data()
    tracking_data[court_code] = court_tracking
    save_tracking_data(tracking_data)
    # release the lock
    lock.release()


class Downloader:
    def __init__(self, court_code):
        self.root_url = "https://judgments.ecourts.gov.in"
        self.search_url = f"{self.root_url}/pdfsearch/?p=pdf_search/home/"
        self.captcha_url = f"{self.root_url}/pdfsearch/vendor/securimage/securimage_show.php"  # not lint skip/
        self.captcha_token_url = f"{self.root_url}/pdfsearch/?p=pdf_search/checkCaptcha"
        self.pdf_link_url = f"{self.root_url}/pdfsearch/?p=pdf_search/openpdfcaptcha"
        self.pdf_link_url_wo_captcha = f"{root_url}/pdfsearch/?p=pdf_search/openpdf"

        self.court_code = court_code
        self.tracking_data = get_tracking_data()
        self.court_codes = get_court_codes()
        self.court_name = self.court_codes[self.court_code]
        self.court_tracking = self.tracking_data.get(self.court_code, {})
        self.session_cookie_name = "JUDGEMENTSSEARCH_SESSID"
        self.ecourts_token_cookie_name = "JSESSION"
        self.session_id = None
        self.ecourts_token = None
        self.app_token = (
            "490a7e9b99e4553980213a8b86b3235abc51612b038dbdb1f9aa706b633bbd6c"
        )

    def download(self):
        try:
            self.process_court()
        except Exception as e:
            print(e)
            traceback.print_exc()
            print("Error processing court", self.court_code, self.court_name)

    def process_court(self):
        last_date = self.court_tracking.get("last_date", "2023-12-31")
        from_date, to_date = self.get_new_date_range(last_date)
        if from_date is None:
            print("No more data to download for: ", self.court_code, self.court_name)
            return
        search_payload = self.default_search_payload()
        search_payload["from_date"] = from_date
        search_payload["to_date"] = to_date
        self.init_user_session()
        search_payload["state_code"] = self.court_code
        search_payload["app_token"] = self.app_token
        results_available = True
        pdfs_downloaded = 0

        while results_available:
            response = self.request_api("POST", self.search_url, search_payload)
            try:
                res_dict = response.json()
                if (
                    "reportrow" in res_dict
                    and "aaData" in res_dict["reportrow"]
                    and len(res_dict["reportrow"]["aaData"]) > 0
                ):
                    no_of_results = len(res_dict["reportrow"]["aaData"])
                    print(
                        f"Found {no_of_results} results for {self.court_code}, {self.court_name}, from: {from_date}, to: {to_date}"
                    )
                    for idx, row in enumerate(res_dict["reportrow"]["aaData"]):
                        try:
                            is_pdf_downloaded = self.process_result_row(
                                row, row_pos=idx
                            )
                            if is_pdf_downloaded:
                                pdfs_downloaded += 1
                            else:
                                self.court_tracking["failed_dates"] = (
                                    self.court_tracking.get("failed_dates", [])
                                )
                                if from_date not in self.court_tracking["failed_dates"]:
                                    self.court_tracking["failed_dates"].append(
                                        from_date
                                    )
                            if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                # after 25 downloads, need to solve captcha for every pdf link request. Starting with a fresh session would be faster so that we get another 25 downloads without captcha
                                print(
                                    f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session"
                                )
                                break

                        except Exception as e:
                            print(e)
                            traceback.print_stack(e)
                            print("Error processing row", row)
                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        self.init_user_session()
                        search_payload["app_token"] = self.app_token
                        continue
                    # prepare next iteration
                    search_payload["sEcho"] += 1
                    search_payload["iDisplayStart"] += page_size
                    print("Next iteration: ", search_payload["iDisplayStart"])
                else:
                    last_date = to_date
                    self.court_tracking["last_date"] = last_date
                    save_court_tracking_date(self.court_code, self.court_tracking)
                    from_date, to_date = self.get_new_date_range(to_date)
                    if from_date is None:
                        print(
                            "No more data to download for: ",
                            self.court_code,
                            self.court_name,
                        )
                        results_available = False
                    else:
                        search_payload["from_date"] = from_date
                        search_payload["to_date"] = to_date
                        search_payload["sEcho"] = 1
                        search_payload["iDisplayStart"] = 0
                        search_payload["iDisplayLength"] = page_size
                        print(
                            f"Downloading data for: {self.court_code}, court: {self.court_name}, from: {from_date},to: {to_date}"
                        )

            except Exception as e:
                print(e)
                self.court_tracking["failed_dates"] = self.court_tracking.get(
                    "failed_dates", []
                )
                if from_date not in self.court_tracking["failed_dates"]:
                    self.court_tracking["failed_dates"].append(
                        from_date
                    )  # TODO: should be all the dates from from_date to to_date in case step date > 1
                save_court_tracking_date(self.court_code, self.court_tracking)

    def process_result_row(self, row, row_pos):
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")
        # html_element = LH.fromstring(html)
        # why am I using both LH and BS4? idk.
        # title = html_element.xpath("./button/font/text()")[0]
        # description = html_element.xpath("./text()")[0]
        # case_details = html_element.xpath("./strong//text()")
        # check if button with onclick is present
        if not (soup.button and "onclick" in soup.button.attrs):
            print("No button found, likely multi language judgment")
            with open("html-parse-failures.txt", "a") as f:
                f.write(html + "\n")
            # TODO: requires special parsing
            return False
        pdf_fragment = self.extract_pdf_fragment(soup.button["onclick"])
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        is_pdf_downloaded = self.is_pdf_downloaded(pdf_fragment)
        is_fresh_download = not is_pdf_downloaded
        if not is_pdf_downloaded:
            is_fresh_download = self.download_pdf(pdf_fragment, row_pos)
        metadata_output = pdf_output_path.with_suffix(".json")
        metadata = {
            "court_code": self.court_code,
            "court_name": self.court_name,
            "raw_html": html,
            # "title": title,
            # "description": description,
            # "case_details": case_details,
            "pdf_link": pdf_fragment,
            "downloaded": is_pdf_downloaded or is_fresh_download,
        }
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_output, "w") as f:
            json.dump(metadata, f)
        return is_fresh_download

    def download_pdf(self, pdf_fragment, row_pos):
        # prepare temp pdf request
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        pdf_link_payload = self.default_pdf_link_payload()
        pdf_link_payload["path"] = pdf_fragment
        pdf_link_payload["val"] = row_pos
        pdf_link_payload["app_token"] = self.app_token
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )
        if "outputfile" not in pdf_link_response.json():
            print("Error downloading pdf", pdf_link_response.json())
            return False
        pdf_download_link = pdf_link_response.json()["outputfile"]

        # download pdf and save
        pdf_response = requests.request(
            "GET",
            root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
        )
        pdf_output_path.parent.mkdir(parents=True, exist_ok=True)
        # number of response butes
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0:
            print("Empty pdf", pdf_output_path)
            return False
        if no_of_bytes == 315:
            print("404 pdf response")
            return False
        with open(pdf_output_path, "wb") as f:
            f.write(pdf_response.content)
        print(f"Downloaded {pdf_output_path}, size: {no_of_bytes}")
        return True

    def update_headers_with_new_session(self, headers):
        cookie = SimpleCookie()
        cookie.load(headers["Cookie"])
        cookie[self.session_cookie_name] = self.session_id
        headers["Cookie"] = cookie.output(header="", sep=";").strip()

    def extract_pdf_fragment(self, html_attribute):
        pattern = r"javascript:open_pdf\('.*?','.*?','(.*?)'\)"
        match = re.search(pattern, html_attribute)
        if match:
            return match.group(1).split("#")[0]
        return None

    def solve_math_expression(self, expression):
        # credits to: https://github.com/NoelShallum
        expression = expression.strip().replace(" ", "")
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

    def is_match_expression(self, expression):
        separators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        for separator in separators:
            if separator in expression:
                return True
        return False

    def solve_captcha(self, retries=0, captcha_url=None):
        if retries > 5:
            raise ValueError("Failed to solve captcha")
        if captcha_url is None:
            captcha_url = self.captcha_url
        # download captch image and save
        captcha_response = requests.get(
            captcha_url, headers={"Cookie": self.get_cookie()}, verify=False
        )
        captcha_filename = f"/tmp/captcha{self.court_code}.png"
        with open(captcha_filename, "wb") as f:
            f.write(captcha_response.content)
        result = reader.readtext(captcha_filename)
        Path(captcha_filename).unlink()
        captch_text = result[0][1].strip()
        if self.is_match_expression(captch_text):
            answer = self.solve_math_expression(captch_text)
        else:
            self.solve_captcha(retries + 1, captcha_url)
        return answer

    def solve_pdf_download_captcha(self, response, pdf_link_payload, retries=0):
        """
        example response: <div class="col-md-auto p-0 ms-3"><img style="padding-right: 0px;border:1px solid #ccc;" id="captcha_image_pdf" src="/pdfsearch/vendor/securimage/securimage_show.php?630074f111af510939c620c884aba1ca" alt="CAPTCHA"   tabindex="0" width="120"/><div id="captcha_image_pdf_audio_div" style="display:inline"><audio id="captcha_image_pdf_audio" preload="none" style="display: none"><source id="captcha_image_pdf_source_wav" src="/pdfsearch/vendor/securimage/securimage_play.php?id=66026662d9542" type="audio/wav"><object type="application/x-shockwave-flash" data="/pdfsearch/vendor/securimage/securimage_play.swf?bgcol=%23ffffff&amp;icon_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fimages%2Fspeaker-btn.png&amp;audio_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fsecurimage_play.php%3F" height="32" width="32"><param name="movie" value="/pdfsearch/vendor/securimage/securimage_play.swf?bgcol=%23ffffff&amp;icon_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fimages%2Fspeaker-btn.png&amp;audio_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fsecurimage_play.php%3F" /></object></audio></div><div id="captcha_image_pdf_audio_controls"  style="display:inline-block;"><a tabindex="0" class="captcha_play_button" href="/pdfsearch/vendor/securimage/securimage_play.php?id=66026662d9551" onclick="return false"><img class="captcha_play_image" height="32" width="32" src="/pdfsearch/vendor/securimage/images/speaker-btn.png" alt="Play CAPTCHA Audio" style="border: 0px"><img class="captcha_loading_image rotating" height="32" width="32" src="/pdfsearch/vendor/securimage/images/loading.png" alt="Loading audio" style="display: none"></a><noscript>Enable Javascript for audio controls</noscript></div><script src="/pdfsearch/vendor/securimage/securimage.js"></script><script>captcha_image_pdf_audioObj = new SecurimageAudio({ audioElement: 'captcha_image_pdf_audio', controlsElement: 'captcha_image_pdf_audio_controls' });</script><a tabindex="0" style="border: 0" href="#" title="Refresh Image" onclick="if (typeof window.captcha_image_pdf_audioObj !== 'undefined') captcha_image_pdf_audioObj.refresh(); document.getElementById('captcha_image_pdf').src = '/pdfsearch/vendor/securimage/securimage_show.php?' + Math.random(); this.blur(); return false"><img height="32" width="32" src="/pdfsearch/vendor/securimage/images/refresh-btn.png" alt="Refresh Image" onclick="clearCaptchaText();" style="border: 0px;" /></a></div> <div class="col-md-4"><input maxlength="6" type="text" name="captchapdf" id="captchapdf" class="captchaClass form-control form-control-sm" placeholder="Enter captcha"  tabindex="0"/></div>
        """

        # parse html
        html_str = response["filename"]
        html = LH.fromstring(html_str)
        img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
        img_src = root_url + img_src
        # download captch image and save
        captcha_text = self.solve_captcha(captcha_url=img_src)
        pdf_link_payload["captcha1"] = captcha_text
        pdf_link_payload["app_token"] = response["app_token"]
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url_wo_captcha, pdf_link_payload
        )
        res_json = pdf_link_response.json()
        if "message" in res_json and res_json["message"] == "Captcha not solved":
            print("Captcha not solved", pdf_link_response.json())
            if retries == 2:
                return res_json
            print("Retrying pdf captch solve")
            return self.solve_pdf_download_captcha(
                response, pdf_link_payload, retries + 1
            )
        return pdf_link_response

    def refresh_token(self, with_app_token=False):
        print("Current session id ", self.session_id)
        print("Current token ", self.app_token)
        answer = self.solve_captcha()
        captcha_check_payload = {
            "captcha": answer,
            "search_opt": "PHRASE",
            "ajax_req": "true",
        }
        if with_app_token:
            captcha_check_payload["app_token"] = self.app_token
        res = requests.request(
            "POST",
            self.captcha_token_url,
            headers=self.get_headers(),
            data=captcha_check_payload,
            verify=False,
        )
        res_json = res.json()
        self.app_token = res_json["app_token"]
        self.update_session_id(res)
        print("Refreshed token")

    def request_api(self, method, url, payload, **kwargs):
        headers = self.get_headers()
        # print(
        #     "api_request",
        #     session_id,
        #     payload.get("app_token") if payload else None,
        #     url,
        # )
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
        if "app_token" in response_dict:
            self.app_token = response_dict["app_token"]
        self.update_session_id(response)
        if url == self.captcha_token_url:
            return response

        if (
            "filename" in response_dict
            and "securimage_show" in response_dict["filename"]
        ):
            self.app_token = response_dict["app_token"]
            return self.solve_pdf_download_captcha(response_dict, payload)

        elif response_dict.get("session_expire") == "Y":
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        elif "errormsg" in response_dict:
            print("Error", response_dict["errormsg"])
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        return response

    def get_pdf_output_path(self, pdf_fragment):
        return output_dir / pdf_fragment.split("#")[0]

    def is_pdf_downloaded(self, pdf_fragment):
        pdf_metadata_path = self.get_pdf_output_path(pdf_fragment).with_suffix(".json")
        if pdf_metadata_path.exists():
            pdf_metadata = get_json_file(pdf_metadata_path)
            return pdf_metadata["downloaded"]
        return False

    def get_search_url(self):
        return f"{self.root_url}/pdfsearch/?p=pdf_search/home/"

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
            "GET", "https://judgments.ecourts.gov.in/pdfsearch/", verify=False
        )
        self.session_id = res.cookies.get(self.session_cookie_name)
        self.ecourts_token = res.cookies.get(self.ecourts_token_cookie_name)

    def get_cookie(self):
        return f"{self.ecourts_token_cookie_name}={self.ecourts_token}; {self.session_cookie_name}={self.session_id}"

    def update_session_id(self, response):
        new_session_cookie = response.cookies.get(self.session_cookie_name)
        if new_session_cookie:
            self.session_id = new_session_cookie

    def get_new_date_range(self, last_date: str) -> tuple[str | None, str | None]:
        day_step = 1
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

    def get_headers(self):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.get_cookie(),
            "DNT": "1",
            "Origin": self.root_url,
            "Referer": self.root_url + "/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        return headers


def run():
    court_codes = get_court_codes()

    def process(court_code):
        try:
            Downloader(court_code).download()
        except Exception as e:
            print(e)
            traceback.print_exc()
            print("Error processing court", court_code, court_codes[court_code])

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for code, name in court_codes.items():
            futures.append(executor.submit(process, code))

        for future in concurrent.futures.as_completed(futures):
            res = future.result()


if __name__ == "__main__":
    run()

"""
captcha prompt while downloading pdf seems to be different from session timeout
Every search API request returns a new app_token in response payload and new PHPSESSID in response cookies that need to be sent in the next request.
openpdfcaptcha request refreshes the app_token but not PHPSESSID

"""
