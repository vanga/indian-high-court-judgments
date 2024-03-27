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
import pandas as pd


reader = easyocr.Reader(["en"])

root_url = "https://judgments.ecourts.gov.in"
url = f"{root_url}/pdfsearch/?p=pdf_search/home/"
captcha_url = f"{root_url}/pdfsearch/vendor/securimage/securimage_show.php"
captch_token_url = f"{root_url}/pdfsearch/?p=pdf_search/checkCaptcha"
output_dir = Path("./data")

payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=27~1&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&int_fin_party_val=undefined&int_fin_case_val=undefined&int_fin_court_val=undefined&int_fin_decision_val=undefined&act=&sel_search_by=undefined&sections=undefined&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=2&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ajax_req=true&app_token=1fbc7fbb840eb95975c684565909fe6b3b82b8119472020ff10f40c0b1c901fe"
headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": "Judge_Ecourts_Token=!2Y2LYrE/lsuCOSSUizlIzVEN2IeK+WxMnua8ZYFUpwjXK1w1jz8RhpWAScsfjDoIes6wjkTc2CeiciDQx1Yra6VCW92Uek3hoR8/BxZ3; PHPSESSID=lftt1d8mbba0svk85a5pqf1niq",
    "DNT": "1",
    "Origin": "https://judgments.ecourts.gov.in",
    "Referer": "https://judgments.ecourts.gov.in/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

pdf_link_url = f"{root_url}/pdfsearch/?p=pdf_search/openpdfcaptcha"
pdf_link_payload = "val=0&lang_flg=undefined&path=cnrorders/taphc/orders/2017/HBHC010262202017_1_2047-06-29.pdf#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined&ajax_req=true&app_token=c64944b84c687f501f9692e239e2a0ab007eabab497697f359a2f62e4fcd3d10"

pdf_link_url_wo_captch = f"{root_url}/pdfsearch/?p=pdf_search/openpdf"


page_size = 1000
session_id = "gorei1o9u139odm7fsaj5qipis"
app_token = "09b296bd4c14c8ae7837e6b60fbe5336d973bee84beb497da6180d768024ff82"
init_session_id = "gorei1o9u139odm7fsaj5qipis"
init_app_token = "09b296bd4c14c8ae7837e6b60fbe5336d973bee84beb497da6180d768024ff82"


def get_json_file(file_path):
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
        json.dump(tracking_data, f, indent=4)


def save_court_tracking_date(court_code, court_tracking):
    tracking_data = get_tracking_data()
    tracking_data[court_code] = court_tracking
    save_tracking_data(tracking_data)


def default_search_payload():
    search_payload = urllib.parse.parse_qs(payload)
    search_payload = {k: v[0] for k, v in search_payload.items()}
    search_payload["sEcho"] = 1
    search_payload["iDisplayStart"] = 0
    search_payload["iDisplayLength"] = page_size
    return search_payload


def default_pdf_link_payload():
    pdf_link_payload_o = urllib.parse.parse_qs(pdf_link_payload)
    pdf_link_payload_o = {k: v[0] for k, v in pdf_link_payload_o.items()}
    return pdf_link_payload_o


def init_user_session():
    res = requests.request("GET", "https://judgments.ecourts.gov.in/pdfsearch/")
    session_cookie = res.cookies.get("PHPSESSID")
    ecourts_token_cookie = res.cookies.get("Judge_Ecourts_Token")
    return ecourts_token_cookie, session_cookie


def get_new_date_range(last_date: str) -> tuple[str | None, str | None]:
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


def update_headers_with_new_session(headers, new_session_id):
    cookie = SimpleCookie()
    cookie.load(headers["Cookie"])
    cookie["PHPSESSID"] = new_session_id
    headers["Cookie"] = cookie.output(header="", sep=";").strip()


def extract_pdf_fragment(html_attribute):
    pattern = r"javascript:open_pdf\('.*?','.*?','(.*?)'\)"
    match = re.search(pattern, html_attribute)
    if match:
        return match.group(1).split("#")[0]
    return None


def solve_captcha(retries=0, captcha_url=captcha_url):
    # download captch image and save
    captcha_response = requests.get(captcha_url, headers={"Cookie": headers["Cookie"]})
    captcha_filename = "captcha1.png"
    with open(captcha_filename, "wb") as f:
        f.write(captcha_response.content)
    result = reader.readtext(captcha_filename)
    captch_text = result[0][1].strip()
    # strip, remove any special characters present anywhere
    # final text should be 6 characters long
    captch_text = "".join([c for c in captch_text if c.isnumeric()])
    if len(captch_text) != 6:
        if retries > 5:
            raise Exception("Captcha not solved")
        return solve_captcha(retries + 1)
    return captch_text


def solve_pdf_download_captcha(response, pdf_link_payload, retries=0):
    """
    example response: <div class="col-md-auto p-0 ms-3"><img style="padding-right: 0px;border:1px solid #ccc;" id="captcha_image_pdf" src="/pdfsearch/vendor/securimage/securimage_show.php?630074f111af510939c620c884aba1ca" alt="CAPTCHA"   tabindex="0" width="120"/><div id="captcha_image_pdf_audio_div" style="display:inline"><audio id="captcha_image_pdf_audio" preload="none" style="display: none"><source id="captcha_image_pdf_source_wav" src="/pdfsearch/vendor/securimage/securimage_play.php?id=66026662d9542" type="audio/wav"><object type="application/x-shockwave-flash" data="/pdfsearch/vendor/securimage/securimage_play.swf?bgcol=%23ffffff&amp;icon_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fimages%2Fspeaker-btn.png&amp;audio_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fsecurimage_play.php%3F" height="32" width="32"><param name="movie" value="/pdfsearch/vendor/securimage/securimage_play.swf?bgcol=%23ffffff&amp;icon_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fimages%2Fspeaker-btn.png&amp;audio_file=%2Fpdfsearch%2Fvendor%2Fsecurimage%2Fsecurimage_play.php%3F" /></object></audio></div><div id="captcha_image_pdf_audio_controls"  style="display:inline-block;"><a tabindex="0" class="captcha_play_button" href="/pdfsearch/vendor/securimage/securimage_play.php?id=66026662d9551" onclick="return false"><img class="captcha_play_image" height="32" width="32" src="/pdfsearch/vendor/securimage/images/speaker-btn.png" alt="Play CAPTCHA Audio" style="border: 0px"><img class="captcha_loading_image rotating" height="32" width="32" src="/pdfsearch/vendor/securimage/images/loading.png" alt="Loading audio" style="display: none"></a><noscript>Enable Javascript for audio controls</noscript></div><script src="/pdfsearch/vendor/securimage/securimage.js"></script><script>captcha_image_pdf_audioObj = new SecurimageAudio({ audioElement: 'captcha_image_pdf_audio', controlsElement: 'captcha_image_pdf_audio_controls' });</script><a tabindex="0" style="border: 0" href="#" title="Refresh Image" onclick="if (typeof window.captcha_image_pdf_audioObj !== 'undefined') captcha_image_pdf_audioObj.refresh(); document.getElementById('captcha_image_pdf').src = '/pdfsearch/vendor/securimage/securimage_show.php?' + Math.random(); this.blur(); return false"><img height="32" width="32" src="/pdfsearch/vendor/securimage/images/refresh-btn.png" alt="Refresh Image" onclick="clearCaptchaText();" style="border: 0px;" /></a></div> <div class="col-md-4"><input maxlength="6" type="text" name="captchapdf" id="captchapdf" class="captchaClass form-control form-control-sm" placeholder="Enter captcha"  tabindex="0"/></div>
    """

    # parse html
    html_str = response["filename"]
    html = LH.fromstring(html_str)
    img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
    img_src = root_url + img_src
    # download captch image and save
    captcha_text = solve_captcha(captcha_url=img_src)
    pdf_link_payload["captcha1"] = captcha_text
    pdf_link_payload["app_token"] = response["app_token"]
    pdf_link_response = request_api(
        "POST", pdf_link_url_wo_captch, headers, pdf_link_payload
    )
    res_json = pdf_link_response.json()
    if "message" in res_json and res_json["message"] == "Captcha not solved":
        print("Captcha not solved", pdf_link_response.json())
        if retries == 2:
            return res_json
        print("Retrying pdf captch solve")
        return solve_pdf_download_captcha(response, pdf_link_payload, retries + 1)
    return pdf_link_response


def refresh_token(with_app_token=False):
    global session_id, app_token
    print("Current session id ", session_id)
    print("Current token ", app_token)
    update_headers_with_new_session(headers, session_id)
    captch_text = solve_captcha()
    captch_check_payload = {
        "captcha": captch_text,
        "search_opt": "PHRASE",
        "ajax_req": "true",
        # "app_token": app_token,
    }
    if with_app_token:
        captch_check_payload["app_token"] = app_token
    res = requests.request(
        "POST", captch_token_url, headers=headers, data=captch_check_payload
    )
    res_json = res.json()
    app_token = res_json["app_token"]
    session_id = res.cookies.get("PHPSESSID")

    print("Refreshed token")


def request_api(method, url, headers, payload, **kwargs):
    global session_id, app_token
    # print(
    #     "api_request",
    #     session_id,
    #     payload.get("app_token") if payload else None,
    #     url,
    # )
    response = requests.request(
        method, url, headers=headers, data=payload, **kwargs, timeout=60
    )
    # if response is json
    try:
        response_dict = response.json()
    except Exception as e:
        response_dict = {}
    if "app_token" in response_dict:
        app_token = response_dict["app_token"]

    new_session_cookie = response.cookies.get("PHPSESSID")
    if new_session_cookie:
        session_id = new_session_cookie

    if url == captch_token_url:
        return response

    if "filename" in response_dict and "securimage_show" in response_dict["filename"]:
        app_token = response_dict["app_token"]
        update_headers_with_new_session(headers, session_id)
        return solve_pdf_download_captcha(response_dict, payload)

    elif response_dict.get("session_expire") == "Y":
        refresh_token()
        update_headers_with_new_session(headers, session_id)
        if payload:
            payload["app_token"] = app_token
        return request_api(method, url, headers, payload, **kwargs)

    elif "errormsg" in response_dict:

        refresh_token()
        update_headers_with_new_session(headers, session_id)
        if payload:
            payload["app_token"] = app_token
        return request_api(method, url, headers, payload, **kwargs)

    return response


def get_pdf_output_path(pdf_fragment):
    return output_dir / pdf_fragment.split("#")[0]


def download_pdf(pdf_fragment, row_pos):
    global app_token
    # prepare temp pdf request
    pdf_output_path = get_pdf_output_path(pdf_fragment)
    if pdf_output_path.exists() and pdf_output_path.stat().st_size > 0:
        return pdf_output_path
    pdf_link_payload = default_pdf_link_payload()
    pdf_link_payload["path"] = pdf_fragment
    pdf_link_payload["val"] = row_pos
    pdf_link_payload["app_token"] = app_token
    update_headers_with_new_session(headers, session_id)
    pdf_link_response = request_api("POST", pdf_link_url, headers, pdf_link_payload)
    if "outputfile" not in pdf_link_response.json():
        print("Error downloading pdf", pdf_link_response.json())
        return False
    pdf_download_link = pdf_link_response.json()["outputfile"]

    # download pdf and save
    pdf_response = requests.request("GET", root_url + pdf_download_link)
    pdf_output_path.parent.mkdir(parents=True, exist_ok=True)
    # number of response butes
    no_of_bytes = len(pdf_response.content)
    if no_of_bytes == 0:
        print("Empty pdf", pdf_output_path)
        return False
    with open(pdf_output_path, "wb") as f:
        f.write(pdf_response.content)
    print("Downloaded", pdf_output_path, no_of_bytes)
    return True


def process_result_row(row, row_pos):
    html = row[1]
    soup = BeautifulSoup(html, "html.parser")
    html_element = LH.fromstring(html)
    title = html_element.xpath("./button/font/text()")[0]
    description = html_element.xpath("./text()")[0]
    case_details = html_element.xpath("./strong//text()")

    pdf_fragment = extract_pdf_fragment(soup.button["onclick"])
    is_downloaded = download_pdf(pdf_fragment, row_pos)
    pdf_output_path = get_pdf_output_path(pdf_fragment)
    metadata_output = pdf_output_path.with_suffix(".json")
    metadata = {
        "title": title,
        "description": description,
        "case_details": case_details,
        "pdf_link": pdf_fragment,
    }
    with open(metadata_output, "w") as f:
        json.dump(metadata, f)
    return is_downloaded


def process_court(code, name, court_tracking):
    global app_token, session_id
    last_date = court_tracking.get("last_date", "2023-12-31")
    from_date, to_date = get_new_date_range(last_date)
    if from_date is None:
        print("No more data to download for: ", code, name)
        return
    search_payload = default_search_payload()
    search_payload["from_date"] = from_date
    search_payload["to_date"] = to_date
    ecourts_token, session_id = init_user_session()
    headers["Cookie"] = f"Judge_Ecourts_Token={ecourts_token}; PHPSESSID={session_id}"
    search_payload["state_code"] = code
    search_payload["app_token"] = app_token
    results_available = True
    while results_available:
        update_headers_with_new_session(headers, session_id)
        response = request_api("POST", url, headers, search_payload)
        try:
            res_dict = response.json()
            if (
                "reportrow" in res_dict
                and "aaData" in res_dict["reportrow"]
                and len(res_dict["reportrow"]["aaData"]) > 0
            ):
                no_of_results = len(res_dict["reportrow"]["aaData"])
                print("Found results", no_of_results)
                pdfs_downloaded = 0
                for idx, row in tqdm(enumerate(res_dict["reportrow"]["aaData"])):
                    try:
                        is_pdf_downloaded = process_result_row(row, row_pos=idx)
                        if is_pdf_downloaded:
                            pdfs_downloaded += 1
                        else:
                            court_tracking["failed_dates"] = court_tracking.get(
                                "failed_dates", []
                            )
                            court_tracking["failed_dates"].append(from_date)
                    except Exception as e:
                        print(e)
                        traceback.print_stack(e)
                        print("Error processing row", row)

                # prepare next iteration
                search_payload["sEcho"] += 1
                search_payload["iDisplayStart"] += page_size
                print("Next iteration: ", search_payload["iDisplayStart"])
            else:
                last_date = to_date
                court_tracking["last_date"] = last_date
                save_court_tracking_date(code, court_tracking)
                from_date, to_date = get_new_date_range(to_date)
                if from_date is None:
                    print("No more data to download for: ", code, name)
                    results_available = False
                else:
                    search_payload["from_date"] = from_date
                    search_payload["to_date"] = to_date
                    search_payload["sEcho"] = 1
                    search_payload["iDisplayStart"] = 0
                    search_payload["iDisplayLength"] = page_size
                    print(
                        f"Downloading data for: {code}, court: {name}, from: {from_date},to: {to_date}"
                    )

        except Exception as e:
            print(e)
            court_tracking["failed_dates"] = court_tracking.get("failed_dates", [])
            court_tracking["failed_dates"].append(
                from_date
            )  # TODO: should be all the dates from from_date to to_date in case step date > 1
            save_court_tracking_date(code, court_tracking)


def run():
    tracking_data = get_tracking_data()
    court_codes = get_court_codes()

    for code, court in court_codes.items():
        try:
            court_tracking = tracking_data.get(code, {})
            process_court(code, court, court_tracking)
        except Exception as e:
            print(e)
            traceback.print_exc()
            print("Error processing court", code, court)
            continue


if __name__ == "__main__":
    run()

"""
captcha prompt while downloading pdf seems to be different from session timeout
Every search API request returns a new app_token in response payload and new PHPSESSID in response cookies that need to be sent in the next request.
openpdfcaptcha request refreshes the app_token but not PHPSESSID

"""
