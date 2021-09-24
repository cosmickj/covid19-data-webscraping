# ! /usr/bin/python3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from configparser import ConfigParser
from datetime import datetime
import pandas as pd
import time
import json
import re

import warnings

warnings.filterwarnings(action="ignore")

import pymysql

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

# 데이터 베이스 연결 설정
config = ConfigParser()
config.read("../config/secret.ini")

HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]
CHARSET1 = config["appmd_db"]["CHARSET1"]
CHARSET2 = config["appmd_db"]["CHARSET2"]

con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
engine = create_engine(con_str, encoding=CHARSET2, pool_size=20, max_overflow=100)

# 크롬 드라이버 연결 설정
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
DRIVER_LOCATION = "/usr/bin/chromedriver"
BINARY_LOCATION = "/usr/bin/google-chrome"

options = Options()
options.add_argument(f"user-agent={user_agent}")
options.add_argument("--headless")
options.add_argument("no-sandbox")
options.add_argument("--disable-extensions")
options.binary_location = BINARY_LOCATION

driver = webdriver.Chrome(executable_path=DRIVER_LOCATION, options=options)
driver.implicitly_wait(3)


def check_sigungun_update(dbtable, sido):
    """해당 시도 데이터 베이스 업데이트 날짜 확인"""
    sql = f"SELECT MAX(standard_date) FROM {dbtable} WHERE SIDO = '{sido}';"
    sql_df = pd.read_sql(sql, con=engine)
    latest_update_date = sql_df.iloc[0, 0].date()
    return latest_update_date


def update_table(table, dbtable, sido):
    table.to_sql(name=f"{dbtable}", con=engine.connect(), if_exists="append", index=False)
    print(f"{sido} Updated Complete.")


def get_soup():
    html = driver.page_source
    return BeautifulSoup(html, "html.parser")


def clean_standard_date(date_element):
    std = date_element.text.find("기준")
    if std == -1:
        std = None
    date_element = date_element.text[:std]
    update_date_text_list = re.findall("\d+.|오전|오후", date_element)
    update_date_text = "".join(update_date_text_list).strip()
    update_date_text = re.sub("[년월일\-]", ".", update_date_text)
    update_date_text = update_date_text + ":00"

    if "오전" in update_date_text:
        update_date_text += "am"
    elif "오후" in update_date_text:
        update_date_text += "pm"

    update_date_text = re.sub("[가-힣]", "", update_date_text)
    try:
        update_date = pd.to_datetime(update_date_text, yearfirst=True)
    except:
        update_date_text = str(datetime.today().year) + "." + update_date_text
        update_date = pd.to_datetime(update_date_text, yearfirst=True)
    return update_date


def extract_target_html(current_html_tag):
    if str(current_html_tag).startswith("<table"):
        return current_html_tag
    elif str(current_html_tag).startswith("<div"):
        return current_html_tag
    else:
        current_html_tag = current_html_tag.find_parent()
        return extract_target_html(current_html_tag)


def extract_sigungu_name(target_data_html, tag_for_name, sido_kr):
    if tag_for_name == None:
        """시군구 단위를 제공하지 않음: 대구, 세종, 제주"""
        sigungu_name_list = [sido_kr]  # global variable
        return sigungu_name_list

    junky_text_pattern = ("계", "구분", "지역", "증감", "자치구별(지역별)", "코로나19")
    data_dummy = target_data_html.select(tag_for_name)

    if tag_for_name.startswith("img"):
        """시군구 명칭을 img 태그 안에 담아둠: 충북"""
        sigungu_name_list = [
            data["alt"]
            for data in data_dummy
            if not data["alt"].endswith(junky_text_pattern) and len(data["alt"]) > 2
        ]
        return sigungu_name_list

    def filter_data(data, pattern):
        chunk = data.text.split()[0]
        return chunk if not chunk.endswith(pattern) and len(chunk) >= 2 else None

    sigungu_name_list = [
        o for data in data_dummy if (o := filter_data(data, junky_text_pattern))
    ]  # 바다코끼리 연산자(the walrus operator)
    return sigungu_name_list


def extract_covid19_confirmed(target_data_html, tag_for_covid19, covid19_count_start_idx, sigungu_name_list):
    raw_covid19_confirmed_count_list = target_data_html.select(tag_for_covid19)
    raw_covid19_confirmed_count_list = raw_covid19_confirmed_count_list[
        covid19_count_start_idx : covid19_count_start_idx + len(sigungu_name_list)
    ]

    covid19_confirmed_count_list = []
    for count in raw_covid19_confirmed_count_list:
        clean_count_text = re.sub("\(.?\d+\)|\D", "", count.text)
        if clean_count_text:
            covid19_confirmed_count_list.append(int(clean_count_text))

    return covid19_confirmed_count_list


start_time = time.time()

"""MAIN PROCESS"""
with open(
    "../config/covid19_sido_info.json",
    "r",
    encoding="utf-8",
) as f:
    sido_data = json.load(f)

sido_list = [
    "seoul",
    "busan",
    "daegu",
    "incheon",
    "gwangju",
    "daejeon",
    "ulsan",
    "sejong",
    "gyeonggi",
    "gangwon",
    "chungbuk",
    "chungnam",
    "jeonbuk",
    "jeonnam",
    "gyeongbuk",
    "gyeongnam",
    "jeju",
]

for sido in sido_list:
    try:
        sido_kr = sido_data[sido]["sido_kr"]
        dbtable = "covid19_status_sigungu"

        db_sigungu_latest_update_date = check_sigungun_update(dbtable, sido_kr)
        if db_sigungu_latest_update_date == datetime.now().date():
            print(f"{sido_kr} is already updated in database")
            continue

        url = sido_data[sido]["url"]
        driver.get(url)
        driver.refresh()

        # 지자체 홈페이지 업데이트 기준 날짜 가져오기
        css_selector_for_update_date = sido_data[sido]["css_selector_for_update_date"]
        date_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector_for_update_date))
        )

        standard_date = clean_standard_date(date_element)

        if standard_date != datetime.now().date():
            print(f"{sido_kr} 홈페이지에 코로나 현황이 아직 업데이트 되지 않았습니다.")
            continue

        soup = get_soup()
        target_text = sido_data[sido]["target_text"]
        target_text_html = soup.find(string=re.compile(target_text)).find_parent()
        target_data_html = extract_target_html(target_text_html)

        # 해당 시도의 시군구 이름 가져오기
        tag_for_name = sido_data[sido]["tag_for_name"]
        sigungu_name_list = extract_sigungu_name(target_data_html, tag_for_name, sido_kr)
        sigungu_name_list = list(dict.fromkeys(sigungu_name_list))  # remove duplicate

        # 시군구별 누적 코로나 확진자 현황 가져오기
        tag_for_covid19 = sido_data[sido]["tag_for_covid19"]
        covid19_count_start_idx = sido_data[sido]["covid19_count_start_idx"]
        covid19_confirmed_list = extract_covid19_confirmed(
            target_data_html,
            tag_for_covid19,
            covid19_count_start_idx,
            sigungu_name_list,
        )

        covid19_status_sigungu = pd.DataFrame(columns=["sido", "sigungu", "confirmed_total", "standard_date"])
        covid19_status_sigungu["sigungu"] = sigungu_name_list
        covid19_status_sigungu["confirmed_total"] = covid19_confirmed_list
        covid19_status_sigungu["sido"] = sido_kr
        covid19_status_sigungu["standard_date"] = standard_date

        update_table(covid19_status_sigungu, dbtable, sido_kr)
    except Exception as e:
        print(e)

    """
    [행정구역 현황]
    서울: 25개 중 25개 제공
    부산: 16개 중 16개 제공
    대구: 8개 중 0개 제공 (통합)
    인천: 10개 중 10개 제공
    광주: 5개 중 5개 제공
    대전: 5개 중 5개 제공
    울산: 5개 중 5개 제공
    세종: 1개 중 1개 제공
    경기: 31개 중 31개 제공
    강원: 18개 중 18개 제공
    충북: 11개 중 11개 제공
    충남: 15개 중 15개 제공
    전북: 14개 중 14개 제공
    전남: 22개 중 22개 제공
    경북: 23개 중 23개 제공
    경남: 18개 중 18개 제공
    제주: 2개 중 0개 제공 (통합)
    """

print("time:", time.time() - start_time)
driver.quit()
