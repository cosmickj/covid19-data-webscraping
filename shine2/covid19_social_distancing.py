# ! /usr/bin/python3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import sqlalchemy as db  # sudo apt-get install -y python3-mysqldb
from configparser import ConfigParser

from datetime import datetime
import pandas as pd
import warnings

warnings.filterwarnings(action="ignore")

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
engine = db.create_engine(con_str, encoding=CHARSET2, pool_size=20, max_overflow=100)

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

social_distancing_url = "http://ncov.mohw.go.kr/regSocdisBoardView.do?brdId=6&brdGubun=68&ncvContSeq=495"
driver.get(social_distancing_url)

html = driver.page_source
soup = BeautifulSoup(html, "html.parser")

raw_date_element = soup.select_one("div.timetable p.info span").text
raw_date_element = raw_date_element.strip().replace("시", ":00")
raw_date_element = str(datetime.today().year) + "." + raw_date_element
standard_date = pd.to_datetime(raw_date_element)

select_latest_standard_date_query = "SELECT DATE(MAX(standard_date)) FROM covid19_social_distancing"
latest_standard_date = pd.read_sql(
    select_latest_standard_date_query,
    con=engine.connect(),
).iloc[0, 0]

if standard_date.date() > latest_standard_date:
    social_distancing_map = soup.find("div", id="main_maplayout")

    sido_list = []
    status_list = []

    for sido in social_distancing_map.select("span.name"):
        sido_list.append(sido.text[:2])

    for status in social_distancing_map.select("span.num"):
        status_list.append(status.text)

    covid19_social_distancing = pd.DataFrame()
    covid19_social_distancing["sido"] = sido_list
    covid19_social_distancing["status"] = status_list
    covid19_social_distancing["standard_date"] = standard_date

    driver.quit()

    covid19_social_distancing.to_sql(
        name="covid19_social_distancing", con=engine.connect(), if_exists="append", index=False
    )
