# !/usr/bin/python3
from configparser import ConfigParser
from bs4 import BeautifulSoup
import requests

from datetime import datetime
import pandas as pd
import re

from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
}
todayDate = datetime.date(datetime.now())

config = ConfigParser()
config.read("./config/secret.ini")

HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]
CHARSET1 = config["appmd_db"]["CHARSET1"]
CHARSET2 = config["appmd_db"]["CHARSET2"]


def mysqlLatestUpd():
    dbcon = pymysql.connect(
        host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE, charset=CHARSET1
    )
    cursor = dbcon.cursor()
    sql = "SELECT DISTINCT UPD_DATE FROM covid19_domestic_num"
    cursor.execute(sql)
    result = cursor.fetchall()
    latest_upd_date = datetime.date(result[-1][-1])
    dbcon.close()
    return latest_upd_date


def mysqlUpload(domestic, region):
    con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
    engine = create_engine(con_str, encoding=CHARSET2)
    conn = engine.connect()
    domestic.to_sql(name="covid19_domestic_num", con=conn, if_exists="append", index=False)
    region.to_sql(name="covid19_region_num", con=conn, if_exists="append", index=False)


# 시도별 국내 현황
region_url = (
    "http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=13&ncvContSeq=&contSeq=&board_id=&gubun="
)


def requestsSoup(url):
    html = requests.get(url, headers=headers).text
    soup = BeautifulSoup(html, "html.parser")
    return soup


def covidUpdDate(soup):
    date = soup.find("p", class_="info").text
    # date = str(todayDate.year)+'.'+re.sub('[\s가-힣]','',date) 2021년 부터 날짜 포맷이 변경됨
    date = date.replace("년", ".")
    date = re.sub("[\s가-힣]", "", date)
    date = pd.to_datetime(date)
    return date


def regionTable():
    # 기본 테이블 틀 생성
    column_name = [
        "UPD_DATE",
        "SIDO",
        "SIGUN",
        "TOT_NUM",
        "TODAY(TOT)",
        "TODAY(DOM)",
        "TODAY(INTL)",
        "RLSE(TOT)",
        "RLSE(NEW)",
        "QUAR(TOT)",
        "QUAR(NEW)",
        "DTH(TOT)",
        "DTH(NEW)",
    ]
    table_region = pd.DataFrame(columns=column_name)

    # 질본 테이블 전처리
    raw_table = pd.read_html(str(region_soup.find("table", {"class": "num midsize"})))[0]
    raw_table.columns = [
        "SIDO",
        "TODAY(TOT)",
        "TODAY(DOM)",
        "TODAY(INTL)",
        "TOT_NUM",
        "QUAR(TOT)",
        "RLSE(TOT)",
        "DTH(TOT)",
        "nonuse",
    ]
    raw_table["SIGUN"] = raw_table["SIDO"]
    raw_table.drop(["nonuse"], axis=1, inplace=True)
    raw_table = raw_table[1:-1].reset_index(drop=True)

    # DB테이블로 변환하기
    table_region = pd.concat([table_region, raw_table])
    table_region["UPD_DATE"] = covid19_upd_date
    return table_region


# 국내 누적 확진자 현황
def domesticTable():
    domestic_url = (
        "http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=11&ncvContSeq=&contSeq=&board_id=&gubun="
    )
    domestic_soup = requestsSoup(domestic_url)

    # 기본 테이블 틀 생성
    column_name = [
        "UPD_DATE",
        "TOT_NUM",
        "TODAY(TOT)",
        "TODAY(DOM)",
        "TODAY(INTL)",
        "RLSE(TOT)",
        "RLSE(NEW)",
        "QUAR(TOT)",
        "QUAR(NEW)",
        "DTH(TOT)",
        "DTH(NEW)",
    ]
    corona_domestic = pd.DataFrame(columns=column_name)

    # 오늘 누적 확진자 현황 수집
    domestic_data = []
    domestic_data.append(domestic_soup.select("dd.ca_value")[0].text)  # 확진환자 누계
    domestic_data.append(domestic_soup.select("dd.ca_value p")[0].text)  # 확진환자 오늘 추가
    domestic_data.append(domestic_soup.select("dd.ca_value p")[1].text)  # 확진환자 오늘 추가 - 국내
    domestic_data.append(domestic_soup.select("dd.ca_value p")[2].text)  # 확진환자 오늘 추가 - 해외
    domestic_data.append(domestic_soup.select("dd.ca_value")[2].text)  # 격리해제 누적
    domestic_data.append(domestic_soup.select("dd.ca_value")[3].text)  # 격리해제 오늘 추가
    domestic_data.append(domestic_soup.select("dd.ca_value")[4].text)  # 격리중 누적
    domestic_data.append(domestic_soup.select("dd.ca_value")[5].text)  # 격리중 오늘 증감
    domestic_data.append(domestic_soup.select("dd.ca_value")[6].text)  # 사망 누적
    domestic_data.append(domestic_soup.select("dd.ca_value")[7].text)  # 사망 오늘 추가

    # 오늘 누적 확진자 현황 데이터 전처리
    domestic_data = [re.sub("[\+\,\s]", "", i) for i in domestic_data]
    domestic_data.insert(0, covid19_upd_date)

    # 통합 테이블 생성 및 데이터 타입 변경
    corona_domestic.loc[len(corona_domestic), :] = domestic_data  # 정보 통합
    corona_domestic[corona_domestic.columns[1:]] = corona_domestic[corona_domestic.columns[1:]].astype(int)
    corona_domestic["UPD_DATE"] = pd.to_datetime(corona_domestic["UPD_DATE"])
    return corona_domestic


try:
    if mysqlLatestUpd() == todayDate:
        print(f"We already have {todayDate} data.")
    else:
        # 새 업로드 확인
        region_soup = requestsSoup(region_url)
        covid19_upd_date = covidUpdDate(region_soup)

        if covid19_upd_date == datetime.today().date():
            domestic_table = domesticTable()
            region_table = regionTable()
            mysqlUpload(domestic_table, region_table)

            print(f"{todayDate} mysql upload complete.")
        else:
            print("It isn't uploaded yet.")
except Exception as e:
    print(e)
    pass
