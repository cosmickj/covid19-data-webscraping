import requests
from bs4 import BeautifulSoup

import re
import pandas as pd
from datetime import datetime

import pymysql
from configparser import ConfigParser

config = ConfigParser()
config.read("../config/secret.ini")

HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]

connection = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, database=DATABASE)
cursor = connection.cursor()

try:
    select_latest_standard_date_query = "SELECT MAX(standard_date) FROM covid19_status_domestic;"
    cursor.execute(select_latest_standard_date_query)
    record = cursor.fetchone()

    if record[0].date() == datetime.today().date():
        print("이미 업데이트 되었습니다.")
    else:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
        }
        url = "http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=11&ncvContSeq=&contSeq=&board_id=&gubun="

        html = requests.get(url, params=headers).text
        soup = BeautifulSoup(html, "html.parser")

        date_element = soup.find("span", class_="t_date").text
        date_element = re.sub("[(가-힣)\s]", "", date_element)
        date_element = str(datetime.now().year) + "." + date_element
        standard_date = pd.to_datetime(date_element)

        if standard_date.date() < datetime.today().date():
            print("새로운 데이터가 업데이트 되지 않았습니다.")
        else:
            covid19_status_domestic_today_params = []
            case_table = soup.find("div", class_="caseTable").select("dd")

            # data pre-processing
            for case in case_table:
                if case.select("p.inner_value"):
                    case_detail = case.select("p.inner_value")
                    covid19_status_domestic_today_params.extend([re.sub("\D", "", i.text) for i in case_detail])
                else:
                    flag = case.text.strip().startswith(("+", "-"))
                    if flag:
                        continue
                    covid19_status_domestic_today_params.append(re.sub("\D", "", case.text))
            covid19_status_domestic_today_params.append(standard_date)

            insert_covid19_status_domestic_today = """
                                                    INSERT INTO covid19_status_domestic (
                                                        confirmed_total,
                                                        confirmed_daily,
                                                        confirmed_daily_domestic,
                                                        confirmed_daily_overseas,
                                                        released_total,
                                                        isolated_total,
                                                        deceased_total,
                                                        standard_date)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                                                    """
            cursor.execute(
                insert_covid19_status_domestic_today,
                covid19_status_domestic_today_params,
            )
            connection.commit()
            print("업데이트 되었습니다.")
finally:
    connection.close()
