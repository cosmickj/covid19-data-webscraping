from bs4 import BeautifulSoup
import requests

from datetime import datetime
import pandas as pd
import re

import pymysql
from configparser import ConfigParser

config = ConfigParser()
config.read("/ShineMacro/shine_covid19_status/config/secret.ini")
HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]

connection = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, database=DATABASE)
cursor = connection.cursor()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
}

try:
    select_latest_standard_date_query = "SELECT MAX(standard_date) FROM covid19_status_sido;"
    cursor.execute(select_latest_standard_date_query)
    record = cursor.fetchone()

    if record[0].date() == datetime.today().date():
        print("이미 업데이트 되었습니다.")

    else:
        url = "http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=13&ncvContSeq=&contSeq=&board_id=&gubun="
        html = requests.get(url, params=headers).text
        soup = BeautifulSoup(html, "html.parser")

        date_element = soup.find("p", class_="info").text
        date_element = date_element.replace("년", ".")
        standard_date = pd.to_datetime(re.sub("[^\d\.]", "", date_element))

        if standard_date.date() < datetime.today().date():
            print("금일 데이터가 업데이트 되지 않았습니다.")

        else:
            for i in soup.find("table", class_="num midsize").select("tbody tr[class != sumline]"):
                covid19_status_sido_today_parmas = []
                covid19_status_sido_today_parmas.append(i.find("th").text)
                covid19_status_sido_today_parmas.append(standard_date)
                covid19_status_sido_today_parmas.extend([re.sub(",", "", data.text) for data in i.select("td")])

                insert_covid19_status_sido_today_query = """
                                                        INSERT INTO covid19_status_sido(
                                                            sido,
                                                            standard_date,
                                                            confirmed_daily,
                                                            confirmed_daily_domestic,
                                                            confirmed_daily_overseas,
                                                            confirmed_total,
                                                            isolated_total,
                                                            released_total,
                                                            deceased_total,
                                                            incidence_rate)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                                        """
                cursor.execute(insert_covid19_status_sido_today_query, covid19_status_sido_today_parmas)
                connection.commit()
finally:
    connection.close()
