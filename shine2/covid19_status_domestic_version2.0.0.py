import requests
from bs4 import BeautifulSoup

import re
import pandas as pd
from datetime import datetime

import pymysql
from configparser import ConfigParser

config = ConfigParser()
config.read('/ShineMacro/shine_covid19_status/config/secret.ini')
HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']

connection = pymysql.connect(host     = HOSTNAME,
                             port     = PORT,
                             user     = USERNAME,
                             password = PASSWORD,
                             database = DATABASE)
cursor =connection.cursor()

select_latest_upd_date_query = "SELECT MAX(upd_date) FROM covid19_status_domestic;"
cursor.execute(select_latest_upd_date_query)
record = cursor.fetchone()

if record[0].date() == datetime.today().date():
    print('이미 업데이트 되었습니다.')
else:
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    url = 'http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=11&ncvContSeq=&contSeq=&board_id=&gubun='

    html = requests.get(url, params=headers).text
    soup = BeautifulSoup(html, 'html.parser')

    date_element = soup.find("span", class_="t_date").text
    date_element = re.sub('[(가-힣)\s]','',date_element)
    date_element = str(datetime.now().year) + '.' + date_element
    upd_date = pd.to_datetime(date_element)

    covid19_status_domestic_today_params = []
    case_table = soup.find("div", class_="caseTable").select("dd")
    for case in case_table:
        if case.select("p.inner_value"):
            case_detail = case.select("p.inner_value")
            covid19_status_domestic_today_params.extend([re.sub('\D','',i.text) for i in case_detail])
        else:
            covid19_status_domestic_today_params.append(re.sub('[가-힣,\s]','',case.text))
    covid19_status_domestic_today_params.append(upd_date)

    insert_covid19_status_domestic_today = """
                                            INSERT INTO covid19_status_domestic (
                                                confirmed_total,
                                                confirmed_daily,
                                                confirmed_daily_domestic,
                                                confirmed_daily_overseas,
                                                released_total,
                                                released_daily,
                                                isolated_total,
                                                isolated_daily,
                                                deceased_total,
                                                deceased_daily,
                                                upd_date)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                            """
    cursor.execute(insert_covid19_status_domestic_today, covid19_status_domestic_today_params)
    connection.commit()
    print('업데이트 되었습니다.')

connection.close()