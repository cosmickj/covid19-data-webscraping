from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from configparser import ConfigParser
# from datetime import datetime
# import pandas as pd
import time
import re
import pymysql

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
DRIVER_LOCATION = "/usr/bin/chromedriver"
BINARY_LOCATION = "/usr/bin/google-chrome"

options = Options()
options.add_argument(f'user-agent={user_agent}')
options.add_argument("--headless")
options.add_argument("no-sandbox")
options.add_argument("--disable-extensions")
options.binary_location = BINARY_LOCATION

browser = webdriver.Chrome(executable_path=DRIVER_LOCATION, options=options)

disaster_message_url = 'https://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/disasterMsgList.jsp?menuSeq=679'

def message_content_filter(text):
    target_message_pattern = "기준|현재|확진자?|\d+명|발생 알림"
    return True if len(re.findall(target_message_pattern, text)) >= 2 else False

config = ConfigParser()
config.read('./config/secret.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']

connection = pymysql.connect(host = HOSTNAME, port = PORT, user = USERNAME, password = PASSWORD, database = DATABASE)
cursor =connection.cursor()

select_max_id_query = "SELECT MAX(id) FROM `disaster_message_live`;"

insert_data_query = "INSERT INTO disaster_message_live VALUES (%s, %s, %s, %s, %s, %s, %s)"

# MAX ID값 가져오기
cursor.execute(select_max_id_query)
record = cursor.fetchone()

key = 'bbs_ordr'
value = record[0] + 1

sido_list = ['강원도', '경기도', '경상남도', '경상북도', '광주광역시',
             '대구광역시', '대전광역시', '부산광역시', '서울특별시', '울산광역시',
             '인천광역시', '전라남도', '전라북도', '제주특별자치도', '충청남도', '충청북도', '세종특별자치시']

browser.get(disaster_message_url)

latest_article = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.ID, "bbs_tr_0_bbs_title")))
latest_article.click()

while True:
    browser.execute_script("window.sessionStorage.setItem(arguments[0], arguments[1]);", key, value)
    browser.execute_script("window.location.reload();")
    
    if browser.find_element_by_id("bbs_gubun").text == "데이터가 없습니다":
        time.sleep(30)
        continue
        
    # Html 파싱하기
    html = browser.page_source.replace('<br>',' ')
    soup = BeautifulSoup(html,'html.parser')

    # 재난문자 발신 시간 가져오기
    find_received_time = soup.find('h3',id='sj').text
    received_time = re.sub('[가-힣]','',find_received_time).strip()

    full_article = soup.find('div',id='cn').text
        
    # 재난문자 발신자 가져오기
    message_sender = full_article.split(']',1)[0].strip()
    message_sender = re.sub('\[|\]','',message_sender)
    
    # 재난문자 내용 가져오기
    partial_article = full_article.split(']',1)[1]
    message_content = partial_article.split('-송출지역-', 1)[0].strip()
    # 재난문자 내용 필터 처리
    if message_content_filter(message_content): for_shine = "Y"
    else: for_shine = "N"

    # 재난문자 수신 지역 가져오기
    received_region_dummy = partial_article.split('-송출지역-', 1)[1].strip()
    # 메세지 수신 지역 필터 처리
    received_region_dummy_set = set(re.findall('|'.join(sido_list),received_region_dummy))
    if len(received_region_dummy_set) == 1:
        received_region = received_region_dummy_set.pop()
    else:
        received_region = "다수 지역"

    disaster_message_data = (value, message_sender, message_content, received_time, received_region, received_region_dummy, for_shine)
    print(disaster_message_data)

    cursor.execute(insert_data_query, disaster_message_data)
    connection.commit()
        
    value += 1
    break

connection.close()
browser.quit()