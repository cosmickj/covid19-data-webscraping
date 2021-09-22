# ! /usr/bin/python3 
print('Start')

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
# import MySQLdb

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from configparser import ConfigParser
import pandas as pd
import time
import re
import json

from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

today_date = datetime.date(datetime.today())

ua = UserAgent()
userAgent = ua.random

DRIVER_LOCATION = "/usr/bin/chromedriver"
BINARY_LOCATION = "/usr/bin/google-chrome"

options = Options()
options.add_argument(f'user-agent={userAgent}')
options.add_argument('headless')
options.binary_location = BINARY_LOCATION

driver = webdriver.Chrome(executable_path=DRIVER_LOCATION, options=options)

# functions for data upload
config = ConfigParser()
config.read('config.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']
CHARSET1 = config['appmd_db']['CHARSET1']
CHARSET2 = config['appmd_db']['CHARSET2']

# 검색
dbcon = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE, charset=CHARSET1)
cursor = dbcon.cursor()

# 삽입
con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
engine = create_engine(con_str, encoding =CHARSET2)
conn = engine.connect()

def checkSidoUpdated(sido_kr):
    sql = f"SELECT MAX(UPD_DATE) FROM `covid19_kr_by_Municipality` WHERE SIDO = '{sido_kr}'"
    cursor.execute(sql)
    result = cursor.fetchone()
    upd_date = result[0].date()
    return upd_date

def upload(table, sido):
    table.to_sql(name='covid19_kr_by_Municipality', con=conn, if_exists='append',index=False)
    print(f'{sido} update complete')

################################################################################################################################################################
def chromedriverSoup(url):
    driver.get(url)
    time.sleep(0.05)
    html = driver.page_source
    return BeautifulSoup(html,'html.parser')

def findDateText(soup):
    target_text = soup.find(string=re.compile('기준')).find_parent().text
    
    if re.search('\d',target_text):
        return target_text
    else:
        """페이지에서 맨 처음 나오는 '기준' 텍스트 찾기"""
        dummy_text = soup.find_all(string=re.compile('기준'))
        for text in dummy_text:
            if '시' in text or ':' in text:
                return text
                # break

def cleanUpdDate(raw_date):
    if "시" in raw_date:
        raw_date = raw_date.replace('시',':00')
        
    raw_date = raw_date[:raw_date.find(":00")+3]
    
    if '오전' in raw_date:
        raw_date = raw_date + 'am'
    elif '오후' in raw_date:
        raw_date = raw_date + 'pm'
        
    raw_date = re.sub('\([가-힣]\)','',raw_date)    # (월) 형식 제거
    raw_date = re.sub('년|월|일','.',raw_date)      # 년,월,일 -> "."
#     raw_date = re.sub('시\s?',':00',raw_date)      # 시 -> :00
    raw_date = re.sub("['(가-힣)\s]",' ',raw_date) # 한글, 빈칸, 괄호 제거
    
    if raw_date.count('.') == 2: # 년도가 들어가지 않은 항목에 년도 붙이기
        raw_date = str(datetime.today().year) + '.' + raw_date
    
    try:
        clean_date = pd.to_datetime(raw_date,yearfirst=True)
        return clean_date
    except:
        raw_date = raw_date.replace(' ','')
        clean_date = pd.to_datetime(raw_date,yearfirst=True)
        return clean_date

def findRawData(soup,text):
    return soup.find(string=re.compile(text))

def findCovid19Data(data):
    raw_data = data.find_parent()
    
    if str(raw_data).startswith('<table'):
        return raw_data
    elif str(raw_data).startswith('<div'):
        return raw_data
    else:
        return findCovid19Data(raw_data)

def cleanData(raw_regjon_data, tag_for_name, tag_for_covid19, name_start_index=0, covid_start_index=0):
    if tag_for_name is None:
        region_name  = [sido_data[sido]['sigun_kr']] # <- 전역 변수 사용
        region_covid = raw_regjon_data.select_one(tag_for_covid19).text
        region_covid = [int(re.sub('\D','',region_covid))]
    else:
        if 'img' in tag_for_name:
            region_name  = [name['alt'] for name in raw_regjon_data.select(tag_for_name)]
        else:
            region_name  = [name.text.split()[0] for name in raw_regjon_data.select(tag_for_name)]
        
        region_covid = [covid19.text for covid19 in raw_regjon_data.select(tag_for_covid19)]
    
        region_name  = region_name[name_start_index:] # 시군구 인덱싱
        region_covid = region_covid[covid_start_index:covid_start_index+len(region_name)] # 코로나 현황 인덱싱
        region_covid = [int(re.sub('\(.?\d+\)|\D','',num)) for num in region_covid] # 코로나 현황 int로 변형
    
    return region_name, region_covid

def buildTable(sido_name, clean_region_data, clean_covid19_data, clean_upd_date):
    table = pd.DataFrame()
    table['sigun']=clean_region_data
    table['tot_num']=clean_covid19_data
    table['upd_date']=clean_upd_date
    table['sido']= sido_name
    return table[['sido','sigun','tot_num','upd_date']]
################################################################################################################################################################
start_time = time.time()

with open('covid19_sido_info.json','r') as f:
    sido_data = json.load(f)

sido_list = [
    'seoul','busan','daegu','incheon','gwangju','daejeon','ulsan','sejong','gyeonggi',
    'gangwon','chungbuk','chungnam','jeonbuk','jeonnam','gyeongbuk','gyeongnam','jeju'
    ]

for sido in sido_list:
    try:
        if checkSidoUpdated(sido_data[sido]['sido_kr']) != today_date: # 오늘자 데이터가 DB 테이블에 없음
            # 1) 타겟 페이지 soup 가져오기
            sido_soup = chromedriverSoup(sido_data[sido]['url']) 
            
            # 2) 시도 업데이트 날짜 가져오기
            sido_upd_date = cleanUpdDate(findDateText(sido_soup))
            
            if sido_upd_date.date() == today_date: # 오늘자 날짜로 페이지 업데이트됨
                # 3) 코로나 현황 정보가 담긴 부분 가져오기
                raw_sido_data = findCovid19Data(findRawData(sido_soup, sido_data[sido]['target_text']))
                
                # 4) 시군구 이름 & 시군구별 코로나 현황 가져오기
                clean_region_data, clean_covid19_data = cleanData(raw_sido_data, sido_data[sido]['tag_for_name'], sido_data[sido]['tag_for_covid19'], sido_data[sido]['name_start_index'], sido_data[sido]['covid_start_index'])
                
                # 5) 최종 테이블 그리기
                final_table = buildTable(sido_data[sido]['sido_kr'], clean_region_data, clean_covid19_data , sido_upd_date)

                # 6) DB테이블에 업데이트
                upload(final_table, sido_data[sido]['sido_kr'])
            else:
                print(f"{sido_data[sido]['sido_kr']} is not updated yet")
        else:
            print(f"{sido_data[sido]['sido_kr']} is already updated")
    except Exception as e:
        print(e)
        
print('time:',time.time() - start_time)
driver.quit()
# display.stop()