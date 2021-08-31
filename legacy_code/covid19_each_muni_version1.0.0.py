# ! /usr/bin/python3
print('Script Start')

# <-------------------------module settings------------------------->
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

from datetime import datetime, timedelta
from fake_useragent import UserAgent
from configparser import ConfigParser
import pandas as pd
import time
import re

from bs4 import BeautifulSoup
import requests

from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

todayDate = datetime.date(datetime.today())

ua = UserAgent()
userAgent = ua.random
headers = {'User-Agent':userAgent}

display = Display(visible=0, size=(1024, 768))
display.start()

DRIVER_LOCATION = "/usr/bin/chromedriver"
BINARY_LOCATION = "/usr/bin/google-chrome"

options = Options()
options.add_argument(f'user-agent={userAgent}')
options.binary_location = BINARY_LOCATION
driver = webdriver.Chrome(executable_path=DRIVER_LOCATION, options=options)

# <-------------------------functions for data upload------------------------->
config = ConfigParser()
config.read('config.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']
CHARSET1 = config['appmd_db']['CHARSET1']
CHARSET2 = config['appmd_db']['CHARSET2']

def checkSidoUpdated(num):
    dbcon = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE, charset=CHARSET1)
    cursor = dbcon.cursor()
    sql = f"SELECT MAX(UPD_DATE) FROM `covid19_kr_by_Municipality` WHERE SIDO = '{sidoList[num]}'"
    cursor.execute(sql)
    result = cursor.fetchone()
    upd_date = result[0].date()
    return upd_date

def upload(table, number):
    con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
    engine = create_engine(con_str, encoding =CHARSET2)

    latest_upd_date_sql = pd.read_sql(f'SELECT MAX(UPD_DATE) FROM covid19_kr_by_Municipality WHERE SIDO = "{sidoList[number]}"',con=engine).iloc[0,0].date()
    latest_upd_date_web = table.iloc[0,-1].date()
    if latest_upd_date_sql != latest_upd_date_web:
        conn = engine.connect()
        table.to_sql(name='covid19_kr_by_Municipality', con=conn, if_exists='append',index=False)
        print(f'{sidoList[number]} update complete')
    else:
        print(f'{sidoList[number]} not yet')

# <-------------------------functions for data preprocessing------------------------->
def bsRequests(url):
    html = requests.get(url, headers=headers)
    bs = BeautifulSoup(html.text,'html.parser')
    return bs

def bsSelenium(url):
    driver.get(url)
    time.sleep(0.5)
    html = driver.page_source
    bs = BeautifulSoup(html,'html.parser')
    return bs

# 시군구 이름과 시군구별 확진자 수 추출
def extractNameNum(place_names=[],place_nums=[]):
    sigun_name = []
    for name in place_names:
        if 'alt' in name.attrs:
            name = name['alt']
            sigun_name.append(name)
        else:
            sigun_name.append(name.text)
    sigun_number = []
    for num in place_nums:
        num = re.sub('\(.*\)|,','',num.text)
        sigun_number.append(int(num))    
    return sigun_name, sigun_number

# 각 지자체별 테이블 생성
def drawTable(sigun_name,sigun_number,sido_name,upd_date):
    result_table = pd.DataFrame(columns=['sido','sigun','tot_num','upd_date'])
    result_table['sigun']=sigun_name
    result_table['tot_num']=sigun_number
    result_table['sido']= sido_name
    result_table['upd_date']=upd_date
    return result_table

# 각 지자체별 업데이트 시점 추출
def cleanDateText(datetext):
    date = re.sub('\([가-힣]\)','',datetext)
    date = re.sub('년|월|일','.',date)
    date = re.sub('[^\d\.\s]','',date).strip()

    if date.count('.') == 2 and (str(datetime.now().year) not in date):
        date = str(datetime.now().year)+'.'+date

    try:
        date = pd.to_datetime(date,yearfirst=True)
    except:
        date = date+'00'
        date = date.replace(' ','')
        try:
            date = pd.to_datetime(date,yearfirst=True)
        except:
            date = date[:-2]+':00'
            date = pd.to_datetime(date,yearfirst=True)

    if date.date() < datetime.now().date():
        date = date + timedelta(hours=12)
        
    return date

# <-------------------------functions for data scraping------------------------->
def scrapeSeoul(url): # 서울
    bs = bsRequests(url)
    date_text = bs.find('p',class_='txt-status').text
    
    table_raw = bs.select_one('table.tstyle-status')
    place_names = table_raw.find_all('th')
    place_nums = table_raw.find_all('td', class_=lambda x: x != 'today')

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'서울',cleanDateText(date_text))
    return table

def scrapeBusan(url): # 부산
    bs = bsRequests(url)
    date_text = bs.find('span',class_='item1').text
    date_text = re.sub('[\\r\\n\\t]','',date_text)
    
    table_raw = bs.select_one('div.covid-state-table table')
    place_names = table_raw.find_all('th')[1:]
    place_nums  = table_raw.find_all('td')[19:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]
    
    table = drawTable(sigun_name,sigun_number,'부산',cleanDateText(date_text))
    return table

def scrapeDaegu(url): # 대구
    bs = bsSelenium(url)
    date_text = bs.find('div',class_='top_date').text
    
    sigun_name = ['대구']
    sigun_number = bs.find('em',class_='info_num').text
    sigun_number = int(re.sub('\W|[가-힣]','',sigun_number))

    table = drawTable(sigun_name,sigun_number,'대구',cleanDateText(date_text))
    return table

def scrapeIncheon(url): # 인천
    bs = bsRequests(url)
    date_text = bs.find('span',class_='corona-data').text
    
    table_raw = bs.find('div',class_='corona-tab')
    place_names = table_raw.find_all('dt')[1:]
    place_nums  = table_raw.find_all('dd')[::2][1:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]
    
    table = drawTable(sigun_name,sigun_number,'인천',cleanDateText(date_text))
    return table

def scrapeGwangju(url): # 광주
    bs = bsSelenium(url)

    date_text = bs.find('p',class_='date_title').text
    table_raw = bs.find('table',class_='mt10')
    place_names = table_raw.find_all('th')[2:]
    place_nums  = table_raw.find_all('td')[1:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'광주',cleanDateText(date_text))
    return table

def scrapeDaejeon(url): # 대전
    bs = bsRequests(url)
    date_text = bs.select('h3 span')[1].text

    table_raw = bs.find('div',class_='wrap_map_status')
    place_names = table_raw.find_all('span')
    place_nums  = table_raw.find_all('strong')

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'대전',cleanDateText(date_text))
    return table

def scrapeUlsan(url): # 울산
    bs = bsRequests(url)
    
    date_text = bs.find('p',class_='exp').text
    place_nums  = bs.select('div.situation1_1 ul li span.num_people')

    sigun_name   = ['울산']
    sigun_number = sum(extractNameNum(place_nums=place_nums)[1])
    
    table = drawTable(sigun_name,sigun_number,'울산',cleanDateText(date_text))
    return table

def scrapeSejong(url): # 세종
    bs=bsSelenium(url)
    date_text = bs.find('span',id='baseDate').text
    
    sigun_name = ['세종시']
    sigun_number = int(bs.find('em',id='info5').text)

    table = drawTable(sigun_name,sigun_number,'세종',cleanDateText(date_text))
    return table

def scrapeGyeonggi(url): # 경기
    bs = bsRequests(url)
    date_text = bs.find('small',class_='date').text
    
    table_raw = bs.find('div',class_='zone')
    place_names = table_raw.find_all('dt')[1:]
    place_nums  = table_raw.find_all('strong')[1:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'경기',cleanDateText(date_text))
    return table

def scrapeGangwon(url): # 강원
    bs = bsSelenium(url)

    date_text = bs.select_one('div.condition h3 span').text
    table_raw = bs.find('table',class_='skinTb')
    place_names = table_raw.select('th.c_blue')
    place_nums  = table_raw.select('td.txt-c')[1:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'강원',cleanDateText(date_text))
    return table

def scrapeChungbuk(url): # 충북
    bs = bsRequests(url)
    date_text = bs.select_one('div.timebox span').text
    
    table_raw = bs.find('div',class_='inline_box2')
    place_names = table_raw.select('div img')
    place_nums  = table_raw.select('div a')

    sigun_name   = extractNameNum(place_names,place_nums)[0][1:-1]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'충북',cleanDateText(date_text))
    return table

def scrapeChungnam(url): # 충남
    bs = bsRequests(url)
    date_text = bs.select_one('div.status p').text
    
    table_raw = bs.find('table',class_='new_tbl_board mb20')
    place_names = table_raw.select('th')[2:]
    place_nums  = table_raw.select('tbody tr td')[2:18]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'충남',cleanDateText(date_text))
    return table

def scrapeJeonbuk(url): # 전북
    bs = bsSelenium(url)

    date_text = bs.select_one('div.nationwide h2 span').text
    table_raw = bs.select('div.city ul li')
    
    sigun_name = []
    sigun_number = []

    for info in table_raw:
        temp = info.text.split()
        sigun_name.append(temp[0])
        sigun_number.append(int(temp[1].replace('명','')))

    table = drawTable(sigun_name,sigun_number,'전북',cleanDateText(date_text))
    return table

def scrapeJeonnam(url): # 전남
    bs = bsSelenium(url)
    date_text = bs.select_one('div.title h2 em').text
    
    table_raw = bs.select_one('table.tb_color2 tbody')
    place_names = table_raw.find_all('th')
    place_nums  = table_raw.find_all('td')

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'전남',cleanDateText(date_text))
    return table

def scrapeGyeongbuk(url): # 경북
    bs = bsSelenium(url)
    date_text = bs.select_one('div.status_tit dl dd').text
    
    table_raw = bs.find('div',class_='city_corona')
    place_names = table_raw.find_all('dt')[2:]
    place_nums  = table_raw.find_all('strong')[2:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'경북',cleanDateText(date_text))
    return table

def scrapeGyeongnam(url): # 경남
    bs = bsRequests(url)
    date_text = bs.find('p',class_='exp').text
    
    table_raw = bs.find('div',class_='table type1 pt10')
    place_names = table_raw.select('th')[2:]
    place_nums  = table_raw.select('td.point')[2:]

    sigun_name   = extractNameNum(place_names,place_nums)[0]
    sigun_number = extractNameNum(place_names,place_nums)[1]

    table = drawTable(sigun_name,sigun_number,'경남',cleanDateText(date_text))
    return table

def scrapeJeju(url): # 제주
    bs = bsSelenium(url)
    date_text = bs.find('span',id='date').text

    sigun_name = ['제주']
    sigun_number = bs.find('b',id='infectJ').text

    table = drawTable(sigun_name,sigun_number,'제주',cleanDateText(date_text))
    return table

# <-------------------------Do scraping------------------------->
#              0     1      2     3      4     5      6      7     8      9     10     11     12    13     14     15    16
sidoList = ['서울','부산','대구','인천','광주','대전','울산','세종','경기','강원','충북','충남','전북','전남','경북','경남','제주']
sidoUrl  = ['https://www.seoul.go.kr/coronaV/coronaStatus.do',
            'http://www.busan.go.kr/covid19/Corona19.do',
            'http://covid19.daegu.go.kr/',
            'https://www.incheon.go.kr/health/HE020409',
            'https://www.gwangju.go.kr/c19/',
            'https://www.daejeon.go.kr/corona19/index.do',
            'https://www.ulsan.go.kr/u/health/contents.ulsan?mId=001002003000000000',
            'https://www.sejong.go.kr/bbs/R3273/list.do?cmsNoStr=17465',
            'https://www.gg.go.kr/contents/contents.do?ciIdx=1150&menuId=2909',
            'http://www.provin.gangwon.kr/covid-19.html',
            'http://www1.chungbuk.go.kr/covid-19/index.do',
            'http://www.chungnam.go.kr/coronaStatus.do?tab=1',
            'https://www.jeonbuk.go.kr/board/list.jeonbuk?boardId=BBS_0000105&menuCd=DOM_000000110001000000&contentsSid=1219&cpath=',
            'https://www.jeonnam.go.kr/coronaMainPage.do',
            'http://gb.go.kr/corona_main.htm',
            'http://xn--19-q81ii1knc140d892b.kr/main/main.do',
            'https://covid19.jeju.go.kr/']
scrapers = [scrapeSeoul,scrapeBusan,scrapeDaegu,scrapeIncheon,scrapeGwangju,scrapeDaejeon,
            scrapeUlsan,scrapeSejong,scrapeGyeonggi,scrapeGangwon,scrapeChungbuk,scrapeChungnam,
            scrapeJeonbuk,scrapeJeonnam,scrapeGyeongbuk,scrapeGyeongnam,scrapeJeju]

for i in range(len(scrapers)):
    try:
        if checkSidoUpdated(i) == todayDate:
            print(f'{sidoList[i]} is already updated')
        else:
            table = scrapers[i](sidoUrl[i])
            upload(table, i)
    except Exception as e:
        print(e)

# <-------------------------E N D------------------------->
print('Script End')
driver.quit()
display.stop()