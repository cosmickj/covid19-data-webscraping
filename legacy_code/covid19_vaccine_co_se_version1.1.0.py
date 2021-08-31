# ! /usr/bin/python3
from bs4 import BeautifulSoup
import requests

from fake_useragent import UserAgent
from datetime import datetime
import pandas as pd
import re

# <-------------------Setting Database------------------->
from configparser import ConfigParser
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
# import MySQLdb

config = ConfigParser()
config.read('config.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']
CHARSET1 = config['appmd_db']['CHARSET1']
CHARSET2 = config['appmd_db']['CHARSET2']

con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
engine = create_engine(con_str, encoding =CHARSET2)
conn = engine.connect()

# <-------------------Setting Modules------------------->
def checkDbUpdate(dbtable):
    sql = f'SELECT MAX(upd_date) FROM {dbtable};'
    sql_df = pd.read_sql(sql,con=engine)
    latest_upd_date = sql_df.iloc[0,0].date()
    return latest_upd_date

def updateTable(table, dbtable):
    table.to_sql(name=f'{dbtable}', con=conn, if_exists='append',index=False)
    print(f"{dbtable} Updated Complete.")

def requestsSoup(url):
    html = requests.get(url, headers=headers).text
    soup = BeautifulSoup(html,'html.parser')
    return soup

def findRawData(soup,text):
    return soup.find(string=re.compile(text))

def findVaccineData(data):
    raw_data = data.find_parent()
    
    if str(raw_data).startswith('<table'):
        return raw_data
    elif str(raw_data).startswith('<div'):
        return raw_data
    else:
        return findVaccineData(raw_data)

# <-------------------Start Scraping------------------->
ua = UserAgent()
userAgent = ua.random
headers = {'User-Agent':userAgent}

if checkDbUpdate('vaccine_company_status') == datetime.today().date():
    print('Already Updated')
else:
    # 1)
    standard_date = datetime.today().strftime("%-m.%-d.")

    press_release_url = "http://ncov.mohw.go.kr/tcmBoardList.do?board_id=140&search_item=1&search_content=0시"
    press_release_soup = requestsSoup(press_release_url)

    today_article = [article for article in press_release_soup.select("a.bl_link") if standard_date in article.text]

    if today_article:
        try:
            today_article = today_article[0]

            keys  = ['js_fn_name','template','brdId','brdGubun','ncvContSeq','board_id','gubun']
            items = re.findall("\/?\w+\.?\w*",today_article['onclick'])
            queries = dict(zip(keys,items))

            today_article_url = f"http://ncov.mohw.go.kr/{queries['template']}?brdId={queries['brdId']}&brdGubun={queries['brdGubun']}&ncvContSeq={queries['ncvContSeq']}&contSeq={queries['ncvContSeq']}&board_id={queries['board_id']}&gubun={queries['gubun']}"
            today_article_soup = requestsSoup(today_article_url)

            # 2)
            raw_target_table = findVaccineData(findRawData(today_article_soup,"예방접종 실적"))
            target_table = pd.read_html(str(raw_target_table))[0]

            # 3) vaccine company status
            vaccine_company_status = target_table.loc[target_table[1]=='누계'].copy()
            vaccine_company_status = vaccine_company_status[[0,2]]
            vaccine_company_status.columns = ['company','vaccinated_total']
            vaccine_company_status['upd_date'] = datetime.today().date()
            vaccine_company_status.reset_index(drop=True,inplace=True)

            # 4) vaccine side effect status
            vaccine_sideeffect = target_table.drop(columns=[2,5]).copy()                  # 예방접종 실적 & 소계 column 삭제
            vaccine_sideeffect.columns = vaccine_sideeffect.iloc[1]                       # 테이블 column 변경
            vaccine_sideeffect = vaccine_sideeffect.drop([0,1,len(vaccine_sideeffect)-1]) # 필요없는 rows 삭제
            vaccine_sideeffect.reset_index(drop=True,inplace=True)

            company_list = list(set(vaccine_sideeffect.iloc[:,0]))

            vaccine_sideeffect_status = pd.DataFrame(columns=['company','side_effect_type','side_effect_new','side_effect_total','upd_date'])
            for company in company_list:
                temp_table = vaccine_sideeffect.loc[vaccine_sideeffect.iloc[:,0]==company].copy()
                
                temp_table = temp_table.T
                temp_table.columns = range(100,100+temp_table.shape[1])
                temp_table = temp_table.iloc[2:].reset_index()
                
                temp_table.columns = ['side_effect_type','side_effect_new','side_effect_total']
                temp_table['side_effect_type'] = temp_table['side_effect_type'].apply(lambda word: re.sub('[^가-힣\s]','',word))
                temp_table['company']  = company
                temp_table['upd_date'] = datetime.today().date()
                
                vaccine_sideeffect_status = pd.concat([vaccine_sideeffect_status,temp_table])

            vaccine_sideeffect_status.reset_index(drop=True,inplace=True)

            # 5) update tables
            updateTable(vaccine_company_status, "vaccine_company_status")
            updateTable(vaccine_sideeffect_status, "vaccine_sideeffect_status")
        except Exception as e:
            print(e)
            print('Something is wrong...')
    else:
        print('It is not updated on web site.')