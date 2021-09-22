# ! /usr/bin/python3
from bs4 import BeautifulSoup
import requests

from fake_useragent import UserAgent
from datetime import datetime
import pandas as pd
import re

# <-------------------Setting headers------------------->
ua = UserAgent()
userAgent = ua.random
headers = {'User-Agent':userAgent}

# <-------------------Setting Database------------------->
from configparser import ConfigParser
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

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
def getBs(url):
    html = requests.get(url, headers=headers).text
    bs = BeautifulSoup(html,'html.parser')
    return bs

def checkDbUpdate(dbtable):
    sql = f'SELECT MAX(upd_date) FROM {dbtable};'
    sql_df = pd.read_sql(sql,con=engine)
    latest_upd_date = sql_df.iloc[0,0].date()
    return latest_upd_date

def uploadTable(table, dbtable):
    table.to_sql(name=f'{dbtable}', con=conn, if_exists='append',index=False)
    print(f"{dbtable} Updated Complete.")

# <-------------------Start Crawlering------------------->
try:
    if checkDbUpdate('vaccine_co_status') != datetime.today().date():
        today_arti_date = datetime.today().strftime("%-m.%-d.")

        bodo_url = 'http://ncov.mohw.go.kr/tcmBoardList.do?board_id=140&search_item=1&search_content=0시'
        bodo_bs  = getBs(bodo_url)
        articles = [x for x in bodo_bs.select('td.ta_l a') if today_arti_date in x.text]

        if len(articles) == 1:
            keys = ['brdId','brdGubun','ncvContSeq','board_id','gubun']
            values = re.findall("\'(\w+)\'",articles[0]['onclick'])
            queries = dict(zip(keys,values))

            arti_url = f"http://ncov.mohw.go.kr/tcmBoardView.do?brdId={queries['brdId']}&brdGubun={queries['brdGubun']}&ncvContSeq={queries['ncvContSeq']}&contSeq={queries['ncvContSeq']}&board_id={queries['board_id']}&gubun={queries['gubun']}"
            today_arti_bs = getBs(arti_url)

            # upd_date
            details = today_arti_bs.select('div.bv_category ul li span')
            upd_date = [details[idx+1].text for idx, detail in enumerate(details) if '작성일' in detail.text]
            upd_date = pd.to_datetime(upd_date[0].split()[0])

            # temp_table
            tables = today_arti_bs.select('table')
            target_table = [table for table in tables if '접종자수' in str(table)]

            temp_table = pd.read_html(str(target_table[0]))[0]
            temp_table.columns = temp_table.iloc[0]
            temp_table = temp_table.iloc[1:-1].copy()

            temp_table.replace('\(\d+\)','',regex=True,inplace=True)
            temp_table.replace('-','0',inplace=True)
            temp_table.rename(columns=lambda x: re.sub('\([가-힣]+\)|\d+\)|\s','',x),inplace=True)

            temp_columns       = list(temp_table.columns)
            temp_columns[0]    = '제조사'
            temp_table.columns = temp_columns

            # vaccine_company
            vaccine_company = temp_table.loc[temp_table['구분']=='누계'].copy()
            vaccine_company = vaccine_company[['제조사','접종자수']]
            vaccine_company.columns = ['company','vaccinated_total']
            vaccine_company['upd_date'] = upd_date

            # <--------------- Upload point --------------->
            uploadTable(vaccine_company, 'vaccine_co_status')
            # <--------------- Upload point --------------->

            # vaccine_sideeffect
            vaccine_sideeffect = pd.DataFrame(columns=['company','se_type','se_new_num','se_tot_num','upd_date'])
            companies = list(set(temp_table['제조사'].values))

            for i in range(len(companies)):
                table_by_company = temp_table.loc[temp_table['제조사']==companies[i]]

                # 필요한 컬럼 요소 추출
                strt_idx    = list(table_by_company.columns).index('일반')
                new_columns = list(table_by_company.columns)[strt_idx:]
                new_columns.insert(0,'합계')
                
                # 테이블 전처리 수행
                table_by_company = table_by_company[new_columns]
                table_by_company = table_by_company.T
                table_by_company['company'] = companies[i]
                table_by_company.reset_index(inplace=True)
                table_by_company.columns = ['se_type','se_new_num','se_tot_num','company']
               
                vaccine_sideeffect = pd.concat([vaccine_sideeffect,table_by_company])
            vaccine_sideeffect['upd_date'] = upd_date

            # <--------------- Upload point --------------->
            uploadTable(vaccine_sideeffect, 'vaccine_se_status')
            # <--------------- Upload point --------------->
        else:
            print('It is not updated yet.')
    else:
        print('We already have.')
except Exception as e:
    print(e)