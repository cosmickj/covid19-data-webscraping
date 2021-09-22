# ! /usr/bin/python3
from bs4 import BeautifulSoup
import requests

from configparser import ConfigParser
from datetime import datetime
import pandas as pd
import re

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

target_url = 'https://ncv.kdca.go.kr/mainStatus.es?mid=a11702000000'
todayDate = datetime.today().date()

# <-------------------Setting Database------------------->
config = ConfigParser()
config.read('config.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']
CHARSET1 = config['appmd_db']['CHARSET1']
CHARSET2 = config['appmd_db']['CHARSET2']

# <-------------------Setting Functions------------------->
def checkDatabaseUpdate():
    dbcon = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE, charset=CHARSET1)
    cursor = dbcon.cursor()
    
    sql = "SELECT MAX(upd_date) FROM `vaccine_kr_status`;"
    cursor.execute(sql)
    
    result = cursor.fetchone()
    latest_upd_date = result[0].date()
    return latest_upd_date

def uploadTable(table):
    con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
    engine = create_engine(con_str, encoding =CHARSET2)
    
    conn = engine.connect()
    table.to_sql(name='vaccine_kr_status', con=conn, if_exists='append',index=False)
    print("Updated Complete.")

def getBs(url):
    html = requests.get(url)
    html.raise_for_status()
    html.encoding=None
    html = html.text
    soup = BeautifulSoup(html,'html.parser')
    return soup

def getUpdDate(bs):
    upd_date = bs.find('span',class_='t_date').text
    upd_date = re.sub('[\s(,)가-힣]','',upd_date)
    upd_date = str(datetime.today().year)+'.' + upd_date+':00'
    upd_date = pd.to_datetime(upd_date)
    return upd_date

def getVaccineTable(bs, date):
    bs_table = bs.select('table')[-1]

    table = pd.read_html(str(bs_table))[0]
    table = table[1:].copy()
    table.columns = ['sido','new_injected_1st','total_injected_1st','new_injected_2nd','total_injected_2nd']
    table.reset_index(drop=True, inplace=True)
    table['upd_date'] = date
    return table

# <-------------------Start Crawler------------------->
bs = getBs(target_url)
upd_date = getUpdDate(bs)

try:
    if checkDatabaseUpdate() != todayDate:
        if todayDate != upd_date.date():
            print("It isn't updated yet.")
        else:
            vaccine_table = getVaccineTable(bs, upd_date)
            uploadTable(vaccine_table)
    else:
        print("It is already updated.")
except Exception as e:
    print(e)