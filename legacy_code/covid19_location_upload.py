#! /usr/bin/python3
import pandas as pd
from datetime import datetime

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

todayDate = datetime.now().date().strftime("%Y%m%d")

change_cols = ["start_time","end_time",'upd_date']

file_path_alerts = '/home/kj/shineDatabase/covid19_location_files/'

try:
    today_table = pd.read_csv(file_path_alerts+f"md_covid19_location_alerts_{todayDate}.csv",index_col=0,parse_dates=change_cols)
except:
    today_table = pd.DataFrame()

def concatFinalTable(table_a, table_b):
    table_new = pd.concat([table_a,table_b])
    table_new.reset_index(drop=True,inplace=True)
    table_new.drop_duplicates(subset=['place_name','address','start_time','end_time'],keep=False, inplace=True)
    return table_new

def upload():
    con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
    engine = create_engine(con_str, encoding =CHARSET2)

    temp_table = pd.read_sql_table("covid19_location_ver2",con=engine)
    raw_table = pd.concat([temp_table,today_table])
    new_table = concatFinalTable(temp_table,raw_table)

    conn = engine.connect()
    new_table.to_sql(name='covid19_location_ver2', con=conn, if_exists='append',index=False)
    print("DONE")

upload()