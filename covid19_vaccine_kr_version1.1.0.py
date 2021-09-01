# ! /usr/bin/python3
print('Start!')

import requests

from configparser import ConfigParser
from datetime import datetime, timedelta
import pandas as pd
import xmltodict
import json

from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

# <-------------------Setting Database------------------->
config = ConfigParser()
config.read('./config/secret.ini')

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

kosis_apiKey = config['kosis_api']['apiKey']

def check_db_update():
    sql = "SELECT MAX(upd_date) FROM `vaccine_region_status`;"
    sql_df = pd.read_sql(sql,con=engine)
    latest_upd_date = sql_df.iloc[0,0].date()
    return latest_upd_date

def upload_table(table):
    table.to_sql(name='vaccine_region_status', con=conn, if_exists='append',index=False)
    print("Updated Complete.")

def get_data_from_xml(url):
    response = requests.get(url).content
    data = xmltodict.parse(response)
    return data

def get_update_date(data):
    raw_upd_date = pd.to_datetime(data['response']['body']['dataTime'].split()[0])
    upd_date = raw_upd_date + timedelta(days=1) # 시간을 전날 24시 형태로 제공하기에 하루를 더해준다
    return upd_date

def create_base_table(data, column):
    dict_data   = json.loads(json.dumps(data, ensure_ascii=False))['response']['body']['items']['item']
    base_table  = pd.DataFrame(dict_data)
    base_table  = base_table.iloc[:17,:]
    base_table.iloc[:,1:] = base_table.iloc[:,1:].apply(pd.to_numeric)
    
    column_list = base_table[column].tolist()
    return base_table, column_list

def create_final_table(base_table,target_column,population):
    ordered_population = []
    for idx in range(len(base_table[target_column])):
        ordered_population.append(population[base_table[target_column][idx]])

    base_table['population'] = ordered_population
    base_table['%_firstTot']  = (base_table['firstTot']  / base_table['population']) * 100
    base_table['%_secondTot'] = (base_table['secondTot'] / base_table['population']) * 100
    base_table['upd_date'] = upd_date
    base_table.columns = ['sido','new_injected_1st','total_injected_1st','new_injected_2nd','total_injected_2nd',
                          'population','percent_total_injected_1st','percent_total_injected_2nd','upd_date']
    final_table = base_table.copy()
    return final_table

def get_kr_population(url,data_list):
    results = requests.get(url).json()
    population = {}
    for i in range(len(results)):
        if len(results[i]['C1']) == 2:
            if results[i]['C1_NM'] in data_list:
                population[results[i]['C1_NM']] = int(results[i]['DT'])
    return population

# 공공데이터포털-질병관리청_코로나19 예방접종 현황: https://www.data.go.kr/data/15078166/openapi.do
sido_vaccine_api = 'https://nip.kdca.go.kr/irgd/cov19stats.do?list=sido'

# KOSIS공유서비스: https://kosis.kr/openapi/index/index.jsp
kr_population_api = f'https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={kosis_apiKey}=&itmId=T20+&objL1=ALL&objL2=&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=1&loadGubun=2&orgId=101&tblId=DT_1B040A3'

"""MAIN PROCESS"""
todayDate = datetime.today().date()

try:
    if check_db_update() != todayDate:
        sido_data = get_data_from_xml(sido_vaccine_api)
        upd_date = get_update_date(sido_data)

        if upd_date == todayDate:
            base_table, sido_list = create_base_table(sido_data, 'sidoNm')

            kr_population = get_kr_population(kr_population_api,sido_list)

            final_table = create_final_table(base_table,'sidoNm',kr_population)

            upload_table(final_table)
    else:
        print(f'{todayDate} 백신 접종 현황 데이터는 이미 업데이트 되어 있습니다.')
except Exception as e:
    print(e)
