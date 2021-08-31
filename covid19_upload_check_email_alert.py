# /usr/bin/python3
from configparser import ConfigParser
from datetime import datetime
import pandas as pd
import json

import pymysql

from email.mime.text import MIMEText
import smtplib

todayDate = datetime.date(datetime.now()).strftime('%Y-%m-%d')

sidoList = [
    'seoul','busan','daegu','incheon','gwangju','daejeon','ulsan','sejong','gyeonggi',
    'gangwon','chungbuk','chungnam','jeonbuk','jeonnam','gyeongbuk','gyeongnam','jeju'
    ]

with open('./config/covid19_sido_info.json','r') as f:
    sido_data = json.load(f)

####################################### Database Connect #######################################
config = ConfigParser()
config.read('./config/secret.ini')

HOSTNAME = config['appmd_db']['HOSTNAME']
PORT     = int(config['appmd_db']['PORT'])
USERNAME = config['appmd_db']['USERNAME']
PASSWORD = config['appmd_db']['PASSWORD']
DATABASE = config['appmd_db']['DATABASE']
CHARSET1 = config['appmd_db']['CHARSET1']
CHARSET2 = config['appmd_db']['CHARSET2']

dbcon = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE, charset=CHARSET1)

def notUpdated():
    sql = f"SELECT DISTINCT SIDO FROM covid19_kr_by_Municipality WHERE UPD_DATE LIKE '%{todayDate}%';"

    result = pd.read_sql_query(sql,dbcon)

    updated_list = result['SIDO'].tolist()
    not_updated = []
    
    for sido in sidoList: # 업데이트 되지 않은 지자체의 이름과 URL을 담아줌
        if sido_data[sido]['sido_kr'] not in updated_list:
            not_updated.append(f"{sido_data[sido]['sido_kr']}: {sido_data[sido]['url']}")

    not_updated = '\n'.join(not_updated)
    return not_updated

def sendEmail():
    mail_content = notUpdated()

    ## 이메일 서버열기
    s= smtplib.SMTP('smtp.gmail.com',587)
    s.starttls()
    s.login(config['gmail']['GMAIL_ID'],config['gmail']['GMAIL_PW'])

    ## 내용 작성하기
    # 완료
    if len(mail_content) == 0:
        msg=MIMEText(f"{datetime.now().strftime('%Y-%m-%d %H:%M')} 지역별 코로나 확진자 업로드가 완료되었습니다.") # 내용
        msg['Subject'] = "모두 정상적으로 업로드 되었습니다." # 제목
    # 미완료
    else:
        msg=MIMEText(f"{mail_content} \n확인 바랍니다.") #내용
        msg['Subject'] = f"{datetime.now().strftime('%Y-%m-%d %H:%M')} 아직 업로드 되지 않은 지자체가 있습니다. 확인 바랍니다." # 제목
    
    ## 메일 보내기
    s.sendmail(config['gmail']['GMAIL_ID'],config['gmail']['GMAIL_ID'],msg.as_string())
    s.quit()

sendEmail()
dbcon.close()