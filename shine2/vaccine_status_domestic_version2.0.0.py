# ! /usr/bin/python3
from configparser import ConfigParser
from datetime import datetime, timedelta
import pandas as pd
import xmltodict
import requests
import json

from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()

# Database setting
config = ConfigParser()
config.read("/ShineMacro/shine_covid19_status/config/secret.ini")

HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]
CHARSET1 = config["appmd_db"]["CHARSET1"]
CHARSET2 = config["appmd_db"]["CHARSET2"]

con_str = f"mysql+mysqldb://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}?charset={CHARSET1}"
engine = create_engine(con_str, encoding=CHARSET2)

sql = "SELECT MAX(standard_date) FROM `vaccine_status_domestic`;"
sql_df = pd.read_sql(sql, con=engine)
latest_upd_date = sql_df.iloc[0, 0].date()

if latest_upd_date == datetime.today().date():
    print("이미 업데이트 되었습니다.")

else:
    # 국내 코로나 백신 현황
    vaccine_status_domestic_api = "https://nip.kdca.go.kr/irgd/cov19stats.do?list=sido"
    response = requests.get(vaccine_status_domestic_api).content
    raw_data = xmltodict.parse(response)

    split_standard_date = raw_data["response"]["body"]["dataTime"].split()[0]
    standard_date = pd.to_datetime(split_standard_date) + timedelta(days=1)

    if standard_date < datetime.today().date():
        print("금일 데이터가 업데이트 되지 않았습니다.")

    else:
        vaccine_data = json.loads(json.dumps(raw_data, ensure_ascii=False))["response"]["body"]["items"]["item"]
        vaccine_status_domestic_df = pd.DataFrame(vaccine_data)

        domestic_total = {
            "sidoNm": "전국",
            "firstCnt": vaccine_status_domestic_df["firstCnt"].astype("int").sum(),
            "firstTot": vaccine_status_domestic_df["firstTot"].astype("int").sum(),
            "secondCnt": vaccine_status_domestic_df["secondCnt"].astype("int").sum(),
            "secondTot": vaccine_status_domestic_df["secondTot"].astype("int").sum(),
        }
        vaccine_status_domestic_df = vaccine_status_domestic_df.append(domestic_total, ignore_index=True)
        vaccine_status_domestic_df["standard_date"] = standard_date

        # 국내 인구 현황
        kosis_apiKey = config["kosis_api"]["apiKey"]
        population_api = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={kosis_apiKey}=&itmId=T20+&objL1=ALL&objL2=&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=1&loadGubun=2&orgId=101&tblId=DT_1B040A3"

        response = requests.get(population_api).json()
        population_dict = {}
        for idx in range(len(response)):
            if len(response[idx]["C1"]) == 2:
                population_dict[response[idx]["C1_NM"]] = int(response[idx]["DT"])

        population_df = pd.DataFrame(list(population_dict.items()), columns=["sidoNm", "population"])

        # 국내 코로나 백신 현황 + 인구 현황
        vaccine_status_domestic_df = vaccine_status_domestic_df.merge(population_df, how="inner", on="sidoNm")
        vaccine_status_domestic_df.columns = [
            "sido",
            "first_vaccinated_daily",
            "first_vaccinated_total",
            "second_vaccinated_daily",
            "second_vaccinated_total",
            "standard_date",
            "population",
        ]

        vaccine_status_domestic_df[
            ["first_vaccinated_total", "second_vaccinated_total"]
        ] = vaccine_status_domestic_df[["first_vaccinated_total", "second_vaccinated_total"]].astype(int)

        vaccine_status_domestic_df["first_vaccinated_total_percentage"] = (
            vaccine_status_domestic_df["first_vaccinated_total"] / vaccine_status_domestic_df["population"]
        ) * 100
        vaccine_status_domestic_df["second_vaccinated_total_percentage"] = (
            vaccine_status_domestic_df["second_vaccinated_total"] / vaccine_status_domestic_df["population"]
        ) * 100

        # 데이터베이스에 업데이트
        vaccine_status_domestic_df.to_sql(
            name="vaccine_status_domestic", con=engine.connect(), if_exists="append", index=False
        )
