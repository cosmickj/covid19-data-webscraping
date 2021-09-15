# 매일 오후 3시에 run (Since June 15, ... between 04:45 and 05:15 GMT to accommodate daily updates ...)
import pandas as pd
from datetime import datetime as dt, timedelta

from collections import Counter

import sqlalchemy as db  # sudo apt-get install -y python3-mysqldb
from configparser import ConfigParser

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
engine = db.create_engine(con_str, encoding=CHARSET2, pool_size=20, max_overflow=100)

select_latest_standard_date_query = (
    "SELECT DATE(MAX(standard_date)) AS latest_standard_date FROM covid19_status_overseas"
)
latest_standard_date = pd.read_sql(
    select_latest_standard_date_query,
    con=engine.connect(),
).iloc[0, 0]

if latest_standard_date == dt.today().date():
    print("이미 업데이트 되었습니다.")
else:
    """MAIN PROCESS"""
    # for i in reversed(range(1, 60)):
    raw_target_date = dt.today().date() - timedelta(days=1)
    target_date = raw_target_date.strftime("%m-%d-%Y")

    try:
        # 해외 코로나 현황 데이터
        status_url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{target_date}.csv"
        status_df = pd.read_csv(status_url)
        status_df = status_df[["Country_Region", "Last_Update", "Confirmed", "Deaths", "Combined_Key"]]

        # 해외 인구 데이터
        population_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
        population_df = pd.read_csv(population_url)
        population_df = population_df[["iso2", "Combined_Key", "Population"]]

        full_df = pd.merge(status_df, population_df, how="left", on="Combined_Key")
        full_df.loc[full_df["Country_Region"] == "Namibia", "iso2"] = "NA"

        full_df_groupby_iso2 = (
            full_df.groupby("iso2")
            .agg({"Last_Update": "max", "Confirmed": "sum", "Deaths": "sum", "Population": "sum"})
            .reset_index()
        )
        full_df_groupby_iso2.columns = [
            "alpha2_code",
            "standard_date",
            "confirmed_total",
            "deceased_total",
            "population",
        ]

        select_latest_data_query = f"""
                                    SELECT alpha2_code, confirmed_total, deceased_total
                                    FROM   covid19_status_overseas
                                    WHERE  DATE(standard_date) = '{raw_target_date.strftime('%Y-%m-%d')}';
                                    """
        latest_df = pd.read_sql_query(select_latest_data_query, con=engine.connect())

        # 신규 확진자
        latest_df_confirmed_total_dict = (
            latest_df[["alpha2_code", "confirmed_total"]].set_index("alpha2_code").T.to_dict("records")[0]
        )
        full_df_groupby_iso2_confirmed_total_dict = (
            full_df_groupby_iso2[["alpha2_code", "confirmed_total"]]
            .set_index("alpha2_code")
            .T.to_dict("records")[0]
        )
        confirmed_total_counter = Counter(full_df_groupby_iso2_confirmed_total_dict)
        confirmed_total_counter.subtract(latest_df_confirmed_total_dict)
        full_df_groupby_iso2["confirmed_daily"] = full_df_groupby_iso2["alpha2_code"].map(
            confirmed_total_counter
        )

        # 100만 명당 신규 확진자수
        full_df_groupby_iso2["confirmed_daily_per_million"] = (
            full_df_groupby_iso2["confirmed_daily"] / full_df_groupby_iso2["population"] * 1_000_000
        )

        # 신규 사망자
        latest_df_deceased_total_dict = (
            latest_df[["alpha2_code", "deceased_total"]].set_index("alpha2_code").T.to_dict("records")[0]
        )
        full_df_groupby_iso2_deceased_total_dict = (
            full_df_groupby_iso2[["alpha2_code", "deceased_total"]]
            .set_index("alpha2_code")
            .T.to_dict("records")[0]
        )
        deceased_total_counter = Counter(full_df_groupby_iso2_deceased_total_dict)
        deceased_total_counter.subtract(latest_df_deceased_total_dict)
        full_df_groupby_iso2["deceased_daily"] = full_df_groupby_iso2["alpha2_code"].map(deceased_total_counter)

        # 100만 명당 신규 사망자수
        full_df_groupby_iso2["deceased_daily_per_million"] = (
            full_df_groupby_iso2["deceased_daily"] / full_df_groupby_iso2["population"] * 1_000_000
        )

        full_df_groupby_iso2.to_sql(
            name="covid19_status_overseas", con=engine.connect(), if_exists="append", index=False
        )
    except:
        print("아직 업데이트 되지 않았습니다.")
