import pandas as pd
import pycountry_convert as pc

from configparser import ConfigParser
import pymysql

config = ConfigParser()
config.read("../config/secret.ini")

HOSTNAME = config["appmd_db"]["HOSTNAME"]
PORT = int(config["appmd_db"]["PORT"])
USERNAME = config["appmd_db"]["USERNAME"]
PASSWORD = config["appmd_db"]["PASSWORD"]
DATABASE = config["appmd_db"]["DATABASE"]
CHARSET1 = config["appmd_db"]["CHARSET1"]
CHARSET2 = config["appmd_db"]["CHARSET2"]

connection = pymysql.connect(host=HOSTNAME, port=PORT, user=USERNAME, password=PASSWORD, database=DATABASE)
cursor = connection.cursor()


def alpha3_to_alpha2(iso_code):
    try:
        alpha_2_code = pc.country_alpha3_to_country_alpha2(iso_code)
    except:
        alpha_2_code = iso_code
    return alpha_2_code


url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
df = pd.read_csv(url)
df = df[
    [
        "iso_code",
        "date",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "daily_vaccinations",
        "total_vaccinations_per_hundred",
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
    ]
]

df = df.loc[df["date"] == max(df["date"])]
# df = df.loc[df["date"] == "2021-08-31"]
df.fillna(0, inplace=True)
df["iso_code"] = df["iso_code"].apply(lambda x: alpha3_to_alpha2(x))

df.columns = [
    "alpha2_code",
    "standard_date",
    "total_vaccinated",
    "first_vaccinated_total",
    "second_vaccinated_total",
    "daily_vaccinated",
    "total_vaccinated_percentage",
    "first_vaccinated_total_percentage",
    "second_vaccinated_total_percentage",
]


insert_sql_query = """
                   INSERT INTO vaccine_status_overseas
                               (alpha2_code,
                               standard_date,
                               total_vaccinated,
                               first_vaccinated_total,
                               second_vaccinated_total,
                               daily_vaccinated,
                               total_vaccinated_percentage,
                               first_vaccinated_total_percentage,
                               second_vaccinated_total_percentage)
                   SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                   FROM   DUAL
                   WHERE  NOT EXISTS (SELECT created_at
                                      FROM   vaccine_status_overseas
                                      WHERE  alpha2_code = %s
                                             AND standard_date = %s);
                   """

for idx, row in df.iterrows():
    insert_vaccine_status_overseas_parmas = row.tolist()
    insert_vaccine_status_overseas_parmas.append(row["alpha2_code"])
    insert_vaccine_status_overseas_parmas.append(row["standard_date"])
    cursor.execute(insert_sql_query, insert_vaccine_status_overseas_parmas)
    connection.commit()
