import pandas as pd

# <-------------------Setting Database------------------->
from configparser import ConfigParser
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from sqlalchemy import create_engine

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

# <-------------------Get Data------------------->
url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"

new_df = pd.read_csv(url)
old_df = pd.read_sql_table('vaccine_ww_status',con=engine)

new_df['date'] = pd.to_datetime(new_df['date'])
old_df.drop(columns=['id'],inplace=True)
needa_upload_df = pd.concat([new_df,old_df]).drop_duplicates(keep=False)

# <-------------------Upload Data------------------->
needa_upload_df.to_sql(name='vaccine_ww_status', con=engine, if_exists='append',index=False)

print("fin")