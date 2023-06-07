import requests
import pandas as pd
from sqlalchemy import create_engine

class coin_min_data:
    def __init__(self,db_info):
        self.db_info = db_info

    def insert(self):
        return 0

######SQL Connection info
DB = 'coin'
usr = 'inseong'
pwd = 'Kiminseong!1'
HOST='coinMysql'

engine = create_engine(f'mysql+pymysql://{usr}:{pwd}@{HOST}/{DB}?charset=utf8mb4')

headers = {"accept": "application/json"}

url = (f"https://api.upbit.com/v1/candles/minutes/1?market={coin_name}&count=1")


response = requests.get(url, headers=headers)

name_list = pd.DataFrame(response.json())

name_list.to_sql(name=coin_name, con=engine, if_exists='append', index=False)
