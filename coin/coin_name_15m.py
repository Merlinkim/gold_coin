import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# MySQL 연결 설정
engine = create_engine('mysql+pymysql://inseong:Kiminseong!1@coinMysql/coin?charset=utf8mb4')
'''코인 이름
형식
[{"market":"BTC-KRW","korean_name":"비트코인","english_name":"bitcoin"}]
'''
conn = pymysql.connect(host='coinMysql',
                       user='inseong',
                       password='Kiminseong!1',
                       db='coin',
                       charset='utf8')

table_name = 'coin_name' # 삽입할 테이블 이름

cursor = conn.cursor()

url = "https://api.upbit.com/v1/market/all?isDetails=false"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

#name_list = pd.DataFrame(response.json())

name_list = pd.DataFrame(response.json())

name_KRW = name_list[name_list["market"].str[:3] == "KRW"]

name_KRW.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

engine.dispose()