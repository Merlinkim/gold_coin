import requests
import pandas as pd


local_data = '/Users/merlinkim/tmp/data/'
origin = 'origin/'
new = 'new/'
tmp = 'tmp/'
minute_file = 'minute/'
dfs_data = '/coin/data/'
name_data = 'name.json'
minute_data = 'min_data.parqeunt'
local_new_name_path= f'{local_data}{name_data}'
old_local = f'{local_data}{new}{name_data}'
url = "https://api.upbit.com/v1/market/all?isDetails=false"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

json_file = response.json()

print(json_file)

json_coin = pd.DataFrame(json_file)

print(json_coin)

json_coin.to_json(local_new_name_path)

df2 = pd.read_json(local_new_name_path)

old = pd.read_json(old_local)

print(df2)
