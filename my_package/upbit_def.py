import requests
import pandas as pd
from sqlalchemy import create_engine

#def coin_name():
# MySQL 연결 설정
#    engine = create_engine('mysql+pymysql://inseong:Kiminseong!1@coinMysql/coin?charset=utf8mb4')
#    '''코인 이름
#    형식
#    [{"market":"BTC-KRW","korean_name":"비트코인","english_name":"bitcoin"}]
#    '''
######## 마켓 코드 가져오기
#    url = "https://api.upbit.com/v1/market/all?isDetails=false"
#
#    headers = {"accept": "application/json"}

#    response = requests.get(url, headers=headers)

#    name_list = pd.DataFrame(response.json())

#    print(name_list)
##############마켓코드를 데이터프레임으로 만들어서 sql로 저장
#    table_name = 'coin_name' 

#    name_list.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

#    engine.dispose()
#    return name_list

############ 분봉정보 저장
#####데이터베이스 테이블 이름은 -가 사용 불가하여 _로 변경
def min_data(savePath):
    json_name = f'{savePath}/name.json'

    df = pd.read_json(json_name)

    df.drop(columns=['korean_name','english_name'],axis=1,inplace=True)

    df['market']=df['market'].str.replace("-",'_')

    df.to_csv(f'{savePath}min_name.txt', sep = '\n' , index =False, header=False)

    return df


######### 데이터 비교 및 추가 삭제
def compare_by_df(origin,new):

    origin_df = pd.read_json(origin)

    new_df = pd.read_json(new)

    if not origin_df.equals(new_df):

        temp_df=pd.concat([origin_df,new_df])

        temp_df.drop_duplicates(inplace=True)

        temp_df.to_json('/Users/merlinkim/tmp/data/tmp/name.json')

        return print("name data has been changed. please check the changed data")
    
    else :

        origin_df.to_json('/Users/merlinkim/tmp/data/tmp/name.json')

        return print("name data didn't changed")

def status_adder(name_path):

    name_df = pd.read_json(name_path)

    name_df['status'] = 'non_start'

    name_df.set_index('status', inplace=True)

    name_df.to_json('name_path')

def status_checker(name_path):

    name_df=pd.read_json(name_path)

    for index, row in name_df.iterrows():

        if row['status'] == 'non_started' or row['status'] == 'progress':

            name_df.at[index,'status'] = 'progress'

            return row['market']

def count_documents_in_folder(folder_path):

    if os.path.exists(folder_path) and os.path.isdir(folder_path):

        files = os.listdir(folder_path)

        document_count = len(files)

        return document_count

    else:

        return 0


def execute_date_checker(route_path,now_market):

    num_file=count_documents_in_folder(route_path)

    data_path=f"{route_path}/{now_market}/{num_file}"

    data_df=pd.read_json(data_path)

    sort_data_df=data_df.sort_values(by='candle_date_time_kst',ascending=False)

    b4_slicing_date=sort_data_df.loc[0,'candle_date_time_kst']
    #"candle_date_time_kst":"2023-08-18T00:59:00"
    #{yyyy}-{MM}-{dd}T{hh}%3A{mm}%3A{ss}
    kst_year=b4_slicing_date[:4]
    kst_month=b4_slicing_date[5:7]
    kst_day=b4_slicing_date[8:10]
    kst_hour=b4_slicing_date[11:13]
    kst_min=b4_slicing_date[14:16]


    execute_date=f"{kst_year}-{kst_month}-{kst_day}T{kst_hour}%3A{kst_min}%3A00"

    list_for_save=[execute_date,num_file]

    return list_for_save


def api_calling_upbit(market,date_info):

    url = f"https://api.upbit.com/v1/candles/minutes/1?market={market}&to={date_info}%2B09%3A00&count=180"

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)

    parsed_data=json_load(response)

    data_for_save=pd.DataFrame(parsed_data)

    return data_for_save

def api_with_execute(route_path,name_path):

    now_market=status_checker(name_path)

    execute_date_info=execute_date_checker(route_path,now_market)

    api_data=api_calling_upbit(now_market,execute_date_info[0])

    api_data.to_json(f"{route_path}/{execute_date_info[1]}")






