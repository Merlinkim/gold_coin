from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from upbit_def import compare_by_df, min_data



now = datetime.now()

id_task='{{run_id}}'

execution_d = '{{execution_date}}'

line_mesg = "curl -X POST -H 'Authorization: Bearer 6Y3IVP0dZD9bREhqMS4pd0sZZg5QAh3N9eAcixrovns' -F 'message=is problem' \
https://notify-api.line.me/api/notify"

local_data = '/Users/merlinkim/tmp/data/'
origin = 'origin/'
new = 'new/'
tmp = 'tmp/'
minute_file = 'minute/'
dfs_data = '/coin/data/'
name_data = 'name.json'
minute_data = 'min_data.parqeunt'

##### LOCAL
local_origin_name_path = f'{local_data}{origin}{name_data}'

local_new_name_path= f'{local_data}{new}{name_data}'

local_data_min = f'{local_data}{minute_file}{minute_data}'

#######    HDFS
dfs_data_origin_name = f'{dfs_data}{origin}{name_data}'

dfs_data_new_name = f'{dfs_data}{new}{name_data}'

dfs_data_min = f'{dfs_data}{minute_file}{minute_data}'

####DAGS
default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 15),
    'retries': 0,
}

with DAG(
    'upbit-coin-name-and-minute-data',
    default_args=default_args,
    schedule_interval='0 5 * * * *'
)as dag:
    start=BashOperator(
        task_id = 'start',
        bash_command="echo 'start'" 
    )
    end = BashOperator(
        task_id = 'END',
        bash_command="echo 'END'"
    )
    raw_name = BashOperator(
        task_id = 'raw_name_data_save',
        bash_command=f"""
        curl --request GET \
        --url 'https://api.upbit.com/v1/market/all?isDetails=false' \
        --header 'accept: application/json' >> {local_new_name_path}
        """
    )
    rm_local_origin = BashOperator(
        task_id = 'delete_local_origin',
        bash_command=f'rm -rf {local_origin_name_path}'
    )
    calling_dfs = BashOperator(
        task_id = 'origin_calling',
        bash_command=f"hdfs dfs -copyToLocal {dfs_data_origin_name} {local_origin_name_path}"
    )
    compare_name = PythonOperator(
        task_id = "compare_name_data",
        python_callable=compare_by_df,
        op_args=[local_origin_name_path,local_new_name_path]
    )
    rm_name_origin = BashOperator(
        task_id = "dfs_origin_name_rm",
        bash_command=f"hdfs dfs -rm {dfs_data_origin_name}"
    )
    transfer_name_data = BashOperator(
        task_id = "transfer_name_data_to_hdfs",
        bash_command=f"hdfs dfs cp {local_new_name_path} {dfs_data_origin_name}"
    )
    error = BashOperator(
        task_id = 'error',
        bash_command=f'{line_mesg}'
    )
    min_data_call = PythonOperator(
        task_id='calling_api_of_min',
        python_callable=min_data,
        op_args=[local_new_name_path,local_data_min]
        ) ### -> 분당 정보를 불러서 로컬에 바로 떨어뜨리기
    
    hdfs_sander = BashOperator(
        task_id = 'dfs_sander',
        bash_command=f'hdfs dfs -put {local_data_min} {dfs_data_min}'
        )    # 로컬의 raw파일을 hdfs로 전송
    hdfs_combine = EmptyOperator(task_id='combine')   # dfs에서 hive에 넘기기 위해서 파일을 parquent로 변환 및 삽입
    
start >> raw_name >> rm_local_origin >> [calling_dfs,error]
calling_dfs >> compare_name
compare_name >> [rm_name_origin,min_data_call]
rm_name_origin>> transfer_name_data >> end
min_data_call >> hdfs_sander >> hdfs_combine >> end