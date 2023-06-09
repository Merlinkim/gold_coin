from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from coin_def import coin_name,min_table,compare_by_df

now = datetime.now()
line_key='L6VEYLTIWw3ZZL3a5hmF2SBpmKCK1s6hzHWCkhd02z'
id_task='{{run_id}}'
execution_d = '{{execution_date}}'
message =f"airflow/coin data \n{now} \n{id_task}----{execution_d} \nhave some error"
line_mesg = f"curl -X POST -H 'Authorization: Bearer {line_key},' -F 'message={message}' https://notify-api.line.me/api/notify"
local_data = '/Users/inseongkim/code/coin/coin/data'
dfs_data = '/coin/data'

default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    'name',
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
        --header 'accept: application/json' >> {local_data}/tmp/name_data.json
        """
    )
    calling_dfs = BashOperator(
        task_id = 'origin_calling',
        bash_command=f"hdfs dfs -copyToLocal {dfs_data}/origin/name_data.json {local_data}/origin/name_data.json"
    )
    compare_name = PythonOperator(
        task_id = "compare_name_data",
        python_callable=compare_by_df
    )
    rm_name_origin = BashOperator(
        task_id = "dfs_origin_name_rm",
        bash_command=f"hdfs dfs -rm {dfs_data}/origin/name_data.json"
    )
    transfer_name_data = BashOperator(
        task_id = "transfer_name_data_to_hdfs",
        bash_command=f"hdfs dfs cp {local_data}/tmp/change_data.json {dfs_data}/origin/"
    )
    error = BashOperator(
        task_id = 'error',
        bash_command=f'{line_mesg}'
    )
    min_data_call = PythonOperator(task_id='calling_api_of_min') ### -> 분당 정보를 불러서 로컬에 바로 떨어뜨리기
    hdfs_sander = EmptyOperator(task_id = 'dfs_sander')    # 로컬의 raw파일을 hdfs로 전송
    hdfs_combine = EmptyOperator(task_id='combine')   # dfs에서 hive에 넘기기 위해서 파일을 parquent로 변환 및 삽입
    
start >> raw_name >> [calling_dfs,error] >> compare_name
compare_name >> rm_name_origin >> transfer_name_data >> end