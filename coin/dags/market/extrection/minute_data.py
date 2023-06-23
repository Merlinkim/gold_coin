from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum


local_tz = pendulum.timezone("Asia/Seoul")



####DAGS
default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1,tzinfo=local_tz),
    'retries': 0,
}

with DAG(
    'calling_Minute_data',
    default_args=default_args,
    schedule_interval='* * * * *'
)as dag:
    start=BashOperator(
        task_id = 'start',
        bash_command="echo 'start'" 
    )
    end = BashOperator(
        task_id = 'END',
        bash_command="echo 'END'"
    )

    exe_date = '{{execution_date.add(hours=9).strftime("%Y%m%d%")}}'
    exe_hour = '{{execution_date.add(hours=9).strftime("%H")}}'
    exe_min = '{{execution_date.add(hours=9).strftime(%M)}}'
    local_min = f'/Users/merlinkim/tmp/data/minute/{exe_date}/{exe_hour}/{exe_min}'
    name_data = f'/Users/merlinkim/tmp/data/name/{exe_date}/min_name.txt'
    dfs_route = f'/coin/data/minute/{exe_date}/{exe_min}'
    sh_route = f'/Users/merlinkim/code/etl/merlin/dags/coin/coin/dags/market/extrection/'
    dir_make = BashOperator(
        task_id = 'make_dir_local',
        bash_command='''
        mkdir -p /Users/merlinkim/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}
        '''
        )

    calling_name = BashOperator(
        task_id = 'coin_name_call',
        bash_command=
        f"sh {sh_route}drop_data_local.sh "'/Users/merlinkim/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}'" "'/Users/merlinkim/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/min_name.txt'""
        )
    make_dfs_file = BashOperator(
        task_id = 'make_dfs_dir',
        bash_command='''
        hdfs dfs -mkdir -p /coin/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}
        '''
        )
    upload_to_dfs = BashOperator(
        task_id = 'upload_to_dfs',
        bash_command='''
        hdfs dfs -copyFromLocal /Users/merlinkim/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}/* /coin/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}/
        '''  
        )

start >> dir_make >> calling_name >> make_dfs_file >> upload_to_dfs >> end