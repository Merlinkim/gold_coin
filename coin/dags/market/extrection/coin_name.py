from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
from upbit_def import min_data

local_tz = pendulum.timezone("Asia/Seoul")

####DAGS
default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 22,tzinfo=local_tz),
    'retries': 0,
}

with DAG(
    'calling_name_data',
    default_args=default_args,
    schedule_interval='0 9 * * *'
)as dag:
    start=BashOperator(
        task_id = 'start',
        bash_command="echo 'start'" 
    )
    end = BashOperator(
        task_id = 'END',
        bash_command="echo 'END'"
    )
    name_data_path = '/Users/merlinkim/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/'
    dir_make = BashOperator(
        task_id = 'make_dir_local',
        bash_command= '''
        mkdir /Users/merlinkim/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}
        '''
    )
    calling_name = BashOperator(
        task_id = 'coin_name_call',
        bash_command="""
        curl --request GET \
        --url 'https://api.upbit.com/v1/market/all?isDetails=false' \
        --header 'accept: application/json' >> /Users/merlinkim/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/name.json
     """
    )
    make_dfs_file = BashOperator(
        task_id = 'make_dfs_dir',
        bash_command='hdfs dfs -mkdir -p /coin/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}'
    )
    upload_to_dfs = BashOperator(
        task_id = 'upload_to_dfs',
        bash_command='hdfs dfs -put /Users/merlinkim/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/name.json /coin/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/name.json'
    )
    make_min_name = PythonOperator(
        task_id = 'min_name_maker',
        python_callable=min_data,
        op_args=[name_data_path]
    )

start >> dir_make >> calling_name >> make_dfs_file >> upload_to_dfs >> make_min_name >> end