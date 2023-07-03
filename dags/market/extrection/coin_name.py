from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
from upbit_def import min_data

local_tz = pendulum.timezone("Asia/Seoul")

####이름 정보를 가져오는 dag
default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 3,tzinfo=local_tz),
    'retries': 0,
}

with DAG(
    'calling_name_data',
    default_args=default_args,
    schedule_interval='0 15 * * *'
)as dag:
    start=BashOperator(
        task_id = 'start',
        bash_command="echo 'start'" 
    )
    end = BashOperator(
        task_id = 'END',
        bash_command="echo 'END'"
    )
    name_data_path = '$HOME/tmp/data/min_name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/'

    dir_make = BashOperator(
        task_id = 'make_dir_local',
        bash_command= '''
        mkdir -p $HOME/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}
        '''
    )
    calling_name = BashOperator(
        task_id = 'coin_name_call',
        bash_command="""
        curl --request GET \
        --url 'https://api.upbit.com/v1/market/all?isDetails=false' \
        --header 'accept: application/json' >> $HOME/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/name.json
     """
    )
    upload_to_dfs = BashOperator(
        task_id = 'upload_to_dfs',
        bash_command='gsutil cp $HOME/tmp/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/name.json gs://coindataformachine/coin/data/name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/'
    )
    mkdir_min_name = BashOperator(
        task_id = 'min_name_dir',
        bash_command=f'mkdir -p {name_data_path}'
    )
    make_min_name = PythonOperator(
        task_id = 'min_name_maker',
        python_callable=min_data,
        op_args=[name_data_path]
    )

start >> dir_make >> calling_name >> upload_to_dfs >> mkdir_min_name >> make_min_name >> end