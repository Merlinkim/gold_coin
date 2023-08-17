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
    'start_date': datetime(2023, 7, 5,tzinfo=local_tz),
    'retries': 0,
}

with DAG(
    'b4_data',
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
    name_roller=EmptyOperator(task_id='name_roller')
    execution_checker=EmptyOperator(task_id='execution_checker')
    api_caller=EmptyOperator(task_id='API_Caller')


    start >> name_roller >> execution_checker >> api_caller >> end


