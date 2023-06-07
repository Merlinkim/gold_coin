from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
now = datetime.now()
line_key='L6VEYLTIWw3ZZL3a5hmF2SBpmKCK1s6hzHWCkhd02z'
id_task='{{run_id}}'
execution_d = '{{execution_date}}'
message =f"airflow/coin data \n{now} \n{id_task}----{execution_d} \nhave some error"
line_mesg = f"curl -X POST -H 'Authorization: Bearer {line_key},' -F 'message={message}' https://notify-api.line.me/api/notify"

default_args = {
    'owner': 'merlin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    'market-data-saving',
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
    table_maker = BashOperator(
        task_id = 'table_maker',
        bash_command="python dags/coin/operpy/coin_def.py"
    )
    error = BashOperator(
        task_id = 'error',
        bash_command=f'{line_mesg}'
    )

start >> table_maker >> end