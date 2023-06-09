from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    'minute-data-saving',
    default_args=default_args,
    schedule_interval='0 5 * * * *'
)as dag:
    start=BashOperator(
        task_id = 'start',
        bash_command="echo 'start'" 
    )
    end=BashOperator(
        task_id='end',
        bash_command="echo 'end'"
    )
    data_calling = PythonOperator(
        task_id='datacalling',
        python_callable=save_and_send
        )  ## csv파일 떨어뜨리고 api정보 
    data_save = EmptyOperator(task_id='data_saving_to_db')
    data_preprocessing = EmptyOperator(task_id='preprocessing')
    post_data_prece = EmptyOperator(task_id='post_data_pre')
    post_data_saving = EmptyOperator(task_id='post_data_save')
    chart_data = EmptyOperator(task_id='chart_data')
    model_input = EmptyOperator(task_id='model_input')
    model_output = EmptyOperator(task_id='model_output')
    chart_data_api = EmptyOperator(task_id='chart_data_api')
    dash_board = EmptyOperator(task_id='dash_board')

    start >> data_calling >> data_save >> [data_preprocessing,post_data_saving,chart_data]
    data_preprocessing >> model_input >> model_output
    post_data_saving >> post_data_prece >> model_input >> model_output
    chart_data >> chart_data_api
    [model_output,chart_data_api] >> dash_board >> end