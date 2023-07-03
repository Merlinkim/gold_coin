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
    'start_date': datetime(2023, 7, 4,tzinfo=local_tz),
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
    local_min = f'$HOME/tmp/data/minute/{exe_date}/{exe_hour}/{exe_min}'
    name_data = f'$HOME/tmp/data/name/{exe_date}/min_name.txt'
    dfs_route = f'/coin/data/minute/{exe_date}/{exe_min}'
    sh_route = 'dags/market/extrection/drop_data_local.sh'

    line_mesg = "curl -X POST -H 'Authorization: Bearer 6Y3IVP0dZD9bREhqMS4pd0sZZg5QAh3N9eAcixrovns' -F 'message=is problem' https://notify-api.line.me/api/notify"

######### local
    dir_make = BashOperator(
        task_id = 'make_dir_local',
        bash_command='''
        mkdir -p $HOME/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}
        '''
        )
##### local
    calling_name = BashOperator(
        task_id = 'coin_name_call',
        bash_command=
        f"sh {sh_route}drop_data_local.sh "'$HOME/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}'" "'$HOME/tmp/data/min_name/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/min_name.txt'""
        )
    upload_to_dfs = BashOperator(
        task_id = 'upload_to_dfs',
        bash_command='''
        gsutil cp $HOME/tmp/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}/* gs://coindataformachine/coin/data/minute/{{execution_date.add(hours=9).strftime("%Y%m%d")}}/{{execution_date.add(hours=9).strftime("%H")}}/{{execution_date.add(hours=9).strftime("%M")}}/
        '''  
        )
    error = BashOperator(
        task_id = 'error_message',
        bash_command=f'{line_mesg}'
    )

start >> dir_make >> calling_name >> upload_to_dfs >> error >> end