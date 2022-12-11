from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from transformation import *

default_args = {

    'owner':'chanz',
    'retries':5,
    'retry_delay': timedelta(minutes=2)

}


with DAG(
    
    dag_id='my_dag1',
    default_args =default_args,
    description='It is my first dag',
    start_date=datetime(2022,12,1,2),
    schedule_interval='@daily'
 
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="Hello , my first task"
    )

    task1