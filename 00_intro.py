from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='00_intro',
    description='first DAG',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1),
    # schedule='* * * * *',
) as dag:
    
    t1 = BashOperator(
        task_id='first_task',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='second_task',
        bash_command='echo hello!!!!!!!!!!'
    )

    t1 >> t2

