from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from utils.json_to_csv import *

with DAG(
    dag_id='08_yt_data_ETL',
    description='json to csv',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=10)
) as dag:
    t1 = PythonOperator(
        task_id='convert',
        python_callable=convert_json_to_csv
    )

    t1