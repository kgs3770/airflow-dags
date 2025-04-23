from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import time
import requests
import os
import csv

def collect_upbit_data():
    upbit_url = 'https://api.upbit.com/v1/ticker'
    params = {'markets': 'KRW-BTC'}

    collected_data = []

    start_time = time.time()
    while time.time() - start_time < 60:
        res = requests.get(upbit_url, params=params)
        data = res.json()[0]

        csv_data = [data['market'], data['trade_date'], data['trade_time'], data['trade_price']]
        collected_data.append(csv_data)

        time.sleep(5)

    # 파일 저장
    now = datetime.now()
    file_name = now.strftime('%H%M%S') + '.csv'
    BASE = os.path.expanduser('~/damf2/data/bitcoin')
    file_path = f'{BASE}/{file_name}'

    os.makedirs(BASE, exist_ok=True)

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(collected_data)

with DAG(
    dag_id='05_bitcoin',
    description='crawling',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1)
) as dag:
    t1 = PythonOperator(
        task_id='collect_bitcoin',
        python_callable=collect_upbit_data
    )

    t1