from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import os
import subprocess

def upload_to_hdfs():
    local_dir = os.path.expanduser('~/damf2/data/review_data')
    hdfs_dir = '/input/review_data'

    # hdfs dfs -mkdir -p /input/review_data
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir])

    files = []

    for file in os.listdir(local_dir):
        # ~/damf2/data/review_data + 114147.csv
        files.append(file)

    for file in files:
        local_file_path = os.path.join(local_dir, file) 
        hdfs_file_path = f'{hdfs_dir}/{file}'

        # hdfs dfs -put local_file hdfs_file
        subprocess.run(['hdfs', 'dfs', '-put', local_file_path, hdfs_file_path])

        os.remove(local_file_path)


with DAG(
    dag_id='04_upload_to_hdfs',
    description='upload',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5)
) as dag:
    t1 = PythonOperator(
        task_id='upload',
        python_callable=upload_to_hdfs
    )

    t1