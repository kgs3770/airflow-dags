from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
# pip install boto3
import boto3
# pip install python-dotenv
from dotenv import load_dotenv
import os

load_dotenv('/home/ubuntu/airflow/.env')

def upload_to_s3():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='ap-northeast-2'
    )

    local_dir = os.path.expanduser('~/damf2/data/bitcoin') 
    bucket_name = 'damf2-och'
    s3_prefix = 'bitcoin-och/'

    files = []
    for file in os.listdir(local_dir):
        files.append(file)

    for file in files:
        local_file_path = os.path.join(local_dir, file)
        s3_path = f'{s3_prefix}{file}'

        s3.upload_file(local_file_path, bucket_name, s3_path)

        os.remove(local_file_path)


with DAG(
    dag_id='06_upload_to_s3',
    description='s3',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5)
) as dag:
    t1 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    t1