from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import csv
import os
import random

def generate_random_review():
    now = datetime.now()
    file_name = now.strftime('%H%M%S') + '.csv'
    BASE = os.path.expanduser('~/damf2/data/review_data')

    file_path = f'{BASE}/{file_name}'

    review_data = []
    for _ in range(20):
        user_id = random.randint(1, 100)
        movie_id = random.randint(1, 1000)
        rating = random.randint(1, 5)
        review_data.append([user_id, movie_id, rating])

    os.makedirs(BASE, exist_ok=True)

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id', 'movie_id', 'rating'])
        writer.writerows(review_data)

with DAG(
    dag_id='03_generate_review',
    description='movie review',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1)
) as dag:
    t1 = PythonOperator(
        task_id='review_generate',
        python_callable=generate_random_review
    )

    t1