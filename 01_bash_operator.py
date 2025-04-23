from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='01_bash_operator',
    description='bash',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1)
) as dag:
    t1 = BashOperator(
        task_id='echo',
        bash_command='echo start!!!!'
    )

    my_command = '''
        {% for i in range(5) %}
            echo {{ ds }}
            echo {{ i }}
        {% endfor %}
    '''

    t2 = BashOperator(
        task_id='for',
        bash_command=my_command
    )

    t1 >> t2