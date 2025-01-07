

from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
        dag_id = 'demo',
        start_date = datetime(2022, 1, 1),
        schedule = '0 0 * * *'
) as dag:

    hello = BashOperator(task_id = 'hello', bash_command = 'echo hello')

    @task()
    def airflow():
        print('airflow')

    hello >> airflow()


    
