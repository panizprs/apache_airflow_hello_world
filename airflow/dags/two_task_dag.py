""" one task dag """

from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
        'owner': 'Paniz',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'catchup': False,
        'start_date': datetime(2023, 1, 1)
}

with DAG(
        dag_id='two_task_dag',
        description='A two-task Airflow DAG',
        schedule_interval=None,
        default_args=default_args
    ) as dag:

    task0 = BashOperator(
            task_id='bash_task_0',
            bash_command='echo "First Task!"')

    task1 = BashOperator(
        task_id='bash_task_1',
        bash_command='echo "Second Task!"')

    task0 >> task1