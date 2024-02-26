''' Extract DAG '''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG
import os

with DAG('extract_dag',
         schedule_interval=None,
         start_date=datetime(2024, 2, 24),
         catchup=False) as dag:

    extract_task = BashOperator(
            task_id='extract_task',
            bash_command=f"wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv "
                         f"-O {os.getenv('TOP_LEVEL_DOMAIN_NAMES_FILE_PATH')}"
            )