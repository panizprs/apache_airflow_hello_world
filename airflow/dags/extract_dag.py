''' Extract DAG '''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable

with DAG('extract_dag',
         schedule_interval=None,
         start_date=datetime(2024, 2, 24),
         catchup=False) as dag:


    file_path = Variable.get("TOP_LEVEL_DOMAIN_NAMES_FILE_PATH")
    extract_task = BashOperator(
            task_id="extract_task",
            bash_command =f'wget -c https://datahub.io/core/top-level-domain-names/'
                          f'r/top-level-domain-names.csv.csv -O {file_path}'
            )
    # print(os.getenv("TOP_LEVEL_DOMAIN_NAMES_FILE_PATH"))