''' Load DAG '''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable

with DAG('load_dag',
    start_date=datetime(2024, 2, 27),
    schedule_interval=None,
    catchup=False) as dag:
    dir_path = Variable.get("TOP_LEVEL_DOMAIN_FILES_DIR")
    transformed_data_file_path = f'{dir_path}/airflow-transform-data.csv'
    db_file_path = f'{dir_path}/airflow-load-db.db'

    load_task = BashOperator(
        task_id='load_task',
        bash_command=f'echo -e ".separator ","\n.import --skip 1 {transformed_data_file_path} '
                     f'top_level_domains" | sqlite3 {db_file_path}',
        dag=dag
    )