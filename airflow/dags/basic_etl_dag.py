''' Basic ETL DAG'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

with DAG(
    dag_id='basic_etl_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    dir_path = Variable.get("TOP_LEVEL_DOMAIN_FILES_DIR")
    raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'
    transformed_data_file_path = f'{dir_path}/airflow-transform-data.csv'
    db_file_path = f'{dir_path}/airflow-load-db.db'

    extract_task = BashOperator(
        task_id='extract_task',
        bash_command=f'wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O {raw_data_file_path}'
        )

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime('%Y-%m-%d')
        generic_type_df.to_csv(transformed_data_file_path, index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)

    load_task = BashOperator(
        task_id='load_task',
        bash_command=f'echo -e ".separator ","\n.import --skip 1 {transformed_data_file_path} top_level_domains" | sqlite3 {db_file_path}',
        dag=dag)

    extract_task >> transform_task >> load_task