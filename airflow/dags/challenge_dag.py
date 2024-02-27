''' Basic ETL DAG'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

with DAG(
    dag_id='challenge_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    dir_path = Variable.get("CHALLENGE_FILES_DIR")
    raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'
    transformed_data_file_path = f'{dir_path}/airflow-transform-data.csv'
    db_file_path = f'{dir_path}/airflow-load-db.db'

    extract_task = BashOperator(
        task_id='extract_task',
        bash_command=f'wget -c https://datahub.io/core/s-and-p-500-companies/r/constituents.csv -O {raw_data_file_path}'
        )

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        companies_per_sector_df = df.groupby('Sector')[['Name']].count()
        companies_per_sector_df['Date'] = today.strftime('%Y-%m-%d')
        companies_per_sector_df.to_csv(transformed_data_file_path)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)

    load_task = BashOperator(
        task_id='load_task',
        bash_command=f'echo -e ".separator ","\n.import --skip 1 {transformed_data_file_path} '
                     f'sp_500_sector_count" | sqlite3 {db_file_path}',
        dag=dag)

    extract_task >> transform_task >> load_task