''' Simple DAG executing branch '''
from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.models import Variable

with DAG(
    dag_id='branching_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    dir_path = Variable.get("CHALLENGE_FILES_DIR")
    raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'


    def transform_data(raw_data_file_path):
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        df['Date'] = today.strftime('%Y-%m-%d')
        return df.to_json()

    def determine_branch():
        '''
        Choosing between count by name (count_name) and count by sector.
        '''
        branching_variable = Variable.get('transform_action')
        return branching_variable


    def count_name(ti):
        df_json = ti.xcom_pull(task_ids='transform_task')
        df = pd.read_json(df_json)
        result_df = df.groupby('Name')['Symbol'].count()
        return result_df.to_json()


    def count_sector(ti):
        df_json = ti.xcom_pull(task_ids='transform_task')
        df = pd.read_json(df_json)
        result_df = df.groupby('Sector')['Symbol'].count()
        return result_df.to_json()

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        op_kwargs={'raw_data_file_path': raw_data_file_path},
        dag=dag)

    determine_branch = BranchPythonOperator (
        task_id= 'determine_branch',
        python_callable=determine_branch
    )

    count_name = PythonOperator(
        task_id='count_name',
        python_callable=count_name
    )
    count_sector = PythonOperator(
        task_id='count_sector',
        python_callable=count_sector
    )

    transform_task >> determine_branch >> [count_name, count_sector]
