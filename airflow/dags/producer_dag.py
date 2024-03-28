''' Simple producer DAG producing dataset, which may be monitoring by consumer_dag '''
from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG, Dataset
from airflow.models import Variable

dir_path = Variable.get("CHALLENGE_FILES_DIR")
raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'
updated_data_file_path = f'{dir_path}/airflow-update-data.csv'

updated_dataset = Dataset(updated_data_file_path)

with DAG(
    dag_id='producer_dag',
    start_date=datetime(2024, 2, 27),
    schedule_interval = '@once',
    catchup=False) as dag:

    def read_data(raw_data_file_path):
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        df['Date'] = today.strftime('%Y-%m-%d')
        df.to_csv(updated_data_file_path, index= False)

    read_task = PythonOperator(
        task_id='read_task',
        python_callable=read_data,
        op_kwargs={'raw_data_file_path': raw_data_file_path},
        outlets=[updated_dataset],
        dag=dag)


    read_task
