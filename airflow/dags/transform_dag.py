''' Transform DAG '''
import os
from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

with DAG(
  dag_id='transform_dag',
  schedule_interval=None,
  start_date=datetime(2024, 2, 25),
  catchup=False) as dag:

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        raw_data_file_path = Variable.get("TOP_LEVEL_DOMAIN_NAMES_FILE_PATH")
        df = pd.read_csv(raw_data_file_path)
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime('%Y-%m-%d')
        transformed_data_file_path = Variable.get("TOP_LEVEL_DOMAIN_NAMES_TRANSFORMED_FILE_PATH")
        generic_type_df.to_csv(transformed_data_file_path, index=False)

    transform_task = PythonOperator(
      task_id='transform_task',
      python_callable=transform_data,
      dag=dag)