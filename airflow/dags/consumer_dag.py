''' Simple consumer DAG depending producer_dag to complete and produce dataset '''
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG, Dataset
from airflow.models import Variable

dir_path = Variable.get("CHALLENGE_FILES_DIR")
updated_data_file_path = f'{dir_path}/airflow-update-data.csv'

updated_dataset = Dataset(updated_data_file_path)

with DAG(
    dag_id='consumer_dag',
    start_date=datetime(2024, 2, 27),
    schedule = [updated_dataset],
    catchup=False) as dag:

    def count_sector():
        df = pd.read_csv(updated_data_file_path)
        result_df = df.groupby('Sector')['Symbol'].count()
        return result_df.to_json()

    count_sector = PythonOperator(
        task_id='count_sector',
        python_callable=count_sector
    )

    count_sector
