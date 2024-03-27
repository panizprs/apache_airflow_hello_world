''' Simple DAG executing branch with task group '''
from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label


with DAG(
    dag_id='task_group_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    dir_path = Variable.get("CHALLENGE_FILES_DIR")
    raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'


    def read_data(ti, raw_data_file_path):
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        df['Date'] = today.strftime('%Y-%m-%d')
        ti.xcom_push(key='csv_file', value = df.to_json())

    def determine_branch():
        '''
        Choosing between count by name (count_name) and count by sector (count_sector).
        '''
        branching_variable = Variable.get('transform_action')
        return "aggregating.{0}".format(branching_variable)


    def count_name(ti):
        df_json = ti.xcom_pull(key='csv_file')
        df = pd.read_json(df_json)
        result_df = df.groupby('Name')['Symbol'].count()
        return result_df.to_json()


    def count_sector(ti):
        df_json = ti.xcom_pull(key='csv_file')
        df = pd.read_json(df_json)
        result_df = df.groupby('Sector')['Symbol'].count()
        return result_df.to_json()

    with TaskGroup('read') as read:
        transform_task = PythonOperator(
            task_id='read_task',
            python_callable=read_data,
            op_kwargs={'raw_data_file_path': raw_data_file_path},
            dag=dag)

    determine_branch = BranchPythonOperator (
        task_id= 'determine_branch',
        python_callable=determine_branch
    )

    with TaskGroup('aggregating') as aggregating:
        count_name = PythonOperator(
            task_id='count_name',
            python_callable=count_name
        )
        count_sector = PythonOperator(
            task_id='count_sector',
            python_callable=count_sector
        )

    transform_task >> Label('preprocessed data') >> determine_branch >> Label('branch on Condition') >> aggregating
