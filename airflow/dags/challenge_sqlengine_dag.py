''' basic ETL DAG with sqlalchemy engine'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from sqlalchemy import create_engine


with DAG(
    dag_id='challenge_sqloperator_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    dir_path = Variable.get("CHALLENGE_FILES_DIR")
    raw_data_file_path = f'{dir_path}/airflow-extract-data.csv'
    transformed_data_file_path = f'{dir_path}/airflow-transform-data.csv'
    db_file_path = f'{dir_path}/airflow-load-db.db'

    # commented extract task since the url is not available anymore.
    # extract_task = BashOperator(
    #     task_id='extract_task',
    #     bash_command=f'wget -c https://datahub.io/core/s-and-p-500-companies/r/constituents.csv -O {raw_data_file_path}'
    #     )

    def transform_data():
        """Read input file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv(raw_data_file_path)
        companies_per_sector_df = df.groupby('Sector')[['Name']].count()
        companies_per_sector_df['Date'] = today.strftime('%Y-%m-%d')
        companies_per_sector_df.to_csv(transformed_data_file_path)
        return transformed_data_file_path

    def load_data(ti):
        transformed_data_file_path = ti.xcom_pull(task_ids='transform_task')
        companies_per_sector_df = pd.read_csv(transformed_data_file_path)
        engine = create_engine('sqlite:///{db_file_path}')
        companies_per_sector_df.to_sql(name = 'sp_500_sector_count1', con=engine,
                                       if_exists='replace')
        query = 'select * from sp_500_sector_count1'
        print(pd.read_sql(query, engine).head())
        print('Top level Domain Table is successfully loaded!')

    def display_data():
        engine = create_engine('sqlite:///{db_file_path}')
        query = 'select * from sp_500_sector_count1'
        companies_per_sector_df = pd.read_sql(query, engine)
        print(companies_per_sector_df.head())
        return companies_per_sector_df.to_json()


    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)


    load_task = PythonOperator(
        task_id='load_task',
        python_callable = load_data,
    )

    display_result = PythonOperator(
        task_id='display_result',
        python_callable=display_data,
        dag = dag
    )

    # extract_task >>
    transform_task >> load_task >> display_result