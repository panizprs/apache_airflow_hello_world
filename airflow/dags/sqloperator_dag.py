''' SQL Operator ETL DAG'''
from datetime import datetime, date
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow import DAG


with DAG(
    dag_id='sqloperator_dag',
    schedule_interval=None,
    start_date=datetime(2024, 2, 27),
    catchup=False) as dag:

    create_table = SqliteOperator(
        task_id='create_table',
        sql=r"""
                CREATE TABLE IF NOT EXISTS top_level_domains1 (
                    Domain varchar(30),
                    Type varchar(30),
                    SponseringOrganization varchar(30),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
        sqlite_conn_id='sqlite_conn',
        dag=dag,
    )

    insert_data = SqliteOperator(
            task_id='insert_data',
            sql=r"""
                    insert into top_level_domains1 
                        (Domain, Type, SponseringOrganization) values
                        ('.abogado', 'generic', 'Top Level Domain Holdings Limited'),
                        ('.academy', 'generic', 'dot Accountant Limited');
                """,
            sqlite_conn_id='sqlite_conn',
            dag=dag,
        )

    display_result = SqliteOperator(
        task_id='display_result',
        sql=r"""SELECT * FROM top_level_domains1""",
        sqlite_conn_id='sqlite_conn',
        dag=dag,
        do_xcom_push=True
    )

    create_table >> insert_data >> display_result