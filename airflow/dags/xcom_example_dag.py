from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

default_args = {
        'owner': 'Paniz',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'catchup': False,
        'start_date': pendulum.today('UTC').add(days=-1)
}

def input(value):
    print('Value is {value}!'.format(value = value))
    return value

def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='input_task')
    value *= 100
    print('Updated value is {value}!'.format(value = value))
    return value

with DAG(
    dag_id='xcom_dag',
    description='Communicating between tasks with Xcom',
    schedule=None,
    default_args=default_args
) as dag:
    input = PythonOperator(
        task_id = 'input_task',
        python_callable=input,
        op_kwargs={'value':1}
    )

    multiply_by_100 = PythonOperator(
        task_id= 'multiply_by_100_task',
        python_callable=multiply_by_100,
    )

input >> multiply_by_100