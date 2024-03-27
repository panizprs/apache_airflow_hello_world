from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
        'owner': 'Paniz',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'catchup': False,
        'start_date': days_ago(1)
}

dags_dir = Variable.get('AIRFLOW__CORE__DAGS_FOLDER')

with DAG (
    dag_id= 'multiple_bash_tasks',
    description= 'DAG with bash scripts',
    schedule_interval= None,
    template_searchpath= f'{dags_dir}/bash_scripts',
    default_args = default_args
) as dag:
        taskA = BashOperator(
                task_id= 'taskA',
                bash_command='taskA.sh'
        )

        taskB = BashOperator(
                task_id= 'taskB',
                bash_command='taskB.sh'
        )
        taskC = BashOperator(
                task_id='taskC',
                bash_command='taskC.sh'
        )
        taskD = BashOperator(
                task_id='taskD',
                bash_command='taskD.sh'
        )
        taskE = BashOperator(
                task_id='taskE',
                bash_command='taskE.sh'
        )
        taskF = BashOperator(
                task_id='taskF',
                bash_command='taskF.sh'
        )
        taskG = BashOperator(
                task_id='taskG',
                bash_command='taskG.sh'
        )

        taskA >> [taskB, taskC, taskD]
        taskB >> taskE
        taskC >> taskF
        taskD >> taskG