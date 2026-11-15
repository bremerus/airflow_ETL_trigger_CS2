from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_catchup_backfill_v02',
    default_args=default_args,
    start_date=datetime(2026, 10, 10, 2),
    schedule_interval='@daily',
    catchup=False
)
def bash_order():

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo Hello this is a catchup"
    )

    task1
mydag = bash_order() 