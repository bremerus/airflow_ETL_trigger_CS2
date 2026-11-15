from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    dag_id = 'dag_with_cron_expression_v04',
    default_args = default_args,
    start_date = datetime(2026, 10, 1, 2),
    schedule_interval='0 3 * * tue,fri'
)
def bash_order():
    task1 = BashOperator(
        task_id='first_task',
        bash_command = 'echo This is a dag with cron expression'
    )
    task1
mydag = bash_order()