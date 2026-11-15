from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'primer',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first Airflow dag that we write.',
    start_date=datetime(2026, 10, 10,2),
    schedule_interval='@daily'
) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo Hello world this is the first task nig!!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am the secodn task and I will be running after task1 !"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey, i am task3 and i will be running at the same time with task 2 after task1 !"
    )

    # dependancy method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # dependancy method 2
    # task1 >> task2
    # task1 >> task3

    # dependancy method 3
    task1 >> [task2, task3]