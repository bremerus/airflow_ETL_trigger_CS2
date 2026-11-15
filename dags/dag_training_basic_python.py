from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args={
    'owner': 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    dag_id='dag_training_basic_python_v10',
    default_args=default_args,
    schedule_interval='2 0 * * *',
    start_date=datetime(2026, 10, 15)
)as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='test_mssql_connection',
        conn_id='my_mssql_conn',
        sql="""
            use airflowDB    
        
            if object_id('table_test', 'U') is not null
                drop table table_test

            create table table_test(
                product_id int,
                product_name nvarchar(50),
                order_date date
            );
        """
    )
    task1