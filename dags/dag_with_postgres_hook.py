import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args= {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

def postgres_to_s3(ds_nodash , next_ds_nodash):
    # step 1: query data from postgresql db and save it into a text file
    # step 2: upload
    hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date >= %s and date <= %s",
                   (ds_nodash , next_ds_nodash))

    with NamedTemporaryFile(mode='w', suffix=f'{ds_nodash}') as f:
    # with open(f"dags/get_orders_{ds_nodash}.txt", "w", newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders_{ds_nodash}.txt")
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename = f.name,
            bucket_name="airflow",
            key=f"orders/{ds_nodash}.txt",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)


with DAG(
    dag_id='dag_with_postgres_hooks_v06',
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    end_date=datetime(2022, 5, 2),
    schedule_interval='@daily',
    catchup=True
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    task1