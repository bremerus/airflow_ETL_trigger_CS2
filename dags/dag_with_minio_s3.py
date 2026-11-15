from datetime import timedelta, datetime
from airflow import DAG



default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_minio_s3_v02',
    start_date=datetime(2026, 10, 10),
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    task1 = S3KeySensor(
        task_id = 'sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        mode = 'poke',
        poke_interval = 5,
        timeout=30
    )