import requests
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import pandas as pd
import io
import numpy as np


default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=3)
}

with DAG(
    dag_id='dag_etl_weather_olympus_v10',
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule_interval='@daily'
) as dag:
    @task
    def extract_weather_data(**kwards):
        target_date = kwards['ds']
        
        lat = 40.08                     #mitikas coordinates
        lon = 22.35

        url = f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max&start_date={target_date}&end_date={target_date}&timezone=auto'

        print(f'Calling API for the daily average temperature in Mitikas for {target_date}')

        response = requests.get(url)
        response.raise_for_status()

        raw_json_data = response.json()

        s3hook = S3Hook(aws_conn_id = 'minio_conn')

        file_name = f'weather_mitikas_{target_date}_raw_data.json'

        s3hook.load_string(
            string_data = json.dumps(raw_json_data),
            key = file_name,
            bucket_name = 'bronze-raw-weather',
            replace = True
        )

        print(f'Succesfully wrote {file_name} to Minio')

        return raw_json_data

    @task
    def transform_weather_data(raw_json_data):
        
        expected_collumns = {'time', 'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 'windspeed_10m_max'}

        if 'daily' not in raw_json_data or not expected_collumns.issubset(raw_json_data['daily'].keys()):
            raise ValueError('DQ error: Schema changed or missing columns')

        df = pd.DataFrame(raw_json_data['daily'])
        df['elevation_m'] = raw_json_data.get('elevation')

        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        df['temperature_2m_max'] = pd.to_numeric(df['temperature_2m_max'], errors='coerce')
        df['temperature_2m_min'] = pd.to_numeric(df['temperature_2m_min'], errors='coerce')
        df['precipitation_sum'] = pd.to_numeric(df['precipitation_sum'], errors='coerce')
        df['windspeed_10m_max'] = pd.to_numeric(df['windspeed_10m_max'], errors='coerce')
        df['elevation_m'] = pd.to_numeric(df['elevation_m'], errors='coerce')

        is_null = (
            df['time'].isnull() |
            df['temperature_2m_max'].isnull() |
            df['temperature_2m_min'].isnull()
        )

        is_outlier = (
            (df['temperature_2m_max'] > 50) | (df['temperature_2m_max'] < -50) |
            (df['temperature_2m_min'] > 50) | (df['temperature_2m_min'] < -50) |
            (df['temperature_2m_max'] < df['temperature_2m_min']) |
            (df['precipitation_sum'] < 0) |
            (df['windspeed_10m_max'] < 0) | (df['windspeed_10m_max'] > 300)
        )

        df['rejection_reason'] = np.select(
            condlist=[is_null, is_outlier],
            choicelist=['Null or Invalid Data Type', 'Extreme Outlier or Logic Error'],
            default='VALID'
        )

        df_clean = df[df['rejection_reason'] == 'VALID'].drop(columns='rejection_reason').copy()
        df_rejected = df[df['rejection_reason'] != 'VALID'].copy()

        df_clean.rename(columns={
            'time': 'weather_date',
            'temperature_2m_max': 'max_temp_c',
            'temperature_2m_min': 'min_temp_c',
            'precipitation_sum': 'precipitation_mm',
            'windspeed_10m_max': 'max_wind_kmh'
        }, inplace=True)

        df_rejected.rename(columns={
            'time': 'weather_date',
            'temperature_2m_max': 'max_temp_c',
            'temperature_2m_min': 'min_temp_c',
            'precipitation_sum': 'precipitation_mm',
            'windspeed_10m_max': 'max_wind_kmh'
        }, inplace=True)

        df_clean['location'] = 'Mytikas'
        df_rejected['location'] = 'Mytikas'
        
        return {
            'clean_csv': df_clean.to_csv(index=False),
            'rejected_csv': df_rejected.to_csv(index=False),
            'rejected_count': len(df_rejected)
        }

    @task
    def load_all_data_minio(transformed_data, **kwards):
        target_date = kwards['ds'].replace('-', '_')

        file_name_clean = f'mitikas_meteo_{target_date.replace('-', '_')}'
        file_name_rejected = f'rejected_mitikas_meteo_{target_date.replace('-', '_')}'

        print(f'Uploading files {file_name_clean} and {file_name_rejected}')

        s3hook = S3Hook(aws_conn_id = 'minio_conn')

        s3hook.load_string(
            string_data = transformed_data['clean_csv'],
            key = file_name_clean,
            bucket_name = 'silver-clean-weather',
            replace = True
        )

        if transformed_data['rejected_count'] > 0:
            s3hook.load_string(
                string_data = transformed_data['rejected_csv'],
                key = file_name_rejected,
                bucket_name = 'rejected-weather',
                replace = True
            )

        print('upload succesfull')

    @task
    def load_data_ssms(transformed_data, **kwargs):
        print('Connection with SSMS')

        execution_date = kwargs['ds']

        mssql_hook = MsSqlHook(mssql_conn_id = 'my_mssql_connection')

        engine = mssql_hook.get_sqlalchemy_engine()

        delete_query=f"""
                delete from silver_weather_olympus where weather_date = '{execution_date}'
                delete from rejected_weather_olympus where weather_date = '{execution_date}'
        """

        mssql_hook.run(delete_query)
        print('uploading Data')
        df_clean = pd.read_csv(io.StringIO(transformed_data['clean_csv']))
        df_clean.to_sql(
            name = 'silver_weather_olympus',
            schema = 'dbo',
            con=engine,
            if_exists='append',
            index = False
        )
        if transformed_data.get('rejected_count', 0) > 0:
            df_rejected = pd.read_csv(io.StringIO(transformed_data['rejected_csv']))
            df_rejected.to_sql(
                name = 'rejected_weather_olympus',
                schema = 'dbo',
                con=engine,
                if_exists='append',
                index = False
            )


    raw_data = extract_weather_data()
    transformed_data = transform_weather_data(raw_data)
    task1 = load_all_data_minio(transformed_data)
    task2 = load_data_ssms(transformed_data)