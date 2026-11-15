import requests
import json
from datetime import datetime, timedelta, date
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
    dag_id='dag_etl_trigger_CS_v07',
    default_args=default_args,
    start_date=datetime(2026, 11, 6),
    schedule_interval='@daily',
    catchup=False
)as dag:

    wait_for_csv = S3KeySensor(
        task_id='wait_for_player_stats_csv',
        bucket_name = 'trigger-for-cs',
        bucket_key = 'player_stats.csv',
        aws_conn_id = 'minio_conn',
        poke_interval = 20,
        timeout = 60 * 60 * 24,
        mode = 'reschedule'
    )
    
    @task
    def extract_data(**kwargs):
        s3hook = S3Hook(aws_conn_id = 'minio_conn')

        file_obj = s3hook.get_key(
            key = 'player_stats.csv',
            bucket_name='trigger-for-cs'
        )
        file_content = file_obj.get()['Body'].read()

        df = pd.read_csv(io.BytesIO(file_content))
        print(f'Number of lines read: {len(df)}')
        return df

    @task
    def transform_data(df):
        df['player_handle'] = df['player_handle'].astype(str).str.strip()

        df['real_name'] = df['real_name'].astype(str).str.strip()

        df['country_iso3'] = df['country_iso3'].astype(str).str.strip()

        df['primary_role'] = df['primary_role'].astype(str).str.strip()

        df['peak_hltv_rating'] = pd.to_numeric(df['peak_hltv_rating'], errors='coerce')

        df['career_kd_ratio'] = pd.to_numeric(df['career_kd_ratio'], errors='coerce')

        df['career_headshot_pct'] = pd.to_numeric(df['career_headshot_pct'], errors='coerce')

        df['years_active_start'] = pd.to_numeric(df['years_active_start'], errors='coerce')

        df['years_active_start'] = df['years_active_start'].astype('Int64')

        df['years_active_start'] = df['years_active_start'].replace({pd.NA: None})

        df['years_active_end'] = pd.to_numeric(df['years_active_end'], errors='coerce')
        
        df['years_active_end'] = df['years_active_end'].astype('Int64')

        df['years_active_end'] = df['years_active_end'].replace({pd.NA: None})

        df['years_active'] = pd.to_numeric(df['years_active'], errors='coerce')  
        
        df['years_active'] = df['years_active'].astype('Int64')

        df['estimated_career_kills'] = pd.to_numeric(df['estimated_career_kills'], errors='coerce')  
                
        df['estimated_career_kills'] = df['estimated_career_kills'].astype('Int64')

        df['estimated_maps_played'] = pd.to_numeric(df['estimated_maps_played'], errors='coerce')  
                        
        df['estimated_maps_played'] = df['estimated_maps_played'].astype('Int64')

        df['estimated_earnings_usd'] = pd.to_numeric(df['estimated_earnings_usd'], errors='coerce')  
                                
        df['estimated_earnings_usd'] = df['estimated_earnings_usd'].astype('Int64')

        df['major_tournament_wins'] = pd.to_numeric(df['major_tournament_wins'], errors='coerce')  
                                        
        df['major_tournament_wins'] = df['major_tournament_wins'].astype('Int64')

        is_outlier = [
            (df['peak_hltv_rating'] < 0).fillna(False).astype(bool),
            (df['career_kd_ratio'] < 0).fillna(False).astype(bool),
            (df['career_headshot_pct'] < 0).fillna(False).astype(bool),
            (df['years_active_end'] < df['years_active_start']).fillna(False).astype(bool),
            (df['years_active'] < 0).fillna(False).astype(bool),
            (df['estimated_career_kills'] < 0).fillna(False).astype(bool),
            (df['estimated_maps_played'] < 0).fillna(False).astype(bool),
            (df['estimated_earnings_usd'] < 0).fillna(False).astype(bool),
            (df['major_tournament_wins'] < 0).fillna(False).astype(bool) 
        ]

        reason = [
            'Negative HLTV Rating',
            'Negative K/D Ratio',
            'Negative Headshot Percentage',
            'End year is before Start year',
            'Negative Years Active',
            'Negative Kills',
            'Negative Maps Played',
            'Negative Earnings',
            'Negative Tournament Wins'
        ]

        df['rejection_reason'] = np.select(is_outlier, reason, default=None)

        df['creation_date'] = date.today()

        valid_data = df[df['rejection_reason'].isnull()].copy()

        valid_data.drop(columns=['rejection_reason'], inplace=True)

        invalid_data = df[df['rejection_reason'].notnull()].copy()

        return{
            'clean_csv' : valid_data.to_csv(index = False),
            'rejected_csv' : invalid_data.to_csv(index = False),
            'reject_count' : len(invalid_data)
        }

    
    @task
    def load_data_to_minio(transformed_data, **kwargs):
        target_date = kwargs['ds'].replace('-', '_')

        file_name = f'data_cs2_{target_date}.csv'
        file_name_reject = f'rejected_data_cs2_{target_date}.csv'

        print(f'Uploading files {file_name} and {file_name_reject}')

        s3hook = S3Hook(aws_conn_id = 'minio_conn')

        s3hook.load_string(
            string_data = transformed_data['clean_csv'],
            key = file_name,
            bucket_name = 'silver-cs',
            replace = True
        )

        if transformed_data['reject_count'] > 0:
            s3hook.load_string(
                string_data = transformed_data['rejected_csv'],
                key = file_name_reject,
                bucket_name = 'silver-cs',
                replace = True
            )

        print('Upload Succesfull')

    @task
    def load_data_to_ssms(transformed_data, **kwargs):
        print('Connecting with SSMS')

        mssql_hook = MsSqlHook(mssql_conn_id = 'my_mssql_connection')

        engine = mssql_hook.get_sqlalchemy_engine()

        delete_query=f"""
                delete from CS2_DWH.dbo.cs2_player_stats;
                delete from CS2_DWH.dbo.rejected_cs2_player_stats;
        """
        mssql_hook.run(delete_query)

        print('uploading Data')
        df_clean = pd.read_csv(io.StringIO(transformed_data['clean_csv']))
        df_clean.to_sql(
            name = 'cs2_player_stats',
            schema = 'dbo',
            con=engine,
            if_exists='append',
            index = False
        )
        if transformed_data.get('reject_count', 0) > 0:
            df_rejected = pd.read_csv(io.StringIO(transformed_data['rejected_csv']))
            df_rejected.to_sql(
                name = 'rejected_cs2_player_stats',
                schema = 'dbo',
                con=engine,
                if_exists='append',
                index = False
            )

    @task
    def cleanup_trigger_file():
        s3hook = S3Hook(aws_conn_id='minio_conn')
        s3hook.delete_objects(
            bucket='trigger-for-cs', 
            keys='player_stats.csv'
        )
        print("Deletted from minio to avoid further triggering")



    file = extract_data()
    transformed_data = transform_data(file)
    
    task_minio = load_data_to_minio(transformed_data)
    task_ssms = load_data_to_ssms(transformed_data)
    
    task_cleanup = cleanup_trigger_file()

    wait_for_csv >> file
    
    [task_minio, task_ssms] >> task_cleanup

    