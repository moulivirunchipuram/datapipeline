from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFactOperator, StageToRedshiftOperator,LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, CreateTables

'''
This is the main dag that runs on an hourly schedule.
'''

# Defining constants 

S3_BUCKET = 'udacity-dend'
S3_SONG_KEY = 'song_data'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'dag'



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,

}

'''
define a dag that runs every hour to carryout the task of
loading data from S3 to redshift and transform data with Airflow
'''

dag = DAG('data_pipeline.dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )
#begin execution
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#A custom operator that loads events data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    region=REGION,
    truncate=False,
    data_format=f"JSON '{LOG_JSON_PATH}'"
)

#A custom operator that loads songs data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    region=REGION,
    truncate=True,
    data_format="JSON 'auto'"
    
)
#A custom operator that loads songplays fact data
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False
)
#A custom operator that loads user dimension data
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.user_table_insert,
    table='users',
    truncate=False
)
#A custom operator that loads songs dimension data
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.song_table_insert,
    table='songs',
    truncate=False
)
#A custom operator that loads artists dimension data
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.artist_table_insert,
    table='artists',
    truncate=False
)
#A custom operator that loads time dimension data
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.time_table_insert,
    table='time',
    truncate=False
)
#declare a list of tables that is passed on to DataQualityOperator as an arg
test_tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    postgres_conn_id=REDSHIFT_CONN_ID,
    tables=test_tables,
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#invoke the operators with appropriate dependency


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

#load the fact table data
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table

#after fact table data are loaded, run the quality operator
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

#end of dag execution
run_quality_checks >> end_operator




