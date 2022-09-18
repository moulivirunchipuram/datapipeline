import logging

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from helpers import CreateTables

REDSHIFT_CONN_ID='redshift'

dag = DAG(
    'create_tables',
    start_date=datetime.datetime.now()
)

drop_artists_table = PostgresOperator(
    task_id="drop_artists_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.artists',
)
drop_songplays_table = PostgresOperator(
    task_id="drop_songplays_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.songplays',
)
drop_songs_table = PostgresOperator(
    task_id="drop_songs_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.songs',
)
drop_users_table = PostgresOperator(
    task_id="drop_users_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.users',
)
drop_time_table = PostgresOperator(
    task_id="drop_time_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.time',
)
drop_staging_events_table = PostgresOperator(
    task_id="drop_staging_events_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.staging_events',
)
drop_staging_songs_table = PostgresOperator(
    task_id="drop_staging_songs_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='DROP TABLE IF EXISTS public.staging_songs',
)

create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_ARTISTS_TABLE_SQL,
)
create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_SONGPLAYS_TABLE_SQL,
)
create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_SONGS_TABLE_SQL,
)
create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_STAGING_EVENTS_TABLE_SQL,
)
create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_STAGING_SONGS_TABLE_SQL,
)
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_TIME_TABLE_SQL,
)
create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=CreateTables.CREATE_USERS_TABLE_SQL,
)

drop_artists_table >> create_artists_table
drop_songplays_table >> create_songplays_table
drop_songs_table >> create_songs_table
drop_users_table >> create_users_table
drop_time_table >> create_time_table
drop_staging_events_table >> create_staging_events_table
drop_staging_songs_table >> create_staging_songs_table






