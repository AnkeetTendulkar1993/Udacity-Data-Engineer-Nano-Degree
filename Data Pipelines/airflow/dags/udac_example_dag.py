from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'retries' : 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          #schedule_interval='@daily'  
          schedule_interval='0 * * * *'        
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket="udacity-dend",
    #s3_key="log_data/",
    s3_key="log_data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift", 
    json_log_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    table_columns="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
    truncate_table=False,
    insert_table_sql=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    table_columns="userid, first_name, last_name, gender, level",
    truncate_table=True,
    insert_table_sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    table_columns="songid, title, artistid, year, duration",
    truncate_table=True,
    insert_table_sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    table_columns="artistid, name, location, lattitude, longitude",
    truncate_table=True,
    insert_table_sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    table_columns="start_time, hour, day, week, month, year, weekday",
    truncate_table=True,
    insert_table_sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    validation_queries=["SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 
                        "SELECT COUNT(*) FROM users WHERE userid IS NULL",
                        "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
                        "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 
                        "SELECT COUNT(*) FROM time WHERE start_time IS NULL"
                       ],
    expected_results=[0,0,0,0,0],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
