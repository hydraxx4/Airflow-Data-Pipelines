FROM datetime import datetime, timedelta
import os
FROM airflow import DAG
FROM airflow.operators.dummy_operator import DummyOperator
FROM airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
FROM helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_run = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'trungt-udacity',
    s3_key = 'log_data',
    extra_params="FORMAT AS JSON 's3://trungt-udacity/log_json_path.json'",
    region = 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = 'trungt-udacity',
    s3_key = 'song_data',
    extra_param = "JSON 'auto' ",
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql = SqlQueries.
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    data_quality_checks = [
        {'data_check': 'SELECT COUNT(*) FROM public.artists WHERE name is NULL'},
        {'data_check': 'SELECT COUNT(*) FROM public.songsplay WHERE userid is NULL' },
        {'data_check': 'SELECT COUNT(*) FROM public.songs WHERE title is NULL'}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
