from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries as sqlq

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
create_tables_qry=''
with open('/home/workspace/airflow/create_tables.sql', 'r') as file:
    create_tables_qry = file.read().replace('\n', '')
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = LoadDimensionOperator(
    task_id='Create_tables',
    dag=dag,
    conn_id='postgres_default',
    queries_list=create_tables_qry
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id='postgres_default',
    s3_src_path='s3://udacity-dend/log_data',
    table='staging_events',
    iam_role='',
    json_type='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    extra_params="timeformat as 'epochmillisecs'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id='postgres_default',
    s3_src_path='s3://udacity-dend/song_data',
    table='staging_songs',
    iam_role='',
    json_type='auto',
    region='us-west-2',
    extra_params=''
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id='postgres_default',
    query=sqlq.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id='postgres_default',
    queries_list=sqlq.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id='postgres_default',
    query=sqlq.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id='postgres_default',
    query=sqlq.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id='postgres_default',
    query=sqlq.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='postgres_default'
    
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables >> [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >>end_operator
