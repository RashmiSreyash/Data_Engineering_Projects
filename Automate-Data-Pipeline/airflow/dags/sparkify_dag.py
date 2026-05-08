from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

# Default arguments (required)
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift',
    schedule_interval='@hourly'
)

# Start task
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)
stage_events = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    dag=dag
)
stage_songs = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path='auto',
    dag=dag
)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)
load_songs_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,  # Use song_table_insert
    truncate=True
)

load_users_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",  # Fixed typo: "userss" -> "users"
    sql_query=SqlQueries.user_table_insert,  # Use user_table_insert
    truncate=True
)

load_artists_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,  # Use artist_table_insert
    truncate=True
)

load_time_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,  # Use time_table_insert
    truncate=True
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        # Test 1: Check for NULLs in users
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},

        # Test 2: Check for NULLs in songs
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},

        # Test 3: Check for NULLs in songplays (using the correct column name)
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL", 'expected_result': 0},

        # Test 4: Verify the table is NOT empty (The fix you just asked for)
        {'check_sql': "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM songplays", 'expected_result': 1}
    ]
)
# End task
End_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)
start_operator >> [stage_events, stage_songs] >> load_songplays_table >> [load_artists_table, load_songs_table,
                                                                          load_time_table,
                                                                          load_users_table] >> run_quality_checks >> End_operator


