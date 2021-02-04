from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from quality_checks_subdag import quality_checks_subdag
from helpers import SqlQueries




default_args = {
    "owner": "Nakul Bajaj",
    "start_date": datetime(2021, 2, 1),
    "end_date": datetime(2021,2,28),
    "retries": 3,
    "email": ["bajaj.nakul@gmail.com"],
    "retries_delay": timedelta(minutes=5),
    "depends_on_past":False,
    "email_on_failure": True,
    "email_on_retry": False
}

dag_name='udac_test_dag_v2'
schema_name=Variable.get("schema_name")
redshift_iam_arn=Variable.get("redshift_iam_arn")

with DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
          max_active_runs=1
        ) as dag:
    with open("/home/workspace/airflow/create_tables.sql","r") \
        as create_tables_file:
        create_tables_sql = create_tables_file.read()
        create_tables_sql = create_tables_sql.format(schema_name=schema_name)

    start_operator = DummyOperator(task_id='Begin_execution')
    
   
    create_schema_redshift = PostgresOperator(task_id="create_schema",
                                              sql=f"""CREATE SCHEMA IF 
                                                   NOT EXISTS {schema_name};
                                                   """,
                                              postgres_conn_id="redshift")
    
    create_tables_redshift = PostgresOperator(task_id="create_tables",
                                              sql=create_tables_sql,
                                              postgres_conn_id="redshift")
        
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        table_name=f"{schema_name}.staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        s3_region="us-west-2",
        json_paths="log_json_path.json",
        redshift_iam_arn=redshift_iam_arn,
        sla=timedelta(minutes=5)
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        table_name=f"{schema_name}.staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_region="us-west-2",
        redshift_iam_arn=redshift_iam_arn,
        sla=timedelta(minutes=5)
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id="redshift",
        sql=SqlQueries.songplay_table_insert.format(schema_name=schema_name),
        target_table="udacity.songplays",
        sla=timedelta(minutes=60)
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id="redshift",
        sql=SqlQueries.user_table_insert.format(schema_name=schema_name),
        target_table="udacity.users",
        sla=timedelta(minutes=30)
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id="redshift",
        sql=SqlQueries.song_table_insert.format(schema_name=schema_name),
        target_table="udacity.songs",
        sla=timedelta(minutes=30)
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id="redshift",
        sql=SqlQueries.artist_table_insert.format(schema_name=schema_name),
        target_table="udacity.artists",
        sla=timedelta(minutes=30)
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id="redshift",
        sql=SqlQueries.time_table_insert.format(schema_name=schema_name),
        target_table="udacity.time",
        sla=timedelta(minutes=30)
    )
    
    run_data_quality_checks = SubDagOperator(
        task_id="run_data_quality_checks",
        subdag=quality_checks_subdag(dag_name,
                                     "run_data_quality_checks",
                                     table_column_mapping={f"{schema_name}.users":"userid",
                                                           f"{schema_name}.artists":"artistid",
                                                           f"{schema_name}.songs":"songid",
                                                           f"{schema_name}.songplays":"playid",
                                                           f"{schema_name}.time":"start_time"},
                                     conn_id="redshift",
                                     args=default_args)
                                )

    end_operator = DummyOperator(task_id='Stop_execution')
    
    
    start_operator >> create_schema_redshift >> create_tables_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table,\
                             load_song_dimension_table,\
                             load_artist_dimension_table,\
                             load_time_dimension_table] >> run_data_quality_checks >> end_operator
