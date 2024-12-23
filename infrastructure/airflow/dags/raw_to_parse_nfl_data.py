from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import psycopg2
import json
import os

def extract_from_postgres():
    user = 'airflow_user'
    password = os.getenv("POSTGRES_PASSWORD")
    print(f"Attempting connection with user: {user}")

    pg_conn = psycopg2.connect(
        dbname="nfl_stats",
        user=user,
        password=password,
        host="infrastructure_postgres_1",
        port="5432"
    )
    
    # Connect to DuckDB
    duck = duckdb.connect('nfl_analytics.db')
    
    # Create materialized view from PostgreSQL JSON
    duck.sql("""
        CREATE OR REPLACE TABLE nfl_raw AS
        FROM read_json_auto('postgres:nfl_data', format='array')
    """)
    
    # Infer schema and create structured tables
    duck.sql("""
        CREATE OR REPLACE TABLE nfl_scores AS
        SELECT 
            data->>'gameId' as game_id,
            data->>'homeTeam'->>'score' as home_score,
            data->>'awayTeam'->>'score' as away_score,
            -- Add more fields based on JSON structure
        FROM nfl_raw
        WHERE data->>'type' = 'scores'
    """)
    
    duck.close()
    pg_conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 20),
    'retries': 0,
    'retry_delay': timedelta(days=1),
}

with DAG(
    'nfl_etl',
    default_args=default_args,
    description='NFL Data ETL Pipeline',
    schedule_interval=timedelta(hours=1),
) as dag:

    load_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_from_postgres
    )