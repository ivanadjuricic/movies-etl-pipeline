from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add etl folder to Python path so Airflow can find our modules
sys.path.insert(0, '/opt/airflow/etl')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='movies_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: S3 -> PostgreSQL -> dbt -> Snowflake',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['movies', 'etl', 'portfolio']
) as dag:

    def extract_task():
        """Extract movies data from AWS S3."""
        from extract import extract_from_s3
        df = extract_from_s3()
        print(f"Extracted {len(df)} rows from S3.")
        # Save to temp file so next task can use it
        df.to_csv('/tmp/movies_raw.csv', index=False)
        return len(df)

    def transform_task():
        """Transform and clean the raw data."""
        import pandas as pd
        from transform import transform
        df_raw = pd.read_csv('/tmp/movies_raw.csv')
        df_movies, df_genres = transform(df_raw)
        # Save transformed data to temp files
        df_movies.to_csv('/tmp/movies_clean.csv', index=False)
        df_genres.to_csv('/tmp/genres_clean.csv', index=False)
        print(f"Transformed: {len(df_movies)} movies, {len(df_genres)} genre rows.")
        return len(df_movies)

    def load_postgres_task():
        """Load transformed data into PostgreSQL staging tables."""
        import pandas as pd
        from load_postgres import load_to_postgres
        df_movies = pd.read_csv('/tmp/movies_clean.csv')
        df_genres = pd.read_csv('/tmp/genres_clean.csv')
        load_to_postgres(df_movies, df_genres)
        print("Data loaded into PostgreSQL successfully.")

    def run_dbt_task():
        """Run dbt models to create analytics tables."""
        import subprocess
        result = subprocess.run(
            ['dbt', 'run', '--project-dir', '/opt/airflow/movies_dbt',
             '--profiles-dir', '/opt/airflow/movies_dbt'],
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        print("dbt models created successfully.")

    # Define tasks
    t1_extract = PythonOperator(
        task_id='extract_from_s3',
        python_callable=extract_task
    )

    t2_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task
    )

    t3_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_postgres_task
    )

    t4_run_dbt = PythonOperator(
        task_id='run_dbt_models',
        python_callable=run_dbt_task
    )

    # Pipeline flow
    t1_extract >> t2_transform >> t3_load_postgres >> t4_run_dbt