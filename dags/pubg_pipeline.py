from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default settings for the DAG
default_args = {
    'owner': 'Harsh Kumar Gupta',
    'start_date': datetime(2024, 1, 1),
    'retries': 0, # If it fails, don't retry immediately (good for debugging)
}

# Define the DAG
with DAG(
    dag_id='pubg_analytics_e2e',
    default_args=default_args,
    schedule_interval=None, # We will trigger it manually
    catchup=False
) as dag:

    # Task 1: Run the Python script to load JSON data to BigQuery
    extract_task = BashOperator(
        task_id='extract_pubg_data',
        bash_command='python /opt/airflow/scripts/load_data.py'
    )

    # Task 2: Run dbt models to transform data
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project'
    )

    # Task 3: Run dbt tests to ensure data quality
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project'
    )

    # Define the dependency chain
    # Python -> dbt run -> dbt test
    extract_task >> dbt_run >> dbt_test