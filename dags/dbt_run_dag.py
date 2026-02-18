from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'questdb_dbt_refresh',
    default_args=default_args,
    description='Triggers dbt models for QuestDB',
    schedule_interval=timedelta(hours=1), # Run every hour
    catchup=False,
) as dag:

    # Task to run dbt
    # Note: We use the absolute path to your analytics folder
    run_dbt = BashOperator(
        task_id='dbt_run',
        # We explicitly call the dbt binary from the local user bin
        bash_command='cd /opt/airflow/analytics && /home/airflow/.local/bin/dbt run --profiles-dir .',
    )