from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='questdb_dbt_refresh',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly', # Run every hour
    catchup=False,               # CRITICAL: Don't run missed tasks from when the laptop was closed
    max_active_runs=1            # Only allow one refresh at a time
) as dag:

    # We use the SQL from your dbt model but run it directly
    refresh_btc_prices = PostgresOperator(
        task_id='refresh_btc_prices',
        postgres_conn_id='questdb_default', # We will set this up in Airflow UI
        sql="""
            -- Drop if exists (QuestDB style)
            DROP TABLE IF EXISTS btcusdt_prices;
            
            -- Create table as select (The core of your dbt model)
            CREATE TABLE btcusdt_prices AS (
                SELECT 
                    timestamp,
                    symbol,
                    close,
                    volume,
                    final_forecast
                FROM nyse_ohlcv
                WHERE symbol = 'BINANCE:BTCUSDT'
            );
        """
    )