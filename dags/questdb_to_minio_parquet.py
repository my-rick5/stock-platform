from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import io
import pyarrow.parquet as pq

# Default arguments apply to all tasks in this DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                            # Try 2 more times if it fails
    'retry_delay': timedelta(minutes=5),     # Wait 5 minutes between attempts
}

def export_questdb_to_parquet(**kwargs):
    # 1. Connect to QuestDB
    conn = psycopg2.connect(
        host="questdb",
        port=8812,
        user="admin",
        password="quest",
        database="qdb"
    )
    
    # 2. Extract: Pull deduplicated data
    query = """
    SELECT * FROM btcusdt_prices 
    LATEST ON timestamp PARTITION BY timestamp
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Record the count for validation later
    db_count = len(df)
    kwargs['ti'].xcom_push(key='db_count', value=db_count)

    # 3. Transform: Convert to Parquet (in-memory)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
    parquet_buffer.seek(0)

    # 4. Load: Push to MinIO
    s3 = S3Hook(aws_conn_id='aws_default')
    filename = f"btc_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    s3_key = f"raw/btc_prices/{filename}"
    
    s3.load_file_obj(
        file_obj=parquet_buffer,
        key=s3_key,
        bucket_name="stock-platform-archive",
        replace=True
    )
    
    # Return the key so validation knows which file to check
    return s3_key

def validate_archive(**kwargs):
    ti = kwargs['ti']
    # 1. Pull metadata from the export task
    s3_key = ti.xcom_pull(task_ids='export_to_parquet')
    db_count = ti.xcom_pull(task_ids='export_to_parquet', key='db_count')
    bucket_name = "stock-platform-archive"
    
    # 2. Connect to MinIO and read the file once
    s3_hook = S3Hook(aws_conn_id='aws_default')
    file_content = s3_hook.get_key(s3_key, bucket_name).get()['Body'].read()
    
    # 3. Load into a Table (this allows us to check rows AND data)
    table = pq.read_table(io.BytesIO(file_content))
    df = table.to_pandas()
    parquet_rows = len(df)
    
    print(f"Validation: QuestDB Rows ({db_count}) vs Parquet Rows ({parquet_rows})")
    
    # CHECK 1: Row Count
    if db_count != parquet_rows:
        raise ValueError(f"Data Mismatch! QuestDB: {db_count}, Parquet: {parquet_rows}. ABORTING.")
    
    # CHECK 2: Null Prices (using your 5% threshold)
    null_count = df['price'].isnull().sum()
    if null_count > (parquet_rows * 0.05):
        raise ValueError(f"Too many NULL prices found: {null_count}. ABORTING.")

    # CHECK 3: Freshness (Data must not be older than 1 hour)
    # Ensure timestamp is datetime objects for the math
    last_ts = pd.to_datetime(df['timestamp']).max()
    # If your local time and QuestDB time are both UTC, this is perfect:
    time_diff = (datetime.utcnow() - last_ts.replace(tzinfo=None)).total_seconds()
    
    if time_diff > 3600:
        raise ValueError(f"Archive data is too old! Last record: {last_ts}. Diff: {time_diff}s")
    
    print("Validation Successful: Integrity and Freshness confirmed.")

with DAG(
    'archive_questdb_to_minio',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 2 * * *', 
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id='export_to_parquet',
        python_callable=export_questdb_to_parquet,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate_parquet_integrity',
        python_callable=validate_archive,
        provide_context=True
    )    

    cleanup_questdb = PostgresOperator(
        task_id='cleanup_questdb_hot_data',
        postgres_conn_id='questdb_default',
        sql="TRUNCATE TABLE btcusdt_prices;",
        autocommit=True
    )

    log_archive_success = PostgresOperator(
        task_id='log_archive_metadata',
        postgres_conn_id='questdb_default',
        sql="""
            INSERT INTO archive_audit_log (event_time, dag_run_id, rows_archived, s3_path, status)
            VALUES (
                now(), 
                '{{ run_id }}', 
                {{ ti.xcom_pull(task_ids='export_to_parquet', key='db_count') }}, 
                '{{ ti.xcom_pull(task_ids='export_to_parquet') }}', 
                'SUCCESS'
            );
        """,
        autocommit=True
    )

    # Updated Chain
    export_task >> validate_task >> cleanup_questdb >> log_archive_success