from unittest.mock import MagicMock, patch
import pandas as pd
import io

@patch('airflow.providers.amazon.aws.hooks.s3.S3Hook')
def test_export_calls_s3_upload(mock_s3_hook):
    """
    Tests that the export function correctly attempts to upload to S3.
    """
    # Import your task function
    from dags.questdb_to_minio_parquet import export_questdb_to_parquet
    
    # Mock the context (ti) for XComs
    mock_ti = MagicMock()
    
    # We mock the psycopg2 connection so we don't need a real QuestDB
    with patch('psycopg2.connect'), patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.return_value = pd.DataFrame({'timestamp': [], 'close': []})
        
        # Execute the function
        export_questdb_to_parquet(ti=mock_ti)
    
    # Verify that load_file_obj was called once
    assert mock_s3_hook.return_value.load_file_obj.called
    
    # Verify it targeted the correct bucket
    args, kwargs = mock_s3_hook.return_value.load_file_obj.call_args
    assert kwargs['bucket_name'] == "stock-platform-archive"
