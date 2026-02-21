import pytest
from airflow.models import DagBag

def test_dag_loads_with_no_errors():
    """
    Test that the DAG can be parsed by Airflow without import errors.
    """
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    
    # 1. Check for syntax or import errors
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"
    
    # 2. Check that our specific DAG exists
    dag_id = "archive_questdb_to_minio"
    dag = dag_bag.get_dag(dag_id)
    assert dag is not None
    assert len(dag.tasks) == 4  # export, validate, cleanup, log
