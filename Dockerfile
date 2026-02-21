FROM apache/airflow:2.7.0

USER airflow
RUN pip install apache-airflow-providers-amazon apache-airflow-providers-postgres pytest pytest-mock