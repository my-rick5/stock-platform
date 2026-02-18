FROM apache/airflow:2.7.0

# Install dbt-postgres as the airflow user
USER airflow
RUN pip install --no-cache-dir dbt-postgres
