#!/bin/bash
set -e

echo "🚀 Starting Stock Platform Deployment..."

# 1. Pull latest images and build custom Airflow image (with pyarrow/pytest)
docker compose build

# 2. Start Services in the background
docker compose up -d

# 3. Health Check: Wait for QuestDB to be ready (Port 8812)
echo "⏳ Waiting for QuestDB to initialize..."
until nc -z localhost 8812; do
  sleep 2
done

# 4. Run Pytest before finalizing
echo "🧪 Running final integration tests..."
docker compose run --rm --entrypoint pytest airflow-webserver tests/

# 5. Initialize Airflow Connections (S3 and QuestDB)
# This prevents manual setup in the UI after every deployment
echo "🔗 Configuring Airflow connections..."
# Delete if exists (the || true prevents the script from stopping if it doesn't exist)
docker compose exec airflow-webserver airflow connections delete 'questdb_default' || true

# Add the connection
docker compose exec airflow-webserver airflow connections add 'questdb_default' \
    --conn-type 'postgres' \
    --conn-host 'questdb' \
    --conn-login 'admin' \
    --conn-password 'quest' \
    --conn-port '8812'

echo "✅ Deployment Complete! Airflow is live at http://localhost:8080"
