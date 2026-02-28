#!/bin/bash

# --- CONFIGURATION ---
PROJECT_ROOT=$(pwd)
CHECKPOINT_DIR="$PROJECT_ROOT/data/checkpoints"
mkdir -p $CHECKPOINT_DIR

echo "--- 🚀 Launching Pure Bare Metal Stack ---"

# 1. Start Services
echo "Starting Kafka & QuestDB..."
brew services start kafka
brew services start questdb

# 2. Health Check
echo "Waiting for services..."
while ! nc -z localhost 9092; do sleep 1; done
while ! nc -z localhost 8812; do sleep 1; done

# 3. Start the Producer (Background)
echo "Starting Producer..."
python3 "$PROJECT_ROOT/src/ingest/producer.py" > producer.log 2>&1 &
PRODUCER_PID=$!

# 4. Start Spark Stream (Background)
echo "Starting Spark Stream..."
caffeinate -i nice -n 10 spark-submit \
  --master "local[2]" \
  --driver-memory 1g \
  --executor-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 \
  "$PROJECT_ROOT/src/processing/spark_stream.py" > spark.log 2>&1 &
SPARK_PID=$!

echo "------------------------------------------------"
echo "✅ SYSTEM ACTIVE"
echo "📝 Logs: tail -f spark.log"
echo "📊 QuestDB: http://localhost:9000"
echo "------------------------------------------------"