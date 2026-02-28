#!/bin/bash

echo "--- 🛑 Cleaning Up ---"

# 1. Kill Python/Spark Processes
echo "Killing background tasks..."
pkill -f producer.py
pkill -f spark_stream.py
pkill -f SparkSubmit

# 2. Stop Services
echo "Stopping Brew services..."
brew services stop kafka
brew services stop zookeeper
brew services stop questdb

# 3. Wipe Checkpoints (Optional - use if you want a fresh start)
# rm -rf data/checkpoints

echo "--- ✅ MacBook Air is now at rest. ---"