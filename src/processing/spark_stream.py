import os
import numpy as np
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from xgboost import XGBRegressor

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, first, last, min, max, sum, timestamp_seconds, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# 1. Environment Setup
os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@17"
os.environ["PATH"] = "/usr/local/opt/openjdk@17/bin:" + os.environ["PATH"]

# 2. Initialize Spark
spark = SparkSession.builder \
    .appName("QuestDB_Ensemble_Forecaster") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Global window for ML features
data_windows = {} 

# 3. Schema Definitions
kafka_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("timestamp", LongType())
])

# The "Truth" Schema for QuestDB
write_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("final_forecast", DoubleType(), True),
    StructField("arima_pred", DoubleType(), True),
    StructField("xgboost_resid", DoubleType(), True)
])

# 4. ML Logic
def generate_ensemble_forecast(symbol, prices):
    if len(prices) < 10:
        return float(prices[-1]), 0.0, 0.0
    try:
        history = [float(p) for p in prices]
        # ARIMA
        arima_model = ARIMA(history, order=(5,1,0))
        arima_fit = arima_model.fit()
        arima_pred = arima_fit.forecast(steps=1)[0]

        # XGBoost on Residuals
        residuals = arima_fit.resid
        X = np.array(range(len(residuals))).reshape(-1, 1)
        y = np.array(residuals)
        
        xgb_model = XGBRegressor(n_estimators=20, max_depth=3, learning_rate=0.1)
        xgb_model.fit(X, y)
        
        next_step = np.array([[len(residuals)]])
        xgboost_resid = xgb_model.predict(next_step)[0]

        return float(arima_pred + xgboost_resid), float(arima_pred), float(xgboost_resid)
    except Exception:
        return float(prices[-1]), 0.0, 0.0

# 5. Batch Processor (The Ghost Buster)
def write_to_questdb(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # 1. Collect data to break the streaming lineage
    rows = batch_df.collect()
    enriched_data = []

    for row in rows:
        sym = row['symbol']
        if sym not in data_windows:
            data_windows[sym] = []
        
        data_windows[sym].append(row['close'])
        if len(data_windows[sym]) > 100:
            data_windows[sym].pop(0)

        f, a, x = generate_ensemble_forecast(sym, data_windows[sym])
        
        print(f"DEBUG Batch {batch_id}: {sym} Forecast -> {f:.2f}")

        enriched_data.append({
            "symbol": str(sym),
            "open": float(row['open']),
            "high": float(row['high']),
            "low": float(row['low']),
            "close": float(row['close']),
            "volume": float(row['volume']),
            "timestamp": row['timestamp'],
            "final_forecast": float(f),
            "arima_pred": float(a),
            "xgboost_resid": float(x)
        })

    # 2. Re-create DF with the explicit 10-column schema
    final_batch_df = spark.createDataFrame(enriched_data, schema=write_schema)

    # 3. Force selection and write
    try:
        final_batch_df.select(
            "symbol", "open", "high", "low", "close", "volume", 
            "timestamp", "final_forecast", "arima_pred", "xgboost_resid"
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:8812/qdb") \
            .option("dbtable", "nyse_ohlcv") \
            .option("user", "admin") \
            .option("password", "quest") \
            .option("driver", "org.postgresql.Driver") \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()
        print(f"✅ SUCCESS: Batch {batch_id} persisted to QuestDB.")
    except Exception as e:
        print(f"❌ JDBC Persistence Error: {e}")

# 6. Stream Definition
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nyse_raw") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), kafka_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", timestamp_seconds(col("timestamp") / 1000))

# Pre-aggregate with placeholders to satisfy the stream structure
ohlcv_df = parsed_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("symbol")) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("volume")
    ) \
    .select(
        "symbol", "open", "high", "low", "close", "volume",
        col("window.start").alias("timestamp")
    )

# 7. Execution
query = ohlcv_df.writeStream \
    .foreachBatch(write_to_questdb) \
    .outputMode("update") \
    .start()

query.awaitTermination()