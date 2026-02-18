import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from statsmodels.tsa.arima.model import ARIMA
from xgboost import XGBRegressor

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "nyse_raw"  # Matched to your Producer
DB_URL = "jdbc:postgresql://localhost:8812/qdb"
DB_USER = "admin"
DB_PASSWORD = "quest"

# Windowed storage
data_windows = {}
vol_windows = {}

# --- SCHEMAS ---
# Updated to match Finnhub Websocket fields
read_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),   # Websocket sends 'price'
    StructField("volume", DoubleType(), True),
    StructField("timestamp", DoubleType(), True) # Read as Double to fix ms issue
])

write_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("realized_volatility", DoubleType(), True),
    StructField("final_forecast", DoubleType(), True),
    StructField("arima_pred", DoubleType(), True),
    StructField("xgboost_resid", DoubleType(), True)
])

# --- ML LOGIC ---
def calculate_volatility(prices):
    if len(prices) < 2: return 0.0
    returns = np.diff(np.log(prices))
    return float(np.std(returns))

def generate_ensemble_forecast(symbol, prices, vols):
    if len(prices) < 10 or len(vols) < 10:
        return float(prices[-1]), 0.0, 0.0
    try:
        history = [float(p) for p in prices]
        current_vols = [float(v) for v in vols]
        
        arima_model = ARIMA(history, order=(5,1,0))
        arima_fit = arima_model.fit()
        arima_pred = arima_fit.forecast(steps=1)[0]

        residuals = arima_fit.resid
        X = np.column_stack([np.array(range(len(residuals))), np.array(current_vols)])
        y = np.array(residuals)
        
        xgb_model = XGBRegressor(n_estimators=20, max_depth=3, learning_rate=0.1)
        xgb_model.fit(X, y)
        
        next_features = np.array([[len(residuals), current_vols[-1]]])
        xgboost_resid = xgb_model.predict(next_features)[0]

        return float(arima_pred + xgboost_resid), float(arima_pred), float(xgboost_resid)
    except:
        return float(prices[-1]), 0.0, 0.0

# --- BATCH PROCESSING ---
def write_to_questdb(batch_df, batch_id):
    rows = batch_df.collect()
    enriched_data = []

    for row in rows:
        sym = row['symbol']
        # Map websocket 'price' to our 'close' logic
        current_price = float(row['price'])
        
        if sym not in data_windows: data_windows[sym] = []
        data_windows[sym].append(current_price)
        if len(data_windows[sym]) > 100: data_windows[sym].pop(0)

        vol = round(calculate_volatility(data_windows[sym]), 6)
        if sym not in vol_windows: vol_windows[sym] = []
        vol_windows[sym].append(vol)
        if len(vol_windows[sym]) > 100: vol_windows[sym].pop(0)

        f, a, x = generate_ensemble_forecast(sym, data_windows[sym], vol_windows[sym])

        enriched_data.append({
            "symbol": str(sym),
            "open": current_price, # Simplified: using price as OHLC for streaming ticks
            "high": current_price,
            "low": current_price,
            "close": current_price,
            "volume": float(row['volume']),
            "timestamp": row['timestamp'],
            "realized_volatility": vol,
            "final_forecast": float(f),
            "arima_pred": float(a),
            "xgboost_resid": float(x)
        })

    if enriched_data:
        spark_batch_df = spark.createDataFrame(enriched_data, schema=write_schema)
        spark_batch_df.write.format("jdbc") \
            .option("url", DB_URL).option("dbtable", "nyse_ohlcv") \
            .option("user", DB_USER).option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append").save()
        print(f"✅ SUCCESS: Batch {batch_id} persisted to QuestDB.")

# --- SPARK ENGINE ---
spark = SparkSession.builder \
    .appName("StockVolatilityEnsemble") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Fix the timestamp: Finnhub sends ms, Spark wants seconds
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), read_schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

query = parsed_df.writeStream \
    .foreachBatch(write_to_questdb) \
    .start()

print(f"🚀 Streaming from {KAFKA_TOPIC} to QuestDB...")
query.awaitTermination()