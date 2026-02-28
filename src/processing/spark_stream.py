import os
import shutil
import numpy as np
import xgboost as xgb
import warnings
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, last, min, max, sum, current_timestamp, first
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Suppress convergence noise
warnings.simplefilter('ignore', ConvergenceWarning)

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
DB_URL = "jdbc:postgresql://localhost:8812/qdb"
DB_USER = "admin"
DB_PASSWORD = "quest"
CHECKPOINT_PATH = "data/checkpoints"

# Memory for models (5 minutes of history @ 1-second intervals)
MAX_WINDOW_SIZE = 300 
MIN_OBSERVATIONS = 120 
FORECAST_HORIZON = 30  # 30 seconds into the future

if os.path.exists(CHECKPOINT_PATH):
    shutil.rmtree(CHECKPOINT_PATH)

data_windows = {}

def calculate_volatility(prices):
    if len(prices) < 10: return 0.0
    log_returns = np.diff(np.log(prices))
    return float(np.std(log_returns) * np.sqrt(5896800))

def generate_ensemble_forecast(sym, prices):
    if len(prices) < MIN_OBSERVATIONS:
        return float(prices[-1]), float(prices[-1]), 0.0
    
    try:
        history = np.array(prices)
        # Smoothing with a 5-period moving average to handle 'flat' ticks
        smoothed = np.convolve(history, np.ones(5)/5, mode='valid')
        
        # 1. ARIMA(2,1,0) - Industry Standard for momentum
        # enforce_stationarity=False helps convergence on high-volatility moves
        arima_model = ARIMA(smoothed, order=(2, 1, 0), 
                            enforce_stationarity=False, 
                            enforce_invertibility=False)
        arima_result = arima_model.fit(method='innovations_mle')
        
        # Forecast 30 steps ahead
        arima_preds = arima_result.forecast(steps=FORECAST_HORIZON)
        arima_final = arima_preds[-1]
        
        # 2. XGBoost - Residual Error Correction
        fitted = arima_result.fittedvalues
        residuals = smoothed - fitted
        
        X = np.array([residuals[i:i+5] for i in range(len(residuals)-6)])
        y = residuals[6:]
        
        regressor = xgb.XGBRegressor(n_estimators=15, max_depth=3, objective='reg:squarederror')
        regressor.fit(X, y)
        
        xg_resid = regressor.predict(residuals[-5:].reshape(1, 5))[0]
        
        return float(arima_final + xg_resid), float(arima_final), float(xg_resid)
    except:
        return float(prices[-1]), float(prices[-1]), 0.0

# --- SPARK SETUP ---
spark = SparkSession.builder \
    .appName("NYSE-Ensemble-Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.5") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[2]") \
    .getOrCreate()

input_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("event_ts", LongType()) # Matches Finnhub JSON
])

write_schema = StructType([
    StructField("symbol", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("event_ts", TimestampType()),     
    StructField("processed_at", TimestampType()), 
    StructField("realized_volatility", DoubleType()),
    StructField("final_forecast", DoubleType()),
    StructField("arima_pred", DoubleType()),
    StructField("xgboost_resid", DoubleType())
])

def write_to_questdb(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows: return

    enriched_data = []
    for row in rows:
        r = row.asDict()
        sym = r['symbol']
        cp = float(r['close'])
        
        if sym not in data_windows: data_windows[sym] = []
        data_windows[sym].append(cp)
        if len(data_windows[sym]) > MAX_WINDOW_SIZE: data_windows[sym].pop(0)

        vol = round(calculate_volatility(data_windows[sym]), 6)
        f, a, x = generate_ensemble_forecast(sym, data_windows[sym])

        enriched_data.append({
            "symbol": str(sym),
            "open": float(r['open']), "high": float(r['high']),
            "low": float(r['low']), "close": cp,
            "volume": float(r['volume']),
            "event_ts": r['event_ts'],
            "processed_at": r['processed_at'], 
            "realized_volatility": vol,
            "final_forecast": float(f),
            "arima_pred": float(a),
            "xgboost_resid": float(x)
        })

    if enriched_data:
        spark.createDataFrame(enriched_data, schema=write_schema) \
            .write.format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "nyse_ohlcv") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("quoteIdentifiers", "true") \
            .mode("append") \
            .save()
        print(f"✅ Batch {batch_id}: {len(enriched_data)} rows forecasted and saved.")

# --- STREAMING LOGIC ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "nyse_raw") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.select(
    from_json(col("value").cast("string"), input_schema).alias("data")
).select(
    col("data.symbol"),
    col("data.price"),
    col("data.volume"),
    col("data.event_ts").alias("raw_ts")
) \
.withColumn("event_ts", (col("raw_ts") / 1000).cast("timestamp")) \
.withColumn("processed_at", current_timestamp())

df_aggregated = df_parsed \
    .withWatermark("event_ts", "10 seconds") \
    .groupBy(window(col("event_ts"), "1 second"), col("symbol")) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("volume"),
        max("event_ts").alias("event_ts"),
        max("processed_at").alias("processed_at")
    ).select(
        "symbol", "open", "high", "low", "close", "volume", "event_ts", "processed_at"
    )

print("🚀 Starting Stream...")
query = df_aggregated.writeStream \
    .foreachBatch(write_to_questdb) \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()