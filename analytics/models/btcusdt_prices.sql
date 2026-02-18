{{ config(materialized='table') }}

SELECT 
    timestamp,
    symbol,
    side,
    price,
    amount,
    final_forecast
FROM {{ source('questdb', 'nyse_ohlcv') }}
WHERE symbol = 'BINANCE:BTCUSDT'
