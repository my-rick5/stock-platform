import websocket
import json
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("FINNHUB_API_KEY") 


# Kafka Setup
p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def on_message(ws, message):
    msg = json.loads(message)
    if msg['type'] == 'trade':
        for trade in msg['data']:
            print(f"🔥 Trade Received: {trade['s']} @ {trade['p']}") # <--- ADD THIS
            payload = {
                'symbol': trade['s'],
                'price': trade['p'],
                'volume': trade['v'],
                'timestamp': trade['t']
            }
            p.produce('nyse_raw', json.dumps(payload).encode('utf-8'), callback=delivery_report)
    p.poll(0)

def on_open(ws):
    print("🚀 Connection Opened! Subscribing...") # <--- ADD THIS
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=" + api_key,
                          on_message=on_message, on_open=on_open)
ws.run_forever()
