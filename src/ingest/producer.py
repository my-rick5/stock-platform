import websocket
import json
import os
import time
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from pathlib import Path

# 1. Get the directory where THIS script lives
current_dir = Path(__file__).resolve().parent

# 2. Navigate up to the project root (e.g., from src/ingest/ to stock-platform/)
# Adjust the number of .parent calls based on how deep the script is
project_root = current_dir.parent.parent 

# 3. Load the specific absolute path
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

FINNHUB_TOKEN = os.getenv('FINNHUB_TOKEN')
FMP_KEY = os.getenv('FMP_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = "nyse_raw"

# Kafka Producer Configuration
p_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(p_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent: {msg.topic()} [{msg.partition()}]")

# 2. Smart Ticker Selection (FMP)
def get_smart_tickers():
    url = f"https://financialmodelingprep.com/stable/most-actives?apikey={FMP_KEY}"
    try:
        print("📡 Contacting FMP for active stocks...")
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        # Returns 30 symbols to keep Laptop RAM in the 'Green Zone'
        return [s['symbol'] for s in r.json()][:30]
    except Exception as e:
        print(f"⚠️ FMP Fetch failed ({e}). Using 'Green Zone' fallback list.")
        return ["AAPL", "NVDA", "TSLA", "AMD", "MSFT", "GOOGL", "META"]

TICKERS = get_smart_tickers()

# 3. WebSocket Logic
def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        for trade in data['data']:
            payload = {
                "symbol": trade['s'],
                "price": float(trade['p']),
                "volume": float(trade['v']),
                "event_ts": int(trade['t'])
            }
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)
        
        # Poll Kafka to trigger delivery reports in terminal
        producer.poll(0)

def on_error(ws, error):
    print(f"‼️ WS Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WS Closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print(f"🚀 Connected! Subscribing to: {TICKERS}")
    for symbol in TICKERS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        time.sleep(0.05) # Polite rate limiting

if __name__ == "__main__":
    if not FINNHUB_TOKEN:
        print("❌ FINNHUB_TOKEN missing from .env")
    else:
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = on_open
        # ping_interval prevents "None - None" silent disconnects
        ws.run_forever(ping_interval=10, ping_timeout=5)