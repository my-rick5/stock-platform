import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

st.set_page_config(page_title="Stock AI Tracker", layout="wide")

def get_data():
    conn = psycopg2.connect(
        host="questdb", # Docker service name
        port=8812,
        user="admin",
        password="quest",
        database="qdb"
    )
    # Pull latest data including your AI forecasts
    query = "SELECT timestamp, close, final_forecast FROM btcusdt_prices LIMIT -100"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("📈 BTC/USDT AI Forecast Dashboard")

try:
    data = get_data()
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Metric Row
    latest_price = data['close'].iloc[-1]
    latest_forecast = data['final_forecast'].iloc[-1]
    
    col1, col2 = st.columns(2)
    col1.metric("Latest Price", f"${latest_price:,.2f}")
    col2.metric("AI Forecast", f"${latest_forecast:,.2f}", 
                delta=f"{latest_forecast - latest_price:.2f}")

    # Main Chart
    st.subheader("Price vs. Forecast")
    st.line_chart(data.set_index('timestamp')[['close', 'final_forecast']])

    # Data Table
    if st.checkbox("Show raw data"):
        st.write(data.tail(10))

except Exception as e:
    st.error(f"Waiting for data from QuestDB... Error: {e}")

# Auto-refresh every 30 seconds
st.empty()
import time
time.sleep(30)
st.rerun()
