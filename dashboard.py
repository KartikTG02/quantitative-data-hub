import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time

st.set_page_config(page_title="NSE Data Hub", layout="wide", page_icon="📈")
st.title("⚡ Automated Market Intelligence Hub")

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="market_data",
        user="admin",
        password="password123",
        port="5432"
    )

conn = get_connection()

st.sidebar.header("Dashboard Controls")
tickers = ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK']
selected_ticker = st.sidebar.selectbox("Select Asset", tickers)
auto_refresh = st.sidebar.checkbox("Live Stream (1s Refresh)", value=True)

def load_data(ticker):
    query = f"""
        SELECT tick_time, price, volume
        FROM fact_market_ticks
        WHERE ticker = '{ticker}'
        ORDER BY tick_time DESC
        LIMIT 150
    """
    df = pd.read_sql(query,conn)
    return df.sort_values('tick_time')

df = load_data(selected_ticker)

if not df.empty:
    current_price = df['price'].iloc[-1]
    avg_price = df['price'].mean()
    delta = current_price - avg_price

    st.metric(
        label=f"{selected_ticker} Last Traded Price",
        value=f"₹{current_price:,.2f}",
        delta=f"{delta:,.2f} (vs SMA)"
    )

    fig = px.line(
        df,
        x='tick_time',
        y='price',
        title=f"Real-Time Tick Data: {selected_ticker}",
        template="plotly_dark"
    )
    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(title_text="Price (₹)")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(f"Waiting for data on {selected_ticker}...")

if auto_refresh:
    time.sleep(1)
    st.rerun()