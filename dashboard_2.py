import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import time

#-- Page Configuration --

st.set_page_config(page_title="Quant Market Terminal", layout="wide", page_icon="📈")
st.title("📈 Real-Time Quantitative Market Terminal")
st.markdown("Visualising 1-minute Tumbling Windows from PySpark Structured Streaming")

# -- Database Connection --

@st.cache_resource
def init_connection():
    return psycopg2.connect(
        dbname="market_data",
        user="admin",
        password="password123",
        host="localhost",
        port=5432
    )

# -- Data Fetching --

def get_gold_data():
    conn = init_connection()
    query = """
        SELECT WINDOW_START, TICKER, SECTOR, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, TOTAL_VOLUME, VWAP
        FROM gold_market_candles
        ORDER BY WINDOW_START ASC;
    """
    df = pd.read_sql(query, conn)
    return df

df = get_gold_data()

if df.empty:
    print("No data found in Postgres. Please ensure Pyspark and Producer are running!")
    st.stop()

st.sidebar.header("Filter Enginer")

available_sectors = df['sector'].unique().tolist()
selected_sector = st.sidebar.selectbox("Select Sector", ["All"] + available_sectors)

if selected_sector != "All":
    df = df[df['sector'] == selected_sector]

available_tickers = df['ticker'].unique().tolist()
selected_ticker = st.sidebar.selectbox("Select Ticker", available_tickers)

ticker_df = df[df["ticker"] == selected_ticker]

if not ticker_df.empty:

    latest_data = ticker_df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest Close", f"₹{latest_data['close_price']:,.2f}")
    col2.metric("Latest VWAP", f"₹{latest_data['vwap']:,.2f}")
    col3.metric("1-min Volume",f"{latest_data['total_volume']:,}")
    col4.metric("Sector", latest_data['sector'])

    st.subheader(f"{selected_ticker} - Live Price vs VWAP")

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=ticker_df['window_start'],
        open = ticker_df['open_price'],
        high = ticker_df['high_price'],
        low = ticker_df['low_price'],
        close=ticker_df['close_price'],
        name = 'Price Action'
    ))

    fig.add_trace(go.Scatter(
        x=ticker_df['window_start'],
        y=ticker_df['vwap'],
        mode='lines',
        name='VWAP (Volume Weighted Average Price)',
        line=dict(color='orange', width=2)
    ))

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Gold Layer (PostgreSQL)")
    st.dataframe(
        ticker_df.sort_values(by='window_start', ascending=False),
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("Waiting for market data for the selected filters...")

st.sidebar.markdown("---")
st.sidebar.caption("Auto-refreshing every 10 seconds...")
time.sleep(10)
st.rerun()