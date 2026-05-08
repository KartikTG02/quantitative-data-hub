import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import time

# -- Page Configuration --
st.set_page_config(page_title="Quant Market Terminal", layout="wide", page_icon="📈")
st.title("📈 Real-Time Quantitative Market Terminal")
st.markdown("Visualising 1-minute Tumbling Windows from PySpark Structured Streaming")

# -- Database Connection --
@st.cache_resource
def init_connection():
    # Swapped to SQLAlchemy to prevent Pandas deprecation warnings
    return create_engine("postgresql://admin:password123@localhost:5432/market_data")

engine = init_connection()

# -- 1. Load Static Metadata (For the UI) --
@st.cache_data(ttl=3600) # Cache this for an hour so we don't spam the DB
def get_metadata():
    query = "SELECT ticker, company_name, sector FROM dim_stock_metadata ORDER BY sector, company_name"
    return pd.read_sql(query, engine)

meta_df = get_metadata()

# -- 2. Build the Sidebar UI (Always visible, even if market is closed) --
st.sidebar.header("Filter Engine")

available_sectors = meta_df['sector'].unique().tolist()
selected_sector = st.sidebar.selectbox("Select Sector", ["All"] + available_sectors)

# Filter available tickers based on the sector selection
if selected_sector != "All":
    filtered_meta = meta_df[meta_df['sector'] == selected_sector]
else:
    filtered_meta = meta_df

available_tickers = filtered_meta['ticker'].tolist()
selected_ticker = st.sidebar.selectbox("Select Ticker", available_tickers)

# Get the full company name for the title
selected_company_name = filtered_meta[filtered_meta['ticker'] == selected_ticker]['company_name'].iloc[0]

# -- 3. Fetch Live Gold Data --
def get_gold_data(ticker):
    # We now only pull the exact data we need for the selected ticker to save RAM
    query = f"""
        SELECT window_start, ticker, company_name, sector, open_price, high_price, low_price, close_price, total_volume, vwap
        FROM gold_market_candles
        WHERE ticker = '{ticker}'
        ORDER BY window_start ASC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        # If the table doesn't exist yet (e.g., market hasn't opened), return empty DataFrame
        return pd.DataFrame()

ticker_df = get_gold_data(selected_ticker)
ticker_df['window_start'] = pd.to_datetime(ticker_df['window_start']).dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
# -- 4. Render the Dashboard --
if not ticker_df.empty:
    # Top Row Metrics
    latest_data = ticker_df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    
    close_val = latest_data['close_price']
    vwap_val = latest_data['vwap']
    vol_val = latest_data['total_volume']
    
    close_display = f"₹{close_val:,.2f}" if pd.notna(close_val) else "N/A"
    vwap_display = f"₹{vwap_val:,.2f}" if pd.notna(vwap_val) else "N/A (0 Vol)"
    vol_display = f"{vol_val:,}" if pd.notna(vol_val) else "0"

    col1.metric("Latest Close", close_display)
    col2.metric("Latest VWAP", vwap_display)
    col3.metric("1-Min Volume", vol_display)
    col4.metric("Sector", latest_data['sector'])

    # Updated Title to use Full Company Name
    st.subheader(f"{selected_company_name} ({selected_ticker}) - Live Price vs VWAP")

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

    # Make the chart look more like a trading terminal
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        margin=dict(l=20, r=20, t=20, b=20)
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Gold Layer")
    st.dataframe(
        ticker_df.sort_values(by='window_start', ascending=False),
        use_container_width=True,
        hide_index=True
    )

else:
    # Graceful handling for market closed / no trades yet
    st.info(f"Waiting for live market data for {selected_company_name}...")
    st.caption("If the market is open, the first candlestick will appear after PySpark completes its first 1-minute window.")

st.sidebar.markdown("---")
st.sidebar.caption("Auto-refreshing every 10 seconds...")
time.sleep(10)
st.rerun()