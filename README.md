# Nifty 50 Quantitative Terminal

A real-time data engineering pipeline and visualization dashboard for the Indian stock market, specifically tracking Nifty 50 constituents. This project streams live per-second tick data from the ICICI Breeze API, processes it in flight, and serves it to a live-updating Streamlit dashboard.

## 🏗️ Architecture & Tech Stack

This project implements a decoupled, event-driven streaming architecture:

* **Data Ingestion:** Live Websocket connection to **ICICI Breeze API**.
* **Message Broker:** **Apache Kafka** (Producers push raw per-second ticks to topics).
* **Stream Processing:** **Apache Spark** (Structured Streaming) consumes Kafka topics, computing 1-minute tumbling windows to calculate Open, High, Low, Close, Volume (OHLCV), and Volume Weighted Average Price (VWAP).
* **Storage Layer:**
    * *Raw Data Lake:* **MinIO** (Object storage for raw JSON ticks).
    * *Aggregated Data:* **PostgreSQL** (Stores the 1-minute OHLCV/VWAP aggregates).
* **Presentation Layer:** **Streamlit** (Fetches aggregates from Postgres to render real-time candlestick charts and data tables).

## 📂 Project Structure

```text
quantitative_data_hub/
├── config/                  # YAML/JSON configs for DB, Kafka, and API keys
├── __init__.py
├── ingestion/           # ICICI Breeze API websocket & Kafka Producer
├── processing/          # PySpark structured streaming jobs
├── storage/             # Postgres and MinIO connection handlers
└── dashboard/           # Streamlit application UI
├── .env.example             # Template for environment variables
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Infrastructure orchestration (Kafka, Postgres, MinIO)
└── README.md
