from sqlalchemy import create_engine, text

engine = create_engine("postgresql://admin:password123@localhost:5432/market_data")

with engine.connect() as conn:
    # Execute the raw SQL command to drop the table
    conn.execute(text("DROP TABLE IF EXISTS gold_market_candles;"))
    conn.commit()

print("Table successfully dropped")