import sqlite3
import pandas as pd
import glob
import os
from pathlib import Path
from datetime import datetime

# File paths
DatabasePath = "CryptoData.db"
RawDataPath = "data/raw/*.csv"

# Create database connection
connect = sqlite3.connect(DatabasePath)
cursor = connect.cursor()

# Create SQL table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS crypto_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    coin TEXT,
    symbol TEXT,
    timestamp TEXT,
    price REAL,
    market_cap REAL,
    volume REAL
);
""")

# Grab all CSV files in data/raw
files = glob.glob(RawDataPath)

# Loop through each CSV
for file in files:
    print(f"Loading {file}...")

    df = pd.read_csv(file)

    # Make column names lowercase for easier matching
    df.columns = df.columns.str.lower()

    # Detect and format timestamp column
    time_col = [col for col in df.columns if "time" in col or "date" in col][0]
    df["timestamp"] = pd.to_datetime(df[time_col], errors="coerce")

    # Detect price column
    price_col = [col for col in df.columns if "price" in col or "close" in col][0]
    df["price"] = pd.to_numeric(df[price_col], errors="coerce")

    # Optional columns (market cap & volume)
    df["market_cap"] = pd.to_numeric(df.get("market_cap", 0), errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce")

    # Tag coin & symbol based on filename
    name = os.path.basename(file).lower()
    if "bitcoin" in name or "btc" in name:
        df["coin"], df["symbol"] = "Bitcoin", "BTC"
    elif "ethereum" in name or "eth" in name:
        df["coin"], df["symbol"] = "Ethereum", "ETH"
    else:
        df["coin"] = name.split(".")[0]
        df["symbol"] = None

    # Insert into SQL database
    df[["coin", "symbol", "timestamp", "price", "market_cap", "volume"]].to_sql(
        "crypto_prices",
        connect,
        if_exists="append",
        index=False
    )

# Commit and close
connect.commit()
connect.close()

print("All data loaded into SQLite database")
