import requests
import pandas as pd
from pathlib import Path

# Creating a folder to save the CSV files
saveDir = Path("data/raw")
saveDir.mkdir(parents=True, exist_ok=True)

def getCryptoData(coin):
    # CoinGecko API endpoint for historical market data
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
    # "365" to get all available data: last year price history
    params = {
        'vs_currency': 'usd',
        'days': '365'
    }
    print(f"Fetching data for {coin}...")
    response = requests.get(url, params=params)
    data = response.json()
    # If API fails or doesn't return price data, print it and stop
    if "prices" not in data:
        print(f"Error fetching {coin}, response was:")
        print(data)
        return
    # Prices come back as a list of [timestamp, price] pairs
    prices = data['prices']
    # Convert to DataFrame
    df = pd.DataFrame(prices, columns=['timestamp', 'price'])
    # Convert timestamp to datetime and store it as 'date'
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    # Keep only date and price, rename price column
    df = df[['date', 'price']].rename(columns={'price': f'{coin}Price'})
    # Create file path like: data/raw/bitcoinPrices.csv
    filePath = saveDir / f"{coin}Prices.csv"
    # Save to CSV
    df.to_csv(filePath, index=False)
    print(f"Data for {coin} saved to {filePath}")

# List of cryptocurrencies to fetch data for
coins = ["bitcoin", "ethereum"]

# Fetch and save data for each coin
for coin in coins:
    getCryptoData(coin)



    

