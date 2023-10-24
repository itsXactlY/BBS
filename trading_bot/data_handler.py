import pandas as pd
import threading
import queue
import pandas_ta as ta
import requests

# Create a dictionary to store DataFrames for different symbols
symbol_dfs = {}
symbol_locks = {}
print_queue = queue.Queue()

def initialize_dataframe(symbol, historical_data=None):
    columns = ["Symbol", "Time", "Open", "High", "Low", "Close", "Volume", "NumTrades", "IsClosed", "CloseTime", "QuoteAssetVolume", "TakerBuyBaseAssetVolume"]
    if historical_data is None:
        df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(historical_data, columns=columns)
    symbol_dfs[symbol] = df
    symbol_locks[symbol] = threading.Lock()

def get_all_symbols():
    print('Fetch all trading pairs on Binance')
    response = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    data = response.json()
    symbols = [symbol['symbol'] for symbol in data['symbols']]

    # Filter USDT-based pairs
    usdt_symbols = [symbol for symbol in symbols if symbol.endswith('USDT')]
    return usdt_symbols


def fetch_historical_data(symbol):
    response = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=50")
    data = response.json()
    historical_data = [[
        symbol,
        candle[0],
        float(candle[1]),
        float(candle[2]),
        float(candle[3]),
        float(candle[4]),
        float(candle[5]),
        int(candle[8]),
        candle[6] == "1",
        candle[6],
        float(candle[7]),
        float(candle[9])
    ] for candle in data]
    return historical_data

def save_dataframe_to_csv(symbol, df):
    try:
        filename = f"binance_data/{symbol}_data.csv"
        df.to_csv(filename, index=False)
    except Exception as e:
        print(f"Error in save_dataframe_to_csv: {e}")

def print_worker():
    print("Print worker started.")
    while True:
        try:
            df = print_queue.get()
            save_dataframe_to_csv(df['Symbol'].iloc[0], df)
            print_queue.task_done()
        except Exception as e:
            print(f"Error in print_worker: {e}")