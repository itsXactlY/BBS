import asyncio
import websockets
import json
import pandas as pd
import concurrent.futures
import requests
import pandas_ta as ta
import threading
import queue

# Create a dictionary to store DataFrames for different symbols
symbol_dfs = {}
symbol_locks = {}
print_queue = queue.Queue()

# Webhooks
webhook_url = "https://discord.com/api/webhooks/..."


def get_all_symbols():
    # Fetch all trading pairs on Binance
    response = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    data = response.json()
    symbols = [symbol['symbol'] for symbol in data['symbols']]

    # Filter Collatereal pairs
    usdt_symbols = [symbol for symbol in symbols if symbol.endswith('USDT')]
    btc_symbols = [symbol for symbol in symbols if symbol.endswith('BTC')]
    
    return usdt_symbols + btc_symbols

def initialize_dataframe(symbol, historical_data=None):
    columns = ["Symbol", "Time", "Open", "High", "Low", "Close", "Volume", "NumTrades", "IsClosed", "CloseTime", "QuoteAssetVolume", "TakerBuyBaseAssetVolume"]
    if historical_data is None:
        df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(historical_data, columns=columns)
    symbol_dfs[symbol] = df
    symbol_locks[symbol] = threading.Lock()

def fetch_historical_data(symbol):
    response = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=300")
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

async def handle_websocket(symbol):
    historical_data = fetch_historical_data(symbol)  # Fetch historical data
    initialize_dataframe(symbol, historical_data)  # Pass historical data when initializing DataFrame
    ema_initialized = False

    async with websockets.connect(f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_1m") as ws:
        async for message in ws:
            data = json.loads(message)
            if 'k' in data:
                kline = data['k']
                candle_data = {
                    "Symbol": data['s'],
                    "Time": kline['t'],
                    "Open": float(kline['o']),
                    "High": float(kline['h']),
                    "Low": float(kline['l']),
                    "Close": float(kline['c']),
                    "Volume": float(kline['v']),
                    "NumTrades": int(kline['n']),
                    "IsClosed": kline['x'],
                    "CloseTime": kline['T'],
                    "QuoteAssetVolume": float(kline['q']),
                    "TakerBuyBaseAssetVolume": float(kline['B'])
                }
                with symbol_locks[symbol]:
                    df = symbol_dfs[symbol]
                    df.loc[len(df)] = candle_data

                    if not ema_initialized:
                        df['EMA'] = df['Close']
                        ema_initialized = True
                    else:
                        df['EMA'] = ta.ema(df['Close'], length=10)

                # Check for support and resistance breakout
                breakout_info = check_support_resistance_breakout(df, window=20)
                symbol = df['Symbol'].iloc[0]
                print(f"Symbol: {symbol} | Current Support: {breakout_info['current_support']:.5f} | Current Resistance: {breakout_info['current_resistance']:.5f} | Buy Breakout Up: {breakout_info['buy_breakout_up']} | Sell Breakout Down: {breakout_info['sell_breakout_down']}")

                # Add the DataFrame to the print queue
                print_queue.put(df)


def check_support_resistance_breakout(df, window=200, webhook_url=webhook_url):
    try:
        if len(df) < window:
            print("Insufficient data for support/resistance calculation.")
            return None

        # Calculate support and resistance zones
        # Use rolling min and max functions on the Low and High columns, respectively
        df['support'] = df['Low'].rolling(window=window).min()
        df['resistance'] = df['High'].rolling(window=window).max()

        latest_data = df.iloc[-1]

        buy_breakout_up = False
        sell_breakout_down = False

        if latest_data['Close'] > latest_data['resistance']:
            buy_breakout_up = True
        elif latest_data['Close'] < latest_data['support']:
            sell_breakout_down = True

        breakout_info = {
            "current_support": latest_data['support'],
            "current_resistance": latest_data['resistance'],
            "buy_breakout_up": buy_breakout_up,
            "sell_breakout_down": sell_breakout_down,
        }

        if buy_breakout_up or sell_breakout_down:
            send_alert_to_webhook(df['Symbol'].iloc[0], breakout_info, webhook_url)
        
        return breakout_info
    except Exception as e:
        print(f"Error checking support/resistance breakout: {e}")
        return None


def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Closed")

def save_dataframe_to_csv(symbol, df):
    filename = f"binance_data/{symbol}_data.csv"
    df.to_csv(filename, index=False)
    
def send_alert_to_webhook(symbol, breakout_info, webhook_url):
    try:
        message = f"""
        
        Symbol: {symbol}
        Current Support: {breakout_info['current_support']:.10f}
        Current Resistance: {breakout_info['current_resistance']:.10f}
        Buy Breakout Up: {breakout_info['buy_breakout_up']}
        Sell Breakout Down: {breakout_info['sell_breakout_down']}
        """
        
        headers = {
            "Content-Type": "application/json"
        }

        chat_message = {
            "username": f"Breakout Alerts",
            "avatar_url": "https://static.wikia.nocookie.net/simpsons/images/4/4e/Seymour_Skinner_is_dead._FINALLY..png/revision/latest?cb=20130113115616",
            "content": message
        }
        response = requests.post(webhook_url, json=chat_message, headers=headers)
        print(response)
    except Exception as e:
        print(f"Error in send_alert_to_webhook: {e}")

def print_worker():
    while True:
        try:
            df = print_queue.get()
            save_dataframe_to_csv(df['Symbol'].iloc[0], df)  # Save the DataFrame to a CSV file
            print_queue.task_done()
        except Exception as e:
            print(f"Error in print_worker: {e}")

def start_websocket(symbol):
    asyncio.run(handle_websocket(symbol))

async def main():
    symbols = get_all_symbols()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, start_websocket, symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    debug_df = True
    if debug_df:
        # Start the print_worker thread for printing DataFrames
        print_thread = threading.Thread(target=print_worker)
        print_thread.daemon = True
        print_thread.start()

    asyncio.run(main())
