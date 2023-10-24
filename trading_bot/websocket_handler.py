import asyncio
import websockets
import json
import queue
from trading_bot.data_handler import initialize_dataframe, fetch_historical_data, symbol_locks, symbol_dfs
import pandas_ta as ta

print_queue = queue.Queue()

def start_websocket(symbol):
    asyncio.run(handle_websocket(symbol))

def check_support_resistance_breakout(df, window=20):
    try:
        if len(df) < window:
            print("Insufficient data for support/resistance calculation.")
            return None

        current_support = df['Low'].rolling(window=window).min().iloc[-1]
        current_resistance = df['High'].rolling(window=window).max().iloc[-1]

        latest_data = df.iloc[-1]

        buy_breakout_up = False
        sell_breakout_down = False

        if latest_data['Close'] > current_resistance:
            buy_breakout_up = True
        elif latest_data['Close'] < current_support:
            sell_breakout_down = True

        return {
            "current_support": current_support,
            "current_resistance": current_resistance,
            "buy_breakout_up": buy_breakout_up,
            "sell_breakout_down": sell_breakout_down,
        }
    except Exception as e:
        print(f"Error checking support/resistance breakout: {e}")
        return None

async def handle_websocket(symbol):
    historical_data = fetch_historical_data(symbol)
    initialize_dataframe(symbol, historical_data)
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

                breakout_info = check_support_resistance_breakout(df, window=20)
                symbol = df['Symbol'].iloc[0]
                print(f"Symbol: {symbol} | Current Support: {breakout_info['current_support']:.5f} | Current Resistance: {breakout_info['current_resistance']:.5f} | Buy Breakout Up: {breakout_info['buy_breakout_up']} | Sell Breakout Down: {breakout_info['sell_breakout_down']}")

                print_queue.put(df)
