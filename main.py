import asyncio
import concurrent.futures
import threading
from trading_bot.data_handler import get_all_symbols, print_worker
from trading_bot.websocket_handler import start_websocket

async def main():
    symbols = get_all_symbols()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, start_websocket, symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

debug_df = True

if __name__ == "__main__":
    if debug_df:
        print_thread = threading.Thread(target=print_worker)
        print_thread.daemon = True
        print_thread.start()

    asyncio.run(main())
