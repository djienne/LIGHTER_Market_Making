import asyncio
import logging
import lighter
import os
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

# The API_KEY_PRIVATE_KEY provided belongs to a dummy account registered on Testnet.
# It was generated using the setup_system.py script, and serves as an example.
# Alternatively, you can go to https://app.lighter.xyz/apikeys for mainnet api keys
BASE_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX"))
MARKET_ID = 48  # Assuming 48 is PAXG, 0 is ETH, 1 is BTC
SPREAD = 0.01  # 1%

BASE_AMOUNT = 0.047

# Global variable to store latest order book data from websocket
latest_order_book = None
order_book_received = asyncio.Event()


def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


def on_order_book_update(market_id, order_book):
    """Callback function for websocket order book updates."""
    global latest_order_book
    try:
        if int(market_id) == MARKET_ID:
            latest_order_book = order_book
            order_book_received.set()
            print(f"Order book updated for market {market_id}")
    except Exception as e:
        print(f"Error in order book callback: {e}")


def on_account_update(account_id, account):
    """Callback for account updates (not used)."""
    pass


async def main():
    global latest_order_book

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))

    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    err = client.check_client()
    if err is not None:
        print(f"CheckClient error: {trim_exception(err)}")
        return

    # Initialize websocket client for order book updates
    print(f"Starting websocket client for market {MARKET_ID}...")
    ws_client = lighter.WsClient(
        order_book_ids=[MARKET_ID],
        account_ids=[],
        on_order_book_update=on_order_book_update,
        on_account_update=on_account_update,
    )

    # Start websocket in background
    ws_task = asyncio.create_task(ws_client.run_async())

    try:
        # Wait for first order book update
        print("Waiting for order book data...")
        await asyncio.wait_for(order_book_received.wait(), timeout=30.0)

        if latest_order_book is None:
            print("No order book data received")
            return

        # Extract bid/ask from websocket data
        bids = latest_order_book.get('bids', [])
        asks = latest_order_book.get('asks', [])

        if not bids or not asks:
            print("Empty bids or asks in order book")
            return

        best_bid = float(bids[0]['price'])
        best_ask = float(asks[0]['price'])
        mid_price = (best_bid + best_ask) / 2
        buy_price = mid_price * (1 - SPREAD)

        print(f"Best Bid: {best_bid}, Best Ask: {best_ask}")
        print(f"Mid Price: {mid_price}")
        print(f"Placing order at: {buy_price}")

        # create order
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=123,
            base_amount=int(BASE_AMOUNT*10000),
            price=int(buy_price*100),
            is_ask=False,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=False
        )
        print(f"Create Order {tx=} {tx_hash=} {err=}")
        if err is not None:
            raise Exception(err)
        
        time.sleep(10)
        
        # cancel order
        tx, tx_hash, err = await client.cancel_order(
            market_index=MARKET_ID,
            order_index=123,
        )

    except asyncio.TimeoutError:
        print("Timeout waiting for order book data")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cleanup
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        await client.close()
        await api_client.close()


if __name__ == "__main__":
    asyncio.run(main())
