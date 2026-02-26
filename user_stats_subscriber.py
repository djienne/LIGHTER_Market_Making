import json
import logging
import asyncio
import argparse
import os
import websockets

# Configure logging
logging.basicConfig(level=logging.INFO)

# Global state variables
portfolio_value = None
available_balance = None

def on_user_stats_update(account_id, stats):
    """Callback function to update global state variables."""
    global portfolio_value, available_balance
    
    portfolio_value = stats.get('portfolio_value')
    available_balance = stats.get('available_balance')
    logging.info(f"Updated stats for account {account_id}: Portfolio Value={portfolio_value}, Available Balance={available_balance}")

async def subscribe_to_user_stats(account_id):
    """Connects to the websocket, subscribes to user_stats, and updates global state."""
    url = "wss://mainnet.zklighter.elliot.ai/stream"
    subscription_msg = {
        "type": "subscribe",
        "channel": f"user_stats/{account_id}"
    }
    
    while True:
        try:
            async with websockets.connect(url) as ws:
                logging.info(f"Connected to {url}")
                await ws.send(json.dumps(subscription_msg))
                logging.info(f"Subscribed to user_stats for account {account_id}")
                
                async for message in ws:
                    logging.info(f"Raw message received: {message}")
                    data = json.loads(message)
                    if data.get("type") == "update/user_stats" or data.get("type") == "subscribed/user_stats":
                        stats = data.get("stats", {})
                        on_user_stats_update(account_id, stats)
                    elif data.get("type") == "ping":
                        logging.debug("Received application-level ping, ignoring.")
                    else:
                        logging.debug(f"Received unhandled message: {data}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"Connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

async def main():
    """Main function to run the subscriber and print global state."""
    parser = argparse.ArgumentParser(description="Subscribe to Lighter user stats")
    parser.add_argument(
        "--account-id",
        type=int,
        default=int(os.getenv("ACCOUNT_INDEX", "107607")),
        help="Account ID to subscribe to (default: ACCOUNT_INDEX env var or 107607)",
    )
    args = parser.parse_args()
    account_id = args.account_id

    # Start the subscriber in the background
    subscriber_task = asyncio.create_task(subscribe_to_user_stats(account_id))

    # Periodically print the global state
    while True:
        print("\n--- Current Global State ---")
        print(f"Portfolio Value: {portfolio_value}")
        print(f"Available Balance: {available_balance}")
        print("--------------------------\n")
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())