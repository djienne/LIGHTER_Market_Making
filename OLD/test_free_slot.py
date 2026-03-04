"""Diagnostic: test if the free SendTx every 15s works at 0 quota."""

import asyncio
import os
import time
import lighter
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "0"))


async def main():
    print("Building signer client...")
    client = lighter.SignerClient(
        url=BASE_URL,
        account_index=ACCOUNT_INDEX,
        api_private_keys={API_KEY_INDEX: API_KEY_PRIVATE_KEY},
    )
    err = client.check_client()
    if err:
        print(f"CheckClient error: {err}")
        return

    print("Client OK. Hard-refreshing nonce...")
    client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
    await asyncio.sleep(2)

    # Wait to clear any firewall cooldowns
    wait_secs = 20
    print(f"Waiting {wait_secs}s to clear any firewall/cooldowns...")
    await asyncio.sleep(wait_secs)

    # Try IOC market order (the kind quota recovery would use)
    for attempt in range(1, 4):
        print(f"\n--- Attempt {attempt} at {time.strftime('%H:%M:%S')} ---")
        try:
            tx, response, err = await client.create_market_order(
                market_index=1,  # BTC
                client_order_index=time.time_ns() % (2**48 - 1),
                base_amount=20,  # 0.0002 BTC (raw: amount / tick)
                avg_execution_price=690000,  # ~69000 (raw: price / tick)
                is_ask=False,    # buy
            )
            if err:
                print(f"  Error response: {err}")
            else:
                quota = getattr(response, 'volume_quota_remaining', '?')
                print(f"  SUCCESS! Quota remaining: {quota}")
        except Exception as e:
            body = getattr(e, 'body', '')
            print(f"  Exception: {type(e).__name__}: {str(e)[:200]}")

        if attempt < 3:
            print(f"  Waiting 16s for next free slot...")
            await asyncio.sleep(16)

    await client.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
