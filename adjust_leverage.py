
import logging
import lighter
import asyncio
import argparse
import os
from dotenv import load_dotenv

async def adjust_leverage(client: lighter.SignerClient, market_id: int, leverage: int, margin_mode_str: str, logger=None):
    """
    Adjusts the leverage for a given market.

    Args:
        client: An initialized lighter.SignerClient object.
        market_id: The ID of the market to adjust leverage for.
        leverage: The desired leverage.
        margin_mode_str: The margin mode ("cross" or "isolated").
        logger: Optional logger instance. Defaults to module-level logger.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    margin_mode = client.CROSS_MARGIN_MODE if margin_mode_str == "cross" else client.ISOLATED_MARGIN_MODE

    logger.info(f"Attempting to set leverage to {leverage} for market {market_id} with {margin_mode_str} margin.")

    try:
        tx, response, err = await client.update_leverage(market_id, margin_mode, leverage)
        if err:
            logger.error(f"Error updating leverage: {err}")
            return None, None, err
        else:
            logger.info("Leverage updated successfully.")
            logger.debug(f"Transaction: {tx}")
            logger.debug(f"Response: {response}")
            return tx, response, None
    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        return None, None, e

async def main():
    """Main function to run the script from the command line."""
    load_dotenv()

    parser = argparse.ArgumentParser(description="Adjust leverage for PAXG on Lighter API.")
    parser.add_argument("--leverage", type=int, default=1, help="The desired leverage (default: 1).")
    parser.add_argument("--margin-mode", type=str, default="cross", choices=["cross", "isolated"], help="The margin mode to use.")
    args = parser.parse_args()

    private_key = os.getenv("API_KEY_PRIVATE_KEY")
    account_index = os.getenv("ACCOUNT_INDEX")
    api_key_index = os.getenv("API_KEY_INDEX")
    base_url = "https://mainnet.zklighter.elliot.ai"

    if not all([private_key, account_index, api_key_index]):
        print("Error: Please ensure API_KEY_PRIVATE_KEY, ACCOUNT_INDEX, and API_KEY_INDEX are set in your .env file.")
        return

    market_id = 48  # Hardcoded for PAXG

    client = lighter.SignerClient(
        url=base_url,
        private_key=private_key,
        account_index=int(account_index),
        api_key_index=int(api_key_index),
    )

    err = client.check_client()
    if err is not None:
        print(f"CheckClient error: {err}")
        await client.close()
        return
    
    await adjust_leverage(client, market_id, args.leverage, args.margin_mode)
    
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
