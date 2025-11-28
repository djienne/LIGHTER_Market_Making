#!/usr/bin/env python3
"""
Fixed balance detection function based on test results.
"""

async def get_account_balances_fixed(api_client):
    """Get account balances - FIXED version based on test script findings."""
    import logging
    import os

    logger = logging.getLogger(__name__)
    ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX"))

    logger.debug("get_account_balances_fixed called")
    logger.info("Retrieving account balances...")

    try:
        import lighter
        account_api = lighter.AccountApi(api_client)
        logger.debug(f"Getting account info for account index: {ACCOUNT_INDEX}")

        account = await account_api.account(by="index", value=str(ACCOUNT_INDEX))
        logger.info("Successfully retrieved account data")

        # Check for accounts array (correct structure based on test results)
        if hasattr(account, 'accounts') and account.accounts:
            logger.info(f"Found accounts array with {len(account.accounts)} entries")
            account_data = account.accounts[0]  # Use first account

            # Look for direct balance fields (based on test script findings)
            # Priority order: available_balance, collateral, total_asset_value, cross_asset_value
            balance_fields = ['available_balance', 'collateral', 'total_asset_value', 'cross_asset_value']

            for field_name in balance_fields:
                if hasattr(account_data, field_name):
                    balance_value = getattr(account_data, field_name)
                    logger.debug(f"Found balance field {field_name}: {balance_value}")

                    try:
                        balance_float = float(balance_value)
                        if balance_float > 0:
                            logger.info(f"✅ Using {field_name} as available capital: ${balance_float}")
                            return balance_float
                        else:
                            logger.debug(f"Balance field {field_name} is zero: {balance_float}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not convert {field_name} value '{balance_value}' to float: {e}")

            logger.warning(f"No positive balance found in any of the fields: {balance_fields}")
        else:
            logger.warning("No accounts array found in response - unexpected API structure")
            logger.warning("Expected account.accounts[0] structure not found")

        logger.warning("No usable balance data found - returning 0")
        return 0

    except Exception as e:
        logger.error(f"Error getting account balances: {e}", exc_info=True)
        return 0


# Test the fixed function
if __name__ == "__main__":
    import asyncio
    import lighter
    import os
    from dotenv import load_dotenv

    load_dotenv()

    BASE_URL = "https://mainnet.zklighter.elliot.ai"

    async def test_fixed_balance():
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        balance = await get_account_balances_fixed(api_client)
        print(f"Fixed balance function returned: ${balance}")
        await api_client.close()

    asyncio.run(test_fixed_balance())