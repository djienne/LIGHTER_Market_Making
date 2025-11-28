#!/usr/bin/env python3
"""
Debug script to investigate account balance retrieval issues.
Run this to see exactly what data is available from the Lighter API.
"""

import asyncio
import logging
import lighter
import os
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX"))


async def debug_account_data():
    """Comprehensive debug of account data structures."""

    logger.info("=== Lighter Account Balance Debug Tool ===")
    logger.info(f"Account Index: {ACCOUNT_INDEX}")
    logger.info(f"API Key Index: {API_KEY_INDEX}")
    logger.info(f"Base URL: {BASE_URL}")

    # Initialize API client
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)

    try:
        # 1. Get raw account data
        logger.info("\n=== RAW ACCOUNT DATA ===")
        account = await account_api.account(by="index", value=str(ACCOUNT_INDEX))

        print(f"Account response type: {type(account)}")
        print(f"Account response: {account}")

        if hasattr(account, '__dict__'):
            print(f"Account __dict__: {account.__dict__}")

        if hasattr(account, 'to_dict'):
            try:
                print(f"Account to_dict(): {account.to_dict()}")
            except Exception as e:
                print(f"to_dict() failed: {e}")

        # 2. Explore all account attributes
        logger.info("\n=== ACCOUNT ATTRIBUTES ===")
        account_attrs = [attr for attr in dir(account) if not attr.startswith('_')]
        print(f"Available attributes: {account_attrs}")

        for attr in account_attrs:
            try:
                value = getattr(account, attr)
                if not callable(value):
                    print(f"{attr}: {value} (type: {type(value)})")
            except Exception as e:
                print(f"{attr}: ERROR - {e}")

        # 3. Check if accounts array exists
        logger.info("\n=== ACCOUNTS ARRAY ===")
        if hasattr(account, 'accounts'):
            accounts = account.accounts
            print(f"Accounts array length: {len(accounts) if accounts else 0}")

            if accounts:
                for i, acc in enumerate(accounts):
                    print(f"\nAccount {i}:")
                    print(f"  Type: {type(acc)}")

                    acc_attrs = [attr for attr in dir(acc) if not attr.startswith('_')]
                    print(f"  Attributes: {acc_attrs}")

                    for attr in acc_attrs:
                        try:
                            value = getattr(acc, attr)
                            if not callable(value):
                                print(f"  {attr}: {value} (type: {type(value)})")
                        except Exception as e:
                            print(f"  {attr}: ERROR - {e}")
        else:
            print("No 'accounts' attribute found")

        # 4. Try authenticated call
        logger.info("\n=== AUTHENTICATED CALL ===")
        try:
            signer_client = lighter.SignerClient(
                url=BASE_URL,
                private_key=API_KEY_PRIVATE_KEY,
                account_index=ACCOUNT_INDEX,
                api_key_index=API_KEY_INDEX,
            )

            auth_token, auth_err = signer_client.create_auth_token_with_expiry()
            if auth_err:
                print(f"Auth token creation failed: {auth_err}")
            else:
                print("Auth token created successfully")

                auth_account = await account_api.account(
                    by="index",
                    value=str(ACCOUNT_INDEX),
                    auth=auth_token
                )

                print(f"Authenticated account response: {auth_account}")
                if hasattr(auth_account, '__dict__'):
                    print(f"Authenticated account __dict__: {auth_account.__dict__}")

            await signer_client.close()

        except Exception as auth_e:
            print(f"Authenticated call failed: {auth_e}")

        # 5. Try other account API methods
        logger.info("\n=== OTHER ACCOUNT METHODS ===")
        account_methods = [method for method in dir(account_api) if not method.startswith('_') and callable(getattr(account_api, method))]
        print(f"Available account API methods: {account_methods}")

        for method_name in account_methods:
            if method_name in ['account']:  # Skip the one we already tested
                continue

            try:
                method = getattr(account_api, method_name)
                print(f"\nTrying method: {method_name}")

                # Try with different parameter combinations
                if 'account_index' in method.__code__.co_varnames:
                    result = await method(account_index=ACCOUNT_INDEX)
                    print(f"  Result: {result}")
                    if hasattr(result, '__dict__'):
                        print(f"  Result dict: {result.__dict__}")
                else:
                    print(f"  Skipping {method_name} - no account_index parameter")

            except Exception as method_e:
                print(f"  {method_name} failed: {method_e}")

        # 6. Check market maker's position detection
        logger.info("\n=== POSITION DETECTION TEST ===")
        try:
            # Import the position checking function from market maker
            import sys
            sys.path.append('.')

            # Test the existing position detection
            pos_result = await account_api.account(by="index", value=str(ACCOUNT_INDEX))
            print(f"Position detection result: {pos_result}")

        except Exception as pos_e:
            print(f"Position detection failed: {pos_e}")

    except Exception as e:
        logger.error(f"Debug failed: {e}", exc_info=True)

    finally:
        await api_client.close()

    logger.info("\n=== DEBUG COMPLETE ===")
    logger.info("If no balance data was found, the account likely has no funds deposited.")
    logger.info("To fund the account, you need to deposit assets through the Lighter interface.")


if __name__ == "__main__":
    asyncio.run(debug_account_data())