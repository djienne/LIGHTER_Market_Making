#!/usr/bin/env python3
"""
Simple test script to check available capital for trading.
This script will try multiple methods to find account balance/capital.
"""

import asyncio
import logging
import lighter
import os
import json
import pprint
from dotenv import load_dotenv

load_dotenv()

# Setup comprehensive logging - show EVERYTHING
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)

# Enable debug logging for ALL modules
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger('lighter').setLevel(logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.DEBUG)
logging.getLogger('asyncio').setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

# Pretty printer for complex objects
pp = pprint.PrettyPrinter(indent=2, width=120)

# Configuration
BASE_URL = "https://mainnet.zklighter.elliot.ai"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX"))


async def test_available_capital():
    """Test different methods to get available trading capital."""

    print("="*60)
    print("LIGHTER DEX - AVAILABLE CAPITAL TEST")
    print("="*60)
    print(f"Account Index: {ACCOUNT_INDEX}")
    print(f"API Key Index: {API_KEY_INDEX}")
    print(f"Base URL: {BASE_URL}")
    print("="*60)

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))

    try:
        # Method 1: Basic Account API
        print("\n[METHOD 1] Basic Account API")
        print("-" * 40)
        account_api = lighter.AccountApi(api_client)
        account_response = await account_api.account(by="index", value=str(ACCOUNT_INDEX))

        print(f"Response type: {type(account_response)}")
        print(f"Response repr: {repr(account_response)}")
        print(f"Response str: {str(account_response)}")

        # Try multiple serialization methods
        print("\n--- DETAILED RESPONSE ANALYSIS ---")

        if hasattr(account_response, '__dict__'):
            print("Response __dict__:")
            pp.pprint(account_response.__dict__)

        if hasattr(account_response, 'to_dict'):
            try:
                to_dict_result = account_response.to_dict()
                print("Response to_dict():")
                pp.pprint(to_dict_result)
            except Exception as e:
                print(f"to_dict() failed: {e}")

        # Try JSON serialization
        try:
            json_str = json.dumps(account_response, default=str, indent=2)
            print("Response as JSON:")
            print(json_str)
        except Exception as e:
            print(f"JSON serialization failed: {e}")

        # Show all attributes and their values
        print("\n--- ALL ATTRIBUTES ---")
        for attr in dir(account_response):
            if not attr.startswith('_'):
                try:
                    value = getattr(account_response, attr)
                    if not callable(value):
                        print(f"{attr}: {value} (type: {type(value)})")
                    else:
                        print(f"{attr}: <method>")
                except Exception as e:
                    print(f"{attr}: ERROR - {e}")

        # Method 2: Authenticated Account API
        print("\n[METHOD 2] Authenticated Account API")
        print("-" * 40)

        signer_client = lighter.SignerClient(
            url=BASE_URL,
            private_key=API_KEY_PRIVATE_KEY,
            account_index=ACCOUNT_INDEX,
            api_key_index=API_KEY_INDEX,
        )

        auth_token, auth_err = signer_client.create_auth_token_with_expiry()
        if auth_err:
            print(f"❌ Auth token failed: {auth_err}")
        else:
            print("✅ Auth token created")

            try:
                auth_account = await account_api.account(
                    by="index",
                    value=str(ACCOUNT_INDEX),
                    auth=auth_token
                )
                print(f"Auth response type: {type(auth_account)}")
                print(f"Auth response repr: {repr(auth_account)}")
                print(f"Auth response str: {str(auth_account)}")

                print("\n--- AUTHENTICATED RESPONSE ANALYSIS ---")
                if hasattr(auth_account, '__dict__'):
                    print("Auth response __dict__:")
                    pp.pprint(auth_account.__dict__)

                if hasattr(auth_account, 'to_dict'):
                    try:
                        auth_to_dict = auth_account.to_dict()
                        print("Auth response to_dict():")
                        pp.pprint(auth_to_dict)
                    except Exception as e:
                        print(f"Auth to_dict() failed: {e}")

                # Show all auth response attributes
                print("\n--- ALL AUTH ATTRIBUTES ---")
                for attr in dir(auth_account):
                    if not attr.startswith('_'):
                        try:
                            value = getattr(auth_account, attr)
                            if not callable(value):
                                print(f"{attr}: {value} (type: {type(value)})")
                        except Exception as e:
                            print(f"{attr}: ERROR - {e}")
            except Exception as e:
                print(f"❌ Authenticated call failed: {e}")

        # Method 3: Account Positions (may contain margin info)
        print("\n[METHOD 3] Account Positions")
        print("-" * 40)

        try:
            positions = await account_api.account_positions(account_index=ACCOUNT_INDEX)
            print(f"Positions type: {type(positions)}")
            print(f"Positions repr: {repr(positions)}")

            print("\n--- POSITIONS ANALYSIS ---")
            if hasattr(positions, '__dict__'):
                print("Positions __dict__:")
                pp.pprint(positions.__dict__)

            if hasattr(positions, 'to_dict'):
                try:
                    pos_dict = positions.to_dict()
                    print("Positions to_dict():")
                    pp.pprint(pos_dict)
                except Exception as e:
                    print(f"Positions to_dict() failed: {e}")

            # Detailed positions analysis
            print("\n--- POSITIONS ATTRIBUTES ---")
            for attr in dir(positions):
                if not attr.startswith('_'):
                    try:
                        value = getattr(positions, attr)
                        if not callable(value):
                            print(f"{attr}: {value} (type: {type(value)})")
                    except Exception as e:
                        print(f"{attr}: ERROR - {e}")
        except Exception as e:
            print(f"❌ Positions call failed: {e}")

        # Method 4: Account Active Orders (may show available margin)
        print("\n[METHOD 4] Account Active Orders")
        print("-" * 40)

        try:
            order_api = lighter.OrderApi(api_client)
            active_orders = await order_api.account_active_orders(
                account_index=ACCOUNT_INDEX,
                market_id=48,  # PAXG market
                auth=auth_token
            )
            print(f"Active orders type: {type(active_orders)}")
            print(f"Active orders repr: {repr(active_orders)}")

            print("\n--- ACTIVE ORDERS ANALYSIS ---")
            if hasattr(active_orders, '__dict__'):
                print("Active orders __dict__:")
                pp.pprint(active_orders.__dict__)

            if hasattr(active_orders, 'to_dict'):
                try:
                    orders_dict = active_orders.to_dict()
                    print("Active orders to_dict():")
                    pp.pprint(orders_dict)
                except Exception as e:
                    print(f"Active orders to_dict() failed: {e}")
        except Exception as e:
            print(f"❌ Active orders call failed: {e}")

        # Method 5: Transaction API for balance info
        print("\n[METHOD 5] Transaction API")
        print("-" * 40)

        try:
            tx_api = lighter.TransactionApi(api_client)
            next_nonce = await tx_api.next_nonce(
                account_index=ACCOUNT_INDEX,
                api_key_index=API_KEY_INDEX
            )
            print(f"Next nonce type: {type(next_nonce)}")
            print(f"Next nonce repr: {repr(next_nonce)}")

            print("\n--- NONCE ANALYSIS ---")
            if hasattr(next_nonce, '__dict__'):
                print("Next nonce __dict__:")
                pp.pprint(next_nonce.__dict__)

            if hasattr(next_nonce, 'to_dict'):
                try:
                    nonce_dict = next_nonce.to_dict()
                    print("Next nonce to_dict():")
                    pp.pprint(nonce_dict)
                except Exception as e:
                    print(f"Next nonce to_dict() failed: {e}")
        except Exception as e:
            print(f"❌ Transaction API call failed: {e}")

        # Method 6: Check all available account API methods
        print("\n[METHOD 6] All Account API Methods")
        print("-" * 40)

        account_methods = [m for m in dir(account_api) if not m.startswith('_') and callable(getattr(account_api, m))]
        print(f"Available methods: {account_methods}")

        for method_name in account_methods:
            if method_name == 'account':  # Already tested
                continue

            try:
                method = getattr(account_api, method_name)
                method_params = method.__code__.co_varnames

                print(f"\nTesting {method_name} (params: {method_params})")

                if 'account_index' in method_params:
                    if 'auth' in method_params:
                        result = await method(account_index=ACCOUNT_INDEX, auth=auth_token)
                    else:
                        result = await method(account_index=ACCOUNT_INDEX)
                    print(f"  ✅ {method_name} result type: {type(result)}")
                    print(f"  ✅ {method_name} result repr: {repr(result)}")

                    if hasattr(result, '__dict__'):
                        print(f"  {method_name} __dict__:")
                        pp.pprint(result.__dict__, width=100)

                    if hasattr(result, 'to_dict'):
                        try:
                            result_dict = result.to_dict()
                            print(f"  {method_name} to_dict():")
                            pp.pprint(result_dict, width=100)
                        except Exception as e:
                            print(f"  {method_name} to_dict() failed: {e}")

                    # Show all attributes for this result
                    print(f"  {method_name} attributes:")
                    for attr in dir(result):
                        if not attr.startswith('_'):
                            try:
                                value = getattr(result, attr)
                                if not callable(value):
                                    print(f"    {attr}: {value} (type: {type(value)})")
                            except Exception as e:
                                print(f"    {attr}: ERROR - {e}")
                else:
                    print(f"  ⏭️ Skipping {method_name} (no account_index param)")

            except Exception as e:
                print(f"  ❌ {method_name} failed: {e}")

        await signer_client.close()

        # Method 7: Parse response for any balance-like data
        print("\n[METHOD 7] Deep Balance Detection")
        print("-" * 40)

        def find_balance_data(obj, path="root", depth=0):
            """Recursively search for balance-like data in any object."""
            max_depth = 5  # Prevent infinite recursion
            if depth > max_depth:
                return

            indent = "  " * depth
            balance_keywords = ['balance', 'capital', 'equity', 'margin', 'available', 'free', 'usable', 'cash', 'funds', 'wallet', 'asset', 'collateral']

            print(f"{indent}🔍 Searching {path} (type: {type(obj)})")

            if hasattr(obj, '__dict__'):
                obj_dict = obj.__dict__
                print(f"{indent}  Object has {len(obj_dict)} attributes")

                for attr_name, attr_value in obj_dict.items():
                    current_path = f"{path}.{attr_name}"
                    print(f"{indent}  📝 {attr_name}: {attr_value} (type: {type(attr_value)})")

                    # Check if attribute name suggests it's balance data
                    if any(keyword in attr_name.lower() for keyword in balance_keywords):
                        print(f"{indent}  🎯 POTENTIAL BALANCE FIELD: {current_path} = {attr_value}")

                        try:
                            if isinstance(attr_value, (int, float, str)):
                                float_val = float(attr_value)
                                if float_val > 0:
                                    print(f"{indent}  💰💰 POSITIVE BALANCE FOUND: {current_path} = ${float_val} 💰💰")
                                elif float_val == 0:
                                    print(f"{indent}  ⚪ Zero balance: {current_path} = ${float_val}")
                                else:
                                    print(f"{indent}  🔴 Negative balance: {current_path} = ${float_val}")
                        except Exception as e:
                            print(f"{indent}  ❌ Could not convert to float: {e}")

                    # Recurse into nested objects
                    if hasattr(attr_value, '__dict__') and depth < max_depth:
                        find_balance_data(attr_value, current_path, depth + 1)
                    elif isinstance(attr_value, list) and depth < max_depth:
                        find_balance_data(attr_value, current_path, depth + 1)

            elif isinstance(obj, list):
                print(f"{indent}  List has {len(obj)} items")
                for i, item in enumerate(obj):
                    if i < 10:  # Limit to first 10 items to avoid spam
                        find_balance_data(item, f"{path}[{i}]", depth + 1)
                    elif i == 10:
                        print(f"{indent}  ... (showing only first 10 items)")
                        break

            elif isinstance(obj, dict):
                print(f"{indent}  Dict has {len(obj)} keys")
                for key, value in obj.items():
                    current_path = f"{path}[{key}]"
                    print(f"{indent}  📝 {key}: {value} (type: {type(value)})")

                    if any(keyword in str(key).lower() for keyword in balance_keywords):
                        print(f"{indent}  🎯 POTENTIAL BALANCE KEY: {current_path} = {value}")
                        try:
                            if isinstance(value, (int, float, str)):
                                float_val = float(value)
                                if float_val > 0:
                                    print(f"{indent}  💰💰 POSITIVE BALANCE FOUND: {current_path} = ${float_val} 💰💰")
                        except:
                            pass

                    if isinstance(value, (dict, list)) and depth < max_depth:
                        find_balance_data(value, current_path, depth + 1)
            else:
                print(f"{indent}  Primitive value: {obj}")

        # Re-get account data and search deeply
        print("\n🔍 DEEP SCANNING FOR BALANCE DATA...")
        fresh_account = await account_api.account(by="index", value=str(ACCOUNT_INDEX))
        find_balance_data(fresh_account)

        # Also try the authenticated version
        if auth_token:
            print("\n🔍 DEEP SCANNING AUTHENTICATED DATA...")
            try:
                auth_fresh = await account_api.account(by="index", value=str(ACCOUNT_INDEX), auth=auth_token)
                find_balance_data(auth_fresh, "auth_account")
            except Exception as e:
                print(f"Auth deep scan failed: {e}")

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

    finally:
        await api_client.close()

    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)
    print("If no balance data was found:")
    print("1. Account may have no funds deposited")
    print("2. Funds may need to be deposited via Lighter web interface")
    print("3. Account may need to be activated with a transaction")
    print("4. Balance data may only be available via WebSocket feeds")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(test_available_capital())