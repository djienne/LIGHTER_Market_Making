# Project Overview

This project is a Python-based trading bot for the Lighter exchange. It utilizes the `lighter-python` library to interact with the exchange's API, enabling automated trading operations. The bot is designed to be run from the command line and can be configured to place various types of orders, including limit and market orders.

## Project Structure

```
.
├── .env
├── docker-compose.yml
├── Dockerfile
├── examples
│   ├── create_cancel_order.py
│   ├── create_market_order_max_slippage.py
│   ├── create_market_order.py
│   ├── create_sl_tp.py
│   ├── create_with_multiple_keys.py
│   ├── get_info.py
│   ├── README.md
│   ├── send_tx_batch.py
│   ├── system_setup.py
│   ├── transfer_update_leverage.py
│   ├── ws_async.py
│   ├── ws_send_batch_tx.py
│   ├── ws_send_tx.py
│   └── ws.py
├── GEMINI.md
├── place_limit_buy_order.py
└── requirements.txt
```

## Key Technologies

*   **Python 3:** The core programming language used for the bot's logic.
*   **lighter-python:** A Python library for interacting with the Lighter exchange API.
*   **websockets:** Used for real-time data streaming from the exchange.
*   **python-dotenv:** For managing environment variables, such as API keys.

## Building and Running

### 1. Installation

Install the required Python packages using pip:

```bash
pip install -r requirements.txt
```

### 2. Configuration

1.  Create a `.env` file in the root directory of the project.
2.  Add the following environment variables to the `.env` file:

    ```
    API_KEY_INDEX=<your_api_key_index>
    API_KEY_PRIVATE_KEY=<your_api_key_private_key>
    ACCOUNT_INDEX=<your_account_index>
    ```

    You can obtain these credentials by following the instructions in the `examples/README.md` file.

### 3. Running the Bot

The main script for placing orders is `place_limit_buy_order.py`. You can run it from the command line:

```bash
python place_limit_buy_order.py
```

The `examples` directory contains additional scripts for various trading operations, such as creating and canceling orders, placing market orders, and setting stop-loss and take-profit orders.

## Development Conventions

*   The project follows standard Python 3 coding conventions (PEP 8).
*   The `logging` module is used for logging information and errors.
*   The `asyncio` library is used for asynchronous operations, particularly for handling WebSocket connections.
*   Environment variables are used to store sensitive information, such as API keys.
*   Type hints are used for function arguments and return values.