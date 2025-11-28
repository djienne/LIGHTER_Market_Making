# Lighter DEX Market Maker

This project is a high-frequency market making bot for the Lighter DEX (zkSync Era), implementing the Avellaneda-Stoikov model to optimize bid/ask spreads. It is architected as a set of Dockerized microservices.

## Architecture

The system is composed of three main services orchestrated via `docker-compose.yml`:

1.  **Data Collector** (`gather_lighter_data.py`)
    *   **Role:** Connects to the Lighter DEX WebSocket to stream real-time order book and trade data.
    *   **Output:** Appends data to CSV files in `lighter_data/`.
    *   **Service Name:** `lighter-data-collector`

2.  **Spread Calculator** (`spread_calculator.py`)
    *   **Role:** Analyzes historical data (from `lighter_data/`) to calculate optimal market making parameters (volatility, gamma, k, A) using the Avellaneda-Stoikov model. It uses GARCH models for volatility estimation.
    *   **Output:** Updates JSON parameter files in `params/`.
    *   **Service Name:** `spread-calculator`

3.  **Market Maker** (`market_maker_v2.py`)
    *   **Role:** The execution engine. It reads parameters from `params/` and places/manages limit orders on the Lighter DEX. It supports leverage, dynamic sizing, and safety mechanisms like position flipping.
    *   **Service Name:** `market-maker`

## Key Files

*   `docker-compose.yml`: Defines the services, volumes, and runtime configuration.
*   `requirements.txt`: Python dependencies (includes `lighter-python`, `pandas`, `numba`, `arch`, `websockets`).
*   `.env`: Configuration file for API keys and trading parameters (see `.env.example`).
*   `market_maker_v2.py`: The current active trading logic (version 2).
*   `spread_calculator.py`: The current parameter calculation logic.
*   `gather_lighter_data.py`: Data ingestion script.

## Setup & Configuration

### Prerequisites
*   Docker & Docker Compose
*   Lighter DEX API Credentials (Private Key, Account Index, API Key Index)

### Configuration
1.  Copy `.env.example` to `.env`.
2.  Fill in your credentials:
    ```env
    API_KEY_PRIVATE_KEY=...
    ACCOUNT_INDEX=...
    API_KEY_INDEX=...
    ```
3.  Configure trading parameters in `.env`:
    *   `MARKET_SYMBOL`: e.g., `PAXG`, `ETH`, `BTC`.
    *   `LEVERAGE`: e.g., `1` (1x), `2` (2x).
    *   `CAPITAL_USAGE_PERCENT`: % of capital to use per order (e.g., `0.25`).
    *   `FLIP`: `true`/`false` to toggle short/long bias.

## Running the Bot

**Start all services:**
```bash
docker-compose build
docker-compose up -d
```

**View logs:**
```bash
docker-compose logs -f market-maker
```

**Stop services:**
```bash
docker-compose down
```

## Development

*   **Dependencies:** `pip install -r requirements.txt`
*   **Legacy Files:**
    *   `market_maker.py`: Previous version of the trading logic.
    *   `calculate_avellaneda_parameters.py`: Previous parameter calculator.
    *   `find_trend_lighter.py`: Standalone trend detection script (may be integrated or run separately depending on config).
