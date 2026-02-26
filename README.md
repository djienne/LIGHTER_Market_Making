# Lighter DEX Market Maker

This repository is a Python market-making system for the Lighter DEX. It has three core components:
1. Data Collector (`gather_lighter_data.py`): streams order book and trades to `lighter_data/` as Parquet.
2. Parameter Calculator (`calculate_avellaneda_parameters.py`): computes Avellaneda-Stoikov parameters from collected data and writes JSON files in `params/`.
3. Market Maker (`market_maker_v2.py`): places and manages two-sided limit orders on Lighter.

By default, the market maker uses 12% of available balance per order (see `CAPITAL_USAGE_PERCENT` in `market_maker_v2.py`).

Affiliate link to Support this project : ⚡Trade on Lighter – Spot & Perpetuals, 100% decentralized, no KYC, and ZERO fees – [https://app.lighter.xyz/?referral=FREQTRADE](https://app.lighter.xyz/?referral=FREQTRADE) (I’ll give you 100% kickback with this link)

Note: `market_maker_v2.py` loads Avellaneda parameters from `PARAMS_DIR` and refreshes them periodically. If the params file is missing or invalid, it falls back to a 0.1% spread; with `REQUIRE_PARAMS=true`, it skips quoting instead.

## Quick Start

### Prerequisites
- Python 3.10+
- Docker and Docker Compose (optional)

### Installation
```bash
pip install -r requirements.txt
```

### Configuration
1. Create `.env` from `.env.example` and fill in your credentials.
2. Edit `config.json` to set `CRYPTO_TICKERS` and (optionally) `GAMMA`.

### Environment variables
Required for trading:
- `API_KEY_PRIVATE_KEY`, `ACCOUNT_INDEX`, `API_KEY_INDEX`: Lighter credentials.
- `MARKET_SYMBOL`: market to trade (default `PAXG`). Can also be set via `--symbol`.

Optional and data/ops:
- `PARQUET_MAX_MB`: parquet part size for the data collector (default `5`).
- `LEVERAGE`: leverage to set at startup (default `1`).
- `MARGIN_MODE`: `cross` or `isolated` (default `cross`).
- `PARAMS_DIR`: parameter output directory (default `params`).
- `LOG_DIR`: log directory (default `logs`).
- `HL_DATA_LOC`: where the parameter calculator reads parquet data (default `lighter_data`).

Reserved (declared in code but not currently used in `market_maker_v2.py`):
- `FLIP`

Other optional:
- `REQUIRE_PARAMS`: when `true`, skip quoting if no valid Avellaneda params are loaded (default `false`).

### Notes
- Use a dedicated account or sub-account and start with small amounts.
- The data collector only reads public market data and can run without credentials.
- To find your `ACCOUNT_INDEX`, query `https://mainnet.zklighter.elliot.ai/api/v1/accountsByL1Address?l1_address=0x...` with your L1 wallet address.

### Running with Docker
```bash
docker-compose build
docker-compose up -d
```
Only the data collector runs by default. Uncomment the `parameter-calculator` and `market-maker` services in `docker-compose.yml` when you are ready.

### Running scripts locally
```bash
python gather_lighter_data.py
python calculate_avellaneda_parameters.py PAXG --minutes 15
python market_maker_v2.py --symbol PAXG
```

### Tests
```bash
python test_runner.py
python check_parquet.py
python -m unittest discover -s tests -p "test_*.py"
```

## Example Output
![Market Maker Logs](screen.png)

## Project Structure
- docker-compose.yml
- Dockerfile
- config.json
- requirements.txt
- .env.example
- gather_lighter_data.py
- calculate_avellaneda_parameters.py
- market_maker_v2.py
- utils.py
- orderbook.py (shared order book update logic)
- ws_manager.py (shared WebSocket subscription manager)
- volatility.py
- intensity.py
- backtest.py
- test_runner.py
- check_parquet.py
- tests/
- lighter_data/ (prices_*_part*.parquet, trades_*_part*.parquet)
- params/ (avellaneda_parameters_<SYMBOL>.json)
- logs/
- DOC/
- OLD/

## How It Works

### 1. Data Collection
`gather_lighter_data.py` subscribes to order book and trade channels for `CRYPTO_TICKERS` from `config.json`. It maintains a local order book, stores the top 10 levels, and flushes buffered data to compressed parquet parts in `lighter_data/`. File rotation is size-based via `PARQUET_MAX_MB`.

### 2. Parameter Calculation
`calculate_avellaneda_parameters.py` loads recent parquet data and computes:
- Volatility (`sigma`) via GARCH(1,1), with fallback to rolling statistics.
- Order intensity parameters (`A_bid`, `k_bid`, `A_ask`, `k_ask`).
- Fixed risk aversion (`GAMMA`) from `config.json`.

Results are written to `params/avellaneda_parameters_<SYMBOL>.json`.

### 3. Market Making
`market_maker_v2.py` places two-sided limit orders on Lighter. Sizing uses:
```
base_amount = available_balance * CAPITAL_USAGE_PERCENT / mid_price
```
The size is rounded to tick size when available. If no parameters are loaded, it uses a fixed 0.1% spread around mid price.

## Risk Warning
This trading software will likely lose money and is not competitive with professional firms. Always start with small amounts and understand the risks of automated trading.
