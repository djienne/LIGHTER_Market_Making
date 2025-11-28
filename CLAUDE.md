# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Lighter DEX Market Maker - a complete algorithmic trading system built in Python that implements the Avellaneda-Stoikov market making model. The system consists of three main components:

1. **Data Collector** (`gather_lighter_data.py`) - Streams real-time order book and trade data via WebSocket
2. **Parameter Calculator** (`calculate_avellaneda_parameters.py`) - Calculates optimal trading parameters using the Avellaneda-Stoikov model with GARCH(1,1) volatility estimation
3. **Market Maker** (`market_maker.py`) - Executes the trading strategy by placing and managing orders

## Core Commands

### Docker Operations
```bash
# Build and start all services
docker-compose build
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f [service-name]
```

### Python Script Execution
```bash
# Run data collector (standalone)
python gather_lighter_data.py

# Calculate Avellaneda parameters for a specific crypto (4 hours of data)
python calculate_avellaneda_parameters.py PAXG --hours 4

# Run market maker (requires .env configuration)
python market_maker.py

# Adjust leverage settings
python adjust_leverage.py

# Calculate sigma/volatility
python calculate_sigma.py
```

### Data Analysis Scripts
```bash
# Trend analysis
python find_trend.py
python find_trend_lighter.py

# User statistics
python user_stats_subscriber.py
```

## Architecture

### Service Architecture
The system is designed as a microservices architecture using Docker Compose with three main services:

- **lighter-data-collector**: Continuously collects market data via WebSocket, saves to CSV files in `./lighter_data/`
- **avellaneda-calculator**: Runs every 2 hours, analyzes data and calculates optimal trading parameters, saves JSON files to `./params/`
- **market_maker**: Reads parameters and executes trades, with automatic restarts every 5 minutes

### Data Flow
1. WebSocket data → CSV files (`lighter_data/`)
2. CSV files → Parameter calculation → JSON files (`params/`)
3. JSON parameters → Trading execution → Logs (`logs/`)

### Key Components

**Market Data Pipeline:**
- WebSocket connection to `wss://mainnet.zklighter.elliot.ai/stream`
- Real-time order book and trade data collection
- CSV storage with timestamps for historical analysis

**Avellaneda-Stoikov Model Implementation:**
- GARCH(1,1) volatility estimation with fallback to rolling standard deviation
- Optimal bid/ask spread calculation based on market conditions
- Risk-adjusted reservation price computation
- Parameter validation and atomic file updates

**Trading Engine:**
- Dynamic position sizing based on available capital
- Leverage support (configurable, default 2x)
- Order timeout management (90 seconds default)
- Automatic position closure capabilities

## Configuration

### Environment Variables (.env)
Required for trading:
- `API_KEY_PRIVATE_KEY`: Lighter exchange private key
- `ACCOUNT_INDEX`: Account index on Lighter
- `API_KEY_INDEX`: API key index
- `MARKET_SYMBOL`: Trading pair (default: PAXG)

Optional:
- `LEVERAGE`: Leverage multiplier (default: 2)
- `MARGIN_MODE`: cross/isolated (default: cross)
- `CLOSE_LONG_ON_STARTUP`: true/false (default: false)
- `REQUIRE_PARAMS`: Require valid parameters before trading (default: false)
- `RESTART_INTERVAL_MINUTES`: Auto-restart interval (default: 5)

### Cryptocurrency Configuration
To change trading pairs, update:
1. `MARKET_SYMBOL` in `.env`
2. Add ticker to `CRYPTO_TICKERS` list in `gather_lighter_data.py`
3. Update docker-compose.yml calculator command and healthcheck paths

### Directory Structure
- `lighter_data/`: Historical market data (CSV files)
- `params/`: Calculated trading parameters (JSON files)
- `logs/`: Service logs and debug information
- `OLD/`: Legacy code examples

## Development Notes

### Market Maker Behavior
- Uses all available account funds by default
- Implements compound interest reinvestment
- Static fallback spread of 0.035% when parameters unavailable
- Capital usage: 99% with 1% safety margin
- Dynamic position sizing based on volatility and risk tolerance

### Parameter Calculation
- Requires 1-2 days of historical data for reliable operation
- Runs every 2 hours analyzing last 4 hours of data
- GARCH(1,1) model for volatility with fallback mechanisms
- Atomic parameter file updates to prevent corruption

### Data Collection
- Tracks multiple cryptocurrencies: ETH, BTC, PAXG, ASTER
- WebSocket reconnection logic for reliability
- Continuous operation with health checks

## Important Safety Notes
- Designed for PAXG trading; other cryptocurrencies require configuration changes
- Use dedicated/sub-accounts as the bot uses all available funds
- Start with data collection only for 1-2 days before enabling trading
- Freshly coded system - test with small amounts
- Automatic restarts prevent memory leaks and ensure parameter refresh