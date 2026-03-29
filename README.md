# Lighter DEX Market Maker

Automated two-sided market maker for [Lighter](https://lighter.xyz) perpetual futures. Spread width is driven by realized volatility, and an order-book imbalance (OBI) signal computed from Binance's order book serves as the alpha factor to bias quotes ahead of anticipated moves. Inventory skew tilts quotes to mean-revert position toward zero.

Affiliate link to support this project: [Trade on Lighter](https://app.lighter.xyz/?referral=FREQTRADE) — spot & perpetuals, fully decentralized, no KYC, zero fees (100% kickback with this link).

## Quick Start

### Prerequisites
- Python 3.13+ (venv recommended)
- C compiler and Python dev headers — required to build the Cython extension
  - Debian/Ubuntu: `sudo apt install build-essential python3-dev`
  - macOS: `xcode-select --install`
  - Windows (MinGW — recommended): install [MinGW-w64](https://www.mingw-w64.org/) (or via [WinLibs](https://winlibs.com/)), add its `bin/` to `PATH`, then build with `python setup_cython.py build_ext --inplace --compiler=mingw32`
  - Windows (MSVC — alternative): install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) with the "Desktop development with C++" workload
- Lighter API credentials (private key, public key, account index, API key index)

### Installation
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python setup_cython.py build_ext --inplace
```

The last command compiles the Cython extension (`_vol_obi_fast.so`) with `-O3 -march=native -ffast-math` for ~30x faster order book and spread computation. **Cython is required** — the bot will refuse to start without it. Set `ALLOW_PYTHON_FALLBACK=1` to bypass for dev/test only.

### Configuration

1. Create `.env` with your credentials (only needed for `--live` mode — dry-run and grid mode work without it):
```
API_KEY_PRIVATE_KEY=0x...
API_KEY_PUBLIC_KEY=0x...
API_KEY_INDEX=25
ACCOUNT_INDEX=107607
```

2. Edit `config.json` to tune trading parameters (see below).

### Running
```bash
source venv/bin/activate
python -u market_maker_v2.py --symbol BTC
```

```bash
# Grid dry-run: test N parameter combos in parallel (paper trading, no .env needed)
python -u market_maker_v2.py --symbol BTC --grid grid_config.json
```

See [GRID_QUICKSTART.md](GRID_QUICKSTART.md) for full grid configuration and monitoring details.

Always use `-u` for unbuffered log output. Logs are written to stdout and `logs/`.

## Architecture

```
market_maker_v2.py       Main market-making loop
dry_run.py               Paper-trading fill simulation engine
vol_obi.py               VolObiCalculator + RollingStats spread model
_vol_obi_fast.pyx        Cython extension (RollingStats, VolObiCalculator, CBookSide)
setup_cython.py          Build script for the Cython extension
binance_obi.py           Binance Futures OBI alpha signal
orderbook.py             Local order book update logic
orderbook_sanity.py      WS vs REST book cross-check
ws_manager.py            WebSocket subscription manager (ws_subscribe + ws_subscribe_fast)
trade_log.py             Buffered CSV trade logger
utils.py                 Shared helpers (config loading, Parquet I/O)
logging_config.py        Centralized logging setup
adjust_leverage.py       CLI tool to set leverage/margin mode
grid_dry_run.py          Parallel grid dry-run engine (N sims sharing one WS)
deploy.py                Zip + upload to remote VPS
config.json              Runtime configuration
grid_config.json         Grid parameter search configuration
tests/                   Unit tests (pytest)
```

## How It Works

### Spread Model (vol_obi)
- **RollingStats** maintains a ring buffer of mid-price samples and computes rolling volatility in O(1) using Welford's online algorithm for numerical stability.
- **VolObiCalculator** combines volatility with order-book imbalance (OBI) to produce skewed bid/ask half-spreads. Spreads widen with volatility and shift toward the heavier side of the book. A crossed-quote guard ensures bid < ask after tick rounding.

### Alpha Signal (Binance)
A background WebSocket streams Binance Futures depth data. The OBI signal biases quotes before they hit Lighter. Stale signals (>5s) are discarded.

### Adaptive Quote Threshold
The quote update threshold scales with volume quota pressure:
| Quota remaining | Multiplier | Effect at 10bp base |
|----------------|-----------|---------------------|
| >= 500         | 1x        | 10bp                |
| 50 - 499       | 2x        | 20bp                |
| 10 - 49        | 3.5x      | 35bp                |
| 0 - 9          | 5x        | 50bp                |

This conserves quota when it's scarce by only requoting on larger price moves.

### Quota Recovery
When quota drops below threshold, the bot can execute small IOC market orders via the free 15-second slot to generate fill volume and rebuild quota.

### Dynamic Position Limit
The maximum position size is computed dynamically each loop iteration from live account data: `(available_capital * leverage - 2 * order_value) * 0.9`. When position value reaches this limit, the side that would increase exposure is suppressed (buy orders suppressed when long at limit, sell orders suppressed when short at limit) and any existing orders on that side are canceled. The vol_obi skew normalization stays in sync with this dynamic limit.

### Hot/Cold Path Separation
- **Hot path** (market data): orderbook/ticker use `ws_subscribe_fast()` — a tight `await ws.recv()` loop with no per-message task overhead. Book mutation, mid calc, vol_obi update, and quote diff run synchronously. Base amount and max position are precomputed on capital/mid change events, not per tick.
- **Cold path** (control plane): account data, reconciliation, REST calls, telemetry, trade logging all run on separate WS channels or background tasks. Trade sort/dedup/log is deferred via `call_soon` to avoid blocking market data ingestion.
- **Order state ownership**: all order mutations go through a deferred event queue (`_order_event_queue`). WS handlers and the reconciler push events; `OrderManager.drain_events()` is the sole writer, called at defined points in the hot loop.

### Safety Controls
- **Orderbook sanity**: periodic REST snapshots cross-checked against WS book
- **Stale order poller**: reconciles local vs exchange order state; PLACING orders timeout after 30s
- **Circuit breaker**: pauses trading after consecutive rejections; force-clears stuck orders after 5 retries
- **Max live orders**: caps open orders per market
- **Adaptive backoff**: exponential 429 backoff (15s→120s) with 5-minute time-based auto-decay
- **Nonce safety**: `acknowledge_failure` + `hard_refresh_nonce` on all error paths

## config.json Reference

### `trading`
| Key | Default | Description |
|-----|---------|-------------|
| `leverage` | `2` | Leverage set at startup |
| `margin_mode` | `cross` | `cross` or `isolated` |
| `levels_per_side` | `1` | Quote levels per side |
| `base_amount` | `0.0002` | Fallback order size (base currency) |
| `capital_usage_percent` | `0.12` | Fraction of balance per order |
| `default_quote_update_threshold_bps` | `10.0` | Base requote threshold (adaptive system may widen) |
| `spread_factor_level1` | `2.0` | Level 1 spread multiplier |
| `order_timeout_seconds` | `30.0` | Order placement timeout |
| `position_value_threshold_usd` | `11.0` | USD threshold to consider position flat |
| `min_order_value_usd` | `14.5` | Minimum order value in USD |

### `trading.vol_obi`
| Key | Default | Description |
|-----|---------|-------------|
| `window_steps` | `6000` | Rolling window length |
| `step_ns` | `100000000` | Step size in nanoseconds (100ms) |
| `vol_to_half_spread` | `48.0` | Volatility to half-spread multiplier |
| `min_half_spread_bps` | `8.0` | Minimum half-spread (bps) |
| `c1_ticks` | `20.0` | OBI skew coefficient in ticks |
| `skew` | `3.0` | Global skew scaling factor |
| `looking_depth` | `0.025` | Book depth fraction for OBI |
| `min_warmup_samples` | `100` | Samples before live quoting |
| `warmup_seconds` | `600` | Warmup period (10 minutes) |

### `trading.alpha`
| Key | Default | Description |
|-----|---------|-------------|
| `source` | `binance` | Alpha source (`binance` or `none`) |
| `stale_seconds` | `5.0` | Max age before signal is discarded |
| `window_size` | `6000` | Rolling window for Binance OBI |
| `min_samples` | `150` | Minimum samples before signal is active |
| `looking_depth` | `0.025` | Binance book depth fraction for OBI |

### `performance`
| Key | Default | Description |
|-----|---------|-------------|
| `min_loop_interval` | `0.1` | Min seconds between loop iterations |
| `rate_limit_send_interval` | `0.1` | Min seconds between order sends |

### `performance.quota_recovery`
| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `true` | Enable automatic quota recovery |
| `trigger_threshold` | `5` | Quota level that triggers recovery |
| `target_quota` | `50` | Target quota to recover to |
| `max_attempts` | `3` | Max recovery attempts per cooldown |
| `max_loss_usd` | `2.0` | Max acceptable loss per recovery order |
| `cooldown_seconds` | `120` | Cooldown between recovery cycles |
| `post_warmup_grace_seconds` | `120` | Grace period after warmup before recovery |

### `websocket`
| Key | Default | Description |
|-----|---------|-------------|
| `ping_interval` | `20` | WS ping interval (seconds) |
| `recv_timeout` | `30.0` | WS receive timeout (seconds) |
| `account_recv_timeout` | `300.0` | Account WS receive timeout (seconds) |
| `reconnect_base_delay` | `5` | Base reconnect backoff (seconds) |
| `reconnect_max_delay` | `60` | Max reconnect backoff (seconds) |

### `safety`
| Key | Default | Description |
|-----|---------|-------------|
| `stale_order_poller_interval_sec` | `3.0` | Order reconciliation interval |
| `stale_order_debounce_count` | `2` | Mismatches before safety pause |
| `max_consecutive_order_rejections` | `5` | Rejections triggering circuit breaker |
| `circuit_breaker_cooldown_sec` | `60.0` | Cooldown after circuit breaker trips |
| `order_reconcile_timeout_sec` | `2.0` | Cancel confirmation timeout |
| `max_live_orders_per_market` | `2` | Max open orders per market |

**Config precedence:** env var > `config.json` > hardcoded default.

## Tests

```bash
source venv/bin/activate
pytest tests/ -v
```

## Risk Warning

This is experimental trading software. It will likely lose money and is not competitive with professional market makers. Always start with small amounts and understand the risks of automated trading.

