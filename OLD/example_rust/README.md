# StandX Market Maker

High-performance market making system for the StandX perpetual futures exchange, written in Rust.

## Features

- **Low-latency architecture**: Synchronous hot path (<10μs decisions), async execution
- **Lock-free data structures**: Triple buffer for orderbook, atomic position reads
- **OBI Strategy**: Order Book Imbalance based quotes with volatility scaling and position skew
- **Binance orderbook alpha**: Uses Binance Futures OBI for alpha signal, with automatic StandX fallback
- **Multi-level quoting**: 1-2 order levels per side with configurable spread multipliers
- **Dynamic equity-based sizing**: Order size and position limits derived from account equity with leverage support
- **Modular design**: `QuoteStrategy` trait for easy strategy swapping
- **WebSocket trading**: Low-latency order execution (5-50ms)
- **Position management**: Background polling with lock-free reads
- **Auto-reconnection**: Exponential backoff on disconnect
- **Graceful shutdown**: Cancels all orders on Ctrl+C

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Main Event Loop (tokio::select!)                  │
│                                                                      │
│  WS Orderbook ──► ObiStrategy ──► OrderManager.on_quote() ──► Tx    │
│                   .update()       (SYNC, <10μs)              (mpsc) │
│                      │                   │                     │    │
│                      ▼                   ▼                     ▼    │
│               SharedPosition      OrderDecision         OrderExecutor│
│               (lock-free read)                          (async task)│
└─────────────────────────────────────────────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┼──────┐
│                    Order Event Handler                       │      │
│                                                              ▼      │
│  OrderWsClient ──► OrderEvent (Accept/Cancel) ──► State Update     │
└─────────────────────────────────────────────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┼──────┐
│               Position Poller (HTTP, 2s interval)            │      │
│                                                              ▼      │
│  REST API ──► Position ──► SharedPosition (Atomic Update)          │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.70+ (for local development)
- Docker & Docker Compose (for deployment)
- StandX API credentials (set in `.env`)

### Setup

```bash
# Clone and build
cd standx-rs
cargo build --release

# Create .env file with credentials
cp .env.example .env
# Edit .env with your wallet address and private key

# Run locally
cargo run --release
```

### Docker Deployment

```bash
# Build and start in background
docker compose up -d

# Follow logs
docker compose logs -f bot

# Stop (gracefully cancels all orders)
docker compose down

# Test mode (no real orders)
docker compose --profile test up bot-test
```

### Environment Variables

Copy `.env.example` to `.env` and configure:

| Variable | Description |
|----------|-------------|
| `WALLET_AD` | Your wallet address (0x...) |
| `PRIVATE_KEY` | Your private key (without 0x prefix) |
| `DEPLOY_USER` | SSH username for deployment |
| `DEPLOY_HOST` | Server IP/hostname for deployment |
| `DEPLOY_SSH_KEY` | Path to SSH private key |
| `DEPLOY_DIR` | Remote directory (default: ~/standx-bot) |

### Configuration

Edit `config.json`:

```json
{
  "symbols": ["BTC-USD"],
  "orderbook_levels": 20,
  "history_minutes": 10,
  "debug": true,
  "strategy": {
    "tick_size": 0.01,
    "lot_size": 0.001,
    "step_ns": 100000000,
    "window_steps": 6000,
    "update_interval_steps": 1,
    "vol_to_half_spread": 0.8,
    "half_spread_bps": 0.0,
    "min_half_spread_bps": 2.0,
    "skew": 1.0,
    "c1": 0.0,
    "c1_ticks": 160.0,
    "looking_depth": 0.025,
    "min_order_qty_dollar": 10.0,
    "order_levels": 1,
    "spread_level_multiplier": 1.5,
    "alpha_source": "binance",
    "binance_stale_ms": 5000,
    "leverage": 1.0
  },
  "order": {
    "enabled": false,
    "reprice_threshold_bps": 1.0,
    "pending_timeout_secs": 5,
    "max_live_age_secs": 60,
    "circuit_breaker_rejections": 5,
    "circuit_breaker_recovery_secs": 300,
    "safety_pause_recovery_secs": 30
  },
  "position": {
    "enabled": true,
    "poll_interval_secs": 2,
    "stale_threshold_secs": 10
  },
  "pnl_tracking": {
    "enabled": false,
    "poll_interval_secs": 60,
    "csv_path": "wallet_history.csv"
  },
  "orderbook_sanity_check": {
    "enabled": false,
    "interval_secs": 30,
    "drift_threshold_bps": 1.0
  }
}
```

## Configuration Reference

### General

| Parameter | Description | Default |
|-----------|-------------|---------|
| `symbols` | Trading pairs to quote | `["BTC-USD"]` |
| `orderbook_levels` | Orderbook depth to track | `20` |
| `history_minutes` | Warmup period before trading | `10` |
| `debug` | Enable debug logging | `true` |
| `stats_interval_secs` | Stats logging interval | `30` |

### Strategy (`strategy` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `tick_size` | **FALLBACK** - only used if API fetch fails | `0.01` |
| `lot_size` | **FALLBACK** - only used if API fetch fails | `0.001` |
| `step_ns` | Rolling window step size (ns) | `100000000` |
| `window_steps` | Number of steps in rolling window | `6000` |
| `update_interval_steps` | Quote update frequency (in steps) | `1` |
| `vol_to_half_spread` | Volatility to half-spread multiplier | `0.8` |
| `half_spread` | Fixed half-spread in price units (0 to disable) | `0.0` |
| `half_spread_bps` | Fixed half-spread in bps (0 to disable) | `0.0` |
| `min_half_spread_bps` | Minimum half-spread floor in bps | `2.0` |
| `skew` | Position skew factor | `1.0` |
| `c1` | Alpha coefficient in price units (tick-size independent) | `0.0` |
| `c1_ticks` | **DEPRECATED** - use `c1` instead. Falls back if `c1=0` | `160.0` |
| `looking_depth` | OBI depth as fraction of mid | `0.025` |
| `min_order_qty_dollar` | Minimum order size in USD (dust prevention) | `10.0` |
| `order_levels` | Number of order levels per side (1 or 2) | `1` |
| `spread_level_multiplier` | Spread multiplier for outer level (must be >1.0) | `1.5` |
| `alpha_source` | Alpha signal source: `"binance"` or `"standx"` | `"binance"` |
| `binance_stale_ms` | Binance fallback threshold in ms | `5000` |
| `leverage` | Position/order scaling multiplier (1.0–5.0) | `1.0` |

**Note:** `tick_size` and `lot_size` are automatically fetched from the exchange API on startup. The config values are only used as fallbacks if the API is unreachable. Order size (`order_qty_dollar`) and max position (`max_position_dollar`) are dynamically computed from account equity via `SharedEquity` — they are not config parameters.

### Order Management (`order` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable order placement | `false` |
| `reprice_threshold_bps` | Min price change to reprice | `1.0` |
| `pending_timeout_secs` | Order confirmation timeout | `5` |
| `max_live_age_secs` | Max order age before refresh | `60` |
| `circuit_breaker_rejections` | Pause after N consecutive rejections (0=disabled) | `5` |
| `circuit_breaker_recovery_secs` | Auto-resume trading after N seconds (0=manual reset required) | `300` |
| `safety_pause_recovery_secs` | Auto-recovery from duplicate/stuck orders (0=manual) | `30` |
| `max_reconnect_attempts` | Max WebSocket reconnect attempts (0=unlimited) | `0` |

### Position Polling (`position` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable position polling | `true` |
| `poll_interval_secs` | Position polling interval | `2` |
| `stale_threshold_secs` | Warn if position data older than this | `10` |

### PnL Tracking (`pnl_tracking` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable PnL tracking | `false` |
| `poll_interval_secs` | Balance polling interval | `60` |
| `csv_path` | Path to CSV history file | `wallet_history.csv` |

### Orderbook Sanity Check (`orderbook_sanity_check` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable periodic REST API validation | `false` |
| `interval_secs` | Check interval | `30` |
| `drift_threshold_bps` | Drift threshold to trigger correction | `1.0` |

### Symbol Info (`symbol_info` section)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enabled` | Enable automatic tick/lot size detection from API | `true` |
| `poll_interval_secs` | Polling interval to check for tick size changes | `30` |

When `enabled`, the bot:
1. Fetches `tick_size` and `lot_size` from the exchange API on startup
2. Polls periodically to detect runtime changes (e.g., tick size updates)
3. If tick size changes: pauses trading, cancels orders, resets strategy, resumes with new values

## Project Structure

```
standx-rs/
├── src/
│   ├── main.rs           # Application entry point
│   ├── lib.rs            # Library exports
│   ├── config.rs         # Configuration loading
│   ├── logging.rs        # Logging utilities
│   ├── types.rs          # Core data types
│   ├── binance/          # Binance Futures integration
│   │   ├── mod.rs        # Module exports
│   │   ├── client.rs     # Binance WebSocket client
│   │   ├── messages.rs   # Binance message parsing
│   │   ├── orderbook.rs  # Binance orderbook processing
│   │   └── shared_alpha.rs # Lock-free shared alpha (atomic reads)
│   ├── orderbook/        # Lock-free orderbook storage
│   │   ├── snapshot.rs   # CurrentOrderbook triple buffer
│   │   ├── history.rs    # OrderbookHistory ring buffer
│   │   └── sanity_check.rs  # REST API orderbook validation
│   ├── websocket/        # WebSocket client for market data
│   │   ├── client.rs     # WebSocket connection handling
│   │   ├── messages.rs   # StandX message parsing
│   │   └── reconnect.rs  # Auto-reconnection with backoff
│   ├── strategy/         # Quote calculation strategies
│   │   ├── traits.rs     # QuoteStrategy trait
│   │   ├── obi.rs        # OBI strategy implementation
│   │   ├── quotes.rs     # Quote output formatting
│   │   └── rolling.rs    # Rolling statistics
│   └── trading/          # Order execution
│       ├── auth.rs       # JWT authentication
│       ├── client.rs     # REST API client
│       ├── equity.rs     # Dynamic equity-based order sizing (SharedEquity)
│       ├── orders.rs     # Order types and request structs
│       ├── order_ws.rs   # WebSocket order client
│       ├── order_manager.rs  # Order state machine
│       ├── order_checker.rs  # Open orders polling/stale detection
│       ├── position.rs   # Position polling
│       ├── symbol_info.rs # Dynamic tick/lot size detection
│       └── wallet_tracker.rs # PnL tracking with CSV
├── config.json           # Configuration file
├── .env.example          # Environment template
├── .env                  # API credentials (not in git)
└── Cargo.toml
```

## Strategy Interface

The codebase uses a trait-based design for easy strategy swapping:

```rust
pub trait QuoteStrategy {
    /// HOT PATH - Process orderbook, return quote
    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote>;

    fn set_position(&mut self, position: f64);
    fn position(&self) -> f64;
    fn is_valid_for_trading(&self) -> bool;
    fn is_warmed_up(&self) -> bool;
    fn volatility(&self) -> f64;
    fn alpha(&self) -> f64;
    fn history_duration_secs(&self) -> f64;
    fn required_history_secs(&self) -> f64;
    fn reset(&mut self);
}
```

### Adding a New Strategy

1. Create `src/strategy/my_strategy.rs`
2. Implement `QuoteStrategy` trait
3. Export from `src/strategy/mod.rs`
4. Change strategy type in `main.rs`

## OBI Strategy Details

The default OBI (Order Book Imbalance) strategy calculates quotes based on:

1. **Volatility**: Rolling std dev of mid-price changes → base half-spread
2. **Alpha (OBI)**: Z-score of bid-ask quantity imbalance → fair price adjustment (source: Binance or StandX)
3. **Position Skew**: Current position → asymmetric spread widening
4. **BBO Clamping**: Quotes never cross the best bid/ask
5. **Min Spread Floor**: `min_half_spread_bps` enforced after clamping
6. **Tick Snapping**: Final prices rounded to tick grid

```
fair_price = mid_price + c1 * alpha

# Per level (level 0 = inner, level 1 = outer):
level_half_spread = base_half_spread * spread_level_multiplier^level
bid_depth = level_half_spread * (1 + skew * normalized_position)
ask_depth = level_half_spread * (1 - skew * normalized_position)

bid_price = clamp(fair_price - bid_depth, max=best_bid)
ask_price = clamp(fair_price + ask_depth, min=best_ask)

# Apply min_half_spread_bps floor, then snap to tick grid
```

## Latency Budget

| Component | Target |
|-----------|--------|
| `strategy.update()` | <5μs |
| `order_manager.on_quote()` | <10μs |
| `mpsc.try_send()` | <1μs |
| **Total hot path** | **<15μs** |
| WebSocket order send | 5-50ms (async) |

## Safety Features

- **POST-ONLY orders**: All orders are maker-only (no taker fees, no crossing)
- **Position limits**: Stops quoting one side at ±max_position_dollar (derived from equity)
- **NaN/Inf validation**: Rejects quotes with invalid prices before order placement
- **Graceful shutdown**: Cancels all live orders on Ctrl+C
- **Order timeout**: Clears stuck orders after pending_timeout_secs
- **Circuit breaker**: Pauses trading after N consecutive rejections (auto-recovery configurable)
- **Safety pause recovery**: Auto-recovers from duplicate/stuck order states after configurable delay
- **Stale connection detection**: Force reconnects WebSocket if no message received within timeout
- **Open orders checker**: Background polling detects stale/imbalanced orders on exchange
- **Auto-reconnect**: WebSocket reconnection with exponential backoff (retries indefinitely by default)

## License

MIT
