# Grid Dry-Run: Quick Start

Run N independent paper-trading simulations in parallel, each with different spread/skew parameters, sharing a single set of WebSocket connections. Find optimal parameters before going live.

## Setup

```bash
cd lighter_MM

# Install dependencies
pip install -r requirements.txt

# Build Cython extension (required — ~10x speedup on hot path)
python setup_cython.py build_ext --inplace

# No .env needed — grid mode is paper-trading only (no exchange credentials required)
```

## Run the grid

```bash
# Start grid dry-run (paper trading only, no real orders)
python -u market_maker_v2.py --symbol BTC --grid grid_config.json

# Or run with the 3-day monitoring wrapper (auto-restart on crash)
bash run_grid_3d.sh
```

## Grid config (`grid_config.json`)

```json
{
  "capital": 1000,
  "leverage": 2,
  "warmup_seconds": 600,
  "summary_interval_seconds": 60,
  "sim_latency_s": 0.050,
  "parameters": {
    "vol_to_half_spread": [8, 12, 24, 48],
    "skew": [0.1, 1.0, 3.0, 5.0]
  },
  "fixed": {
    "min_half_spread_bps": 4,
    "spread_factor_level1": 2.0,
    "capital_usage_percent": 0.12,
    "num_levels": 2,
    "c1_ticks": 20.0
  }
}
```

- **`parameters`**: axes to vary — Cartesian product creates one slot per combination (4x4 = 16 slots above)
- **`fixed`**: constant across all slots
- **`capital`**: starting virtual wallet per slot (USD)
- **`warmup_seconds`**: collect volatility data before trading (600s recommended)
- **`sim_latency_s`**: simulated exchange latency for realistic fill modeling

### Key parameters

| Parameter | What it controls | Range |
|-----------|-----------------|-------|
| `vol_to_half_spread` | Spread width multiplier (lower = tighter quotes, more fills, more risk) | 6–80 |
| `min_half_spread_bps` | Minimum spread floor in basis points | 2–15 |
| `skew` | Position-dependent skew (higher = more aggressive inventory management) | 0.1–5.0 |
| `spread_factor_level1` | How much wider level 1 is vs level 0 | 1.5–3.0 |
| `capital_usage_percent` | Fraction of capital per order side | 0.05–0.20 |
| `num_levels` | Quote levels per side (buy/sell) | 1–3 |
| `c1_ticks` | OBI alpha sensitivity in ticks | 5–40 |

## Monitoring

```bash
# Latest grid summary (all slots, sorted by PnL)
tail -140 logs/grid/summary.log

# Best performer
grep "^Best:" logs/grid/summary.log | tail -1

# Total fills
grep -c "FILL" logs/grid_debug.log

# Errors
grep "ERROR" logs/grid_debug.log | tail -10

# Process alive?
ps aux | grep grid

# Per-slot trade history
cat logs/grid/trades_BTC_v24_m4_s0.1_f2.0_c0.12_l2_c120.0.csv

# Final results CSV (written on shutdown)
ls logs/grid/results_BTC_*.csv
```

## How it works

- **1 shared WS feed** for orderbook + ticker + Binance alpha (regardless of slot count)
- **N independent simulations**, each with own wallet, position, PnL, order tracking
- Fill simulation uses delta-fill against the live orderbook with simulated latency
- State is **persisted by parameter values** — if you change the grid config and restart, overlapping parameter combinations resume from where they left off; new combos start fresh
- Resource usage: ~2MB RAM per 64 slots, ~2% CPU — runs fine on a $5 VPS

## Persistence

State files are saved every 60 seconds and on shutdown to `logs/grid/`:
- `state_BTC_{param_key}.json` — wallet, position, PnL per slot
- `trades_BTC_{param_key}.csv` — append-only fill log per slot
- `results_BTC_{timestamp}.csv` — summary CSV written on shutdown

On restart, slots automatically restore from their state files. You can safely kill and restart the process without losing data.

## Deploy to VPS

```bash
# From your local machine
python deploy.py --host YOUR_VPS_IP --key path/to/key.pem

# Then on the VPS
cd /home/ubuntu/lighter_MM
pip install -r requirements.txt
python setup_cython.py build_ext --inplace
python -u market_maker_v2.py --symbol BTC --grid grid_config.json
```
