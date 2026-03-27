# Dry-Run / Paper-Trading Mode

## Overview

The market maker supports a **dry-run mode** that simulates order fills against the live orderbook without sending any transactions to the exchange. All decision logic (vol_obi spread model, position skew, dynamic sizing, Binance alpha) runs identically to live mode, but orders are tracked and filled locally.

Dry-run is the **default mode**. Use `--live` to trade with real money.

## Usage

```bash
# Paper trading (default)
python -u market_maker_v2.py --symbol BTC

# Real trading
python -u market_maker_v2.py --symbol BTC --live
```

## How It Works

### Architecture

The dry-run engine (`dry_run.py`) replaces the exchange interaction layer at two points:

1. **`process_batch()`** replaces `sign_and_send_batch()` — instead of signing and sending operations to the exchange, orders are stored locally in a `SimulatedOrder` dict. Create, modify, and cancel operations are handled instantly without nonces, signatures, or network calls.

2. **`check_fills()`** is called on every WebSocket orderbook update (~100ms) — it walks the live orderbook to determine if any simulated limit orders would be filled at their price.

### Fill Simulation

Fill logic follows POST_ONLY maker semantics:

- **Buy limit at price P** fills when the best ask <= P. The engine walks ask levels up to P, sums available liquidity, and fills `min(order_size, available_liquidity)`.
- **Sell limit at price P** fills when the best bid >= P. Same logic on the bid side.
- **Fill price** is always the limit price (maker gets their specified price).
- **Partial fills** occur naturally when available liquidity is less than order size.

### Simulated Latency

Order operations simulate exchange latency (default **50ms**, tunable):

- **Create/Modify**: order is not eligible for fills until `eligible_at = now + latency`. This simulates the order being in-flight to the exchange.
- **Cancel**: order remains live and fillable during the latency window. After the delay, the order is removed. This simulates the real behavior where a cancel is in-transit and the order could still be hit.

To tune latency, modify the `sim_latency_s` parameter in `main()` where `DryRunEngine` is initialized:

```python
_dry_run_engine = DryRunEngine(
    ...
    sim_latency_s=0.050,  # 50ms default; set to 0 for instant fills
)
```

### State Tracking

The engine maintains simulated state that feeds back into the decision logic:

- **Position**: `state.account.position_size` is updated on each fill, so position skew and one-sided quoting work correctly.
- **Capital**: `state.account.available_capital` is adjusted by margin impact (using the configured leverage).
- **PnL**: average-cost-basis model tracking realized PnL, unrealized PnL (from current mid price), and total PnL.
- **Fills**: appended to `state.account.recent_trades` with a `"simulated": True` flag.

### What Is Active vs Skipped

| Component | Dry-Run | Live |
|-----------|---------|------|
| Orderbook WS | Active | Active |
| Ticker WS | Active | Active |
| User stats WS | Active (initial capital only) | Active |
| Account all WS | Active (position updates ignored) | Active |
| Account orders WS (authenticated) | Skipped | Active |
| Stale order reconciler | Skipped | Active |
| TxWebSocket | Skipped | Active |
| SignerClient / nonces | Not created | Active |
| Quota tracking | Skipped | Active |
| vol_obi spread model | Active (identical) | Active |
| Binance alpha | Active (identical) | Active |

### Logging

- Each create/modify/cancel/fill is logged with a `DRY-RUN` prefix.
- A periodic summary (every 60s) reports: position, entry VWAP, mid price, realized PnL, unrealized PnL, total PnL, fill count, total volume, and live order count.
- On shutdown, a final summary is printed.

## Known Limitations

### Queue Position Is Not Simulated

In a real orderbook, limit orders sit in a FIFO queue at their price level. Other orders placed earlier at the same price have priority. The simulation **does not model queue position** — it fills immediately when the price crosses, regardless of how much existing liquidity was ahead of us at that level.

This makes the simulation **optimistic**: in reality, our order might not fill even though the price touched our level, because orders ahead of us consumed the incoming liquidity first. This is an inherent limitation of paper trading without injecting orders into the real book, and is not planned to be addressed.

### No Liquidity Impact

Our simulated orders are not in the real orderbook, so we cannot model the effect our orders would have on other participants. For the small order sizes this bot uses (~$17 per side), this effect is negligible.

### Slightly Optimistic Timing

In live mode, there is network latency between sending an order and it appearing on the exchange. The simulated latency (`sim_latency_s`) approximates this, but the exact value may not match real-world conditions. The default 50ms is a reasonable estimate for the Lighter exchange.
