# Spread Calculation

This document explains how the market making bot calculates spreads based on volatility and the key parameters: `vol_to_half_spread`, `c1_ticks`, and `skew`.

## Overview

The bot uses an Order Book Imbalance (OBI) strategy that generates quotes based on:
1. **Volatility** - rolling standard deviation of price changes
2. **Alpha** - z-score of order book imbalance (buy vs sell pressure)
3. **Position** - current inventory to manage

## Volatility Calculation

Volatility is calculated as a rolling standard deviation of mid-price changes, scaled to per-second:

```
vol_raw = std(mid_price_changes) over window_steps
volatility = vol_raw * sqrt(1e9 / step_ns)
```

With default `step_ns = 100ms` and `window_steps = 6000`:
- Window covers 10 minutes of data
- `vol_scale = sqrt(10) ≈ 3.16`

**Example:** If XAU mid-price has std of ~1.75 per 100ms step:
- `volatility = 1.75 * 3.16 ≈ 5.5` (dollars per second)

## Half-Spread Calculation

The base half-spread in price terms is:

```
half_spread_price = volatility * vol_to_half_spread
```

This is then converted to ticks:
```
half_spread_tick = half_spread_price / tick_size
```

**Example for XAU-USD:**
- `volatility = 8.0` (dollars/sec)
- `vol_to_half_spread = 1.0`
- `half_spread_price = 8.0 * 1.0 = $8.00`
- `half_spread_bps = 8.0 / 5400 * 10000 ≈ 14.8 bps`

| vol_to_half_spread | Half spread (at vol=8) | Full spread |
|--------------------|------------------------|-------------|
| 0.5 | ~7.4 bps | ~15 bps |
| 1.0 | ~14.8 bps | ~30 bps |
| 2.0 | ~29.6 bps | ~59 bps |

## Fair Price and c1_ticks

The bot adjusts the mid-price to a "fair price" based on order book imbalance:

```
c1 = c1_ticks * tick_size
fair_price = mid_price + c1 * alpha
```

Where `alpha` is the z-score of order book imbalance (typically -3 to +3):
- `alpha > 0`: More buy pressure → fair price above mid
- `alpha < 0`: More sell pressure → fair price below mid

**Example for XAU-USD:**
- `c1_ticks = 30`, `tick_size = 0.1`
- `c1 = 30 * 0.1 = $3.00`
- At `alpha = -1.0`: `fair_price = mid - 3.00`
- In bps: `3.00 / 5400 * 10000 ≈ 5.5 bps` shift per unit alpha

| c1_ticks | c1 ($) | Impact per unit alpha |
|----------|--------|----------------------|
| 30 | $3.00 | ~5.5 bps |
| 60 | $6.00 | ~11 bps |
| 100 | $10.00 | ~18.5 bps |

## Skew and Position Management

Skew adjusts the spread asymmetrically based on current position:

```
normalized_position = (position * mid_price) / max_position_dollar
clamped_position = clamp(normalized_position, -1, 1)

bid_depth = half_spread * (1 + skew * clamped_position)
ask_depth = half_spread * (1 - skew * clamped_position)
```

**When long (positive position):**
- Bid depth increases (pushed further from fair)
- Ask depth decreases (pulled closer to fair)
- Effect: More likely to sell, reduce long position

**When short (negative position):**
- Bid depth decreases (pulled closer to fair)
- Ask depth increases (pushed further from fair)
- Effect: More likely to buy, reduce short position

### Skew Sensitivity

The position at which one side goes to zero (one-sided quoting):
```
one_sided_at = 1 / skew
```

| skew | One-sided at | Behavior |
|------|--------------|----------|
| 0.5 | 200% (never) | Both sides always quoted |
| 1.0 | 100% | One-sided only at max position |
| 2.0 | 50% | One-sided at half max position |
| 3.5 | 28.6% | Very aggressive, one-sided early |

**Example at 20% position:**

| skew | Bid depth | Ask depth | Asymmetry |
|------|-----------|-----------|-----------|
| 0.5 | 1.1x base | 0.9x base | ±10% |
| 1.0 | 1.2x base | 0.8x base | ±20% |
| 3.5 | 1.7x base | 0.3x base | ±70% |

## Final Quote Prices

Putting it all together:

```
fair_price = mid_price + c1 * alpha

bid_price = fair_price - bid_depth * tick_size
ask_price = fair_price + ask_depth * tick_size

# Clamped to not cross the BBO
bid_price = min(bid_price, best_bid)
ask_price = max(ask_price, best_ask)
```

## Multi-Level Quotes

When `order_levels = 2`, the bot places orders at two price levels:
- **L0**: Base spread (1.0x multiplier)
- **L1**: Wider spread (spread_level_multiplier, default 2.0x)

## Summary of Parameters

| Parameter | Description | Typical Value |
|-----------|-------------|---------------|
| `vol_to_half_spread` | Volatility to spread multiplier | 1.0 (gives ~10-15 bps) |
| `c1_ticks` | Alpha response in ticks | 30-100 (5-18 bps/alpha) |
| `skew` | Position-based asymmetry | 0.5-1.0 (conservative) |
| `min_half_spread_bps` | Floor for half-spread | 1.0-2.0 bps |
| `spread_level_multiplier` | L1 vs L0 spread ratio | 2.0 |
