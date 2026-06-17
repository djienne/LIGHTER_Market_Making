# Cartea-Jaimungal Market Making Plan

This plan upgrades the Lighter market-making stack from a Vol+OBI spread heuristic toward a more robust Cartea-Jaimungal style control system.

The objective is not to replace the current live bot in one jump. The objective is to add a second quote engine, run it as paper challengers, compare it against the current `vol_obi` champion, then promote only after live-like evidence.

## Principles

- Keep Lighter direct API/WebSocket execution. Do not move to Freqtrade for live execution.
- Reuse the useful ideas from the Cartea-Jaimungal Freqtrade project: dynamic `kappa`, `lambda`, `epsilon`, HJB inventory-aware spreads, toxicity gates, replay/canary evidence.
- Treat the theoretical model as a quote engine, not a complete production system.
- Fail closed when parameters are stale, toxic, statistically weak, or not maker-safe.
- Promote by evidence: paper challenger -> canary live -> capital scale.

## Target Architecture

1. Collector
   - Capture Lighter BBO/order book snapshots, trades, fills, and local quote decisions.
   - Persist short rolling windows for estimator use and longer compressed windows for replay.

2. Parameter Estimator
   - Estimate `lambda+ / lambda-`: market-order arrival intensity by side.
   - Estimate `kappa+ / kappa-`: fill-depth sensitivity from market-order depth survival.
   - Estimate `epsilon+ / epsilon-`: adverse-selection cost from post-fill markouts.
   - Estimate `sigma2_per_sec`: realized mid-price variance.
   - Smooth parameters with an EMA and publish one atomic snapshot.

3. Quote Engines
   - `vol_obi`: current champion.
   - `cartea_jaimungal`: HJB/inventory-aware challenger.
   - Both engines expose the same interface: `on_book_update`, `quote`, `warmed_up`, `reset`.

4. Paper Runner
   - Run many independent challenger slots against one shared Lighter market feed.
   - Persist per-slot wallet, fills, inventory, PnL, spread capture, and markout metrics.

5. Selection Gates
   - Reject winners with low fill count, positive PnL only from directional inventory, high adverse-selection markout, or excessive inventory time at boundary.
   - Require consistency across sub-windows: first half, second half, volatile windows, quiet windows.
   - Winner score should combine PnL, fill quality, maker ratio, inventory stability, and robustness.

6. Promotion
   - Canary live with small capital.
   - Require zero taker fills unless explicitly allowed, sane fees, bounded inventory, and quote/fill behavior close to paper evidence.
   - Scale only after the canary evidence is clean.

## Phase 1 - Static Cartea-Jaimungal Challenger

Add a static-parameter HJB quote engine so we can test the mechanics without waiting for the full estimator.

Inputs:
- `kappa`
- `lambda`
- `epsilon`
- inventory penalties `alpha` and `phi`
- spread clamp bps
- maker fee cushion
- inventory unit size

Outputs:
- bid price
- ask price
- no quote on the side that would breach inventory bounds
- no quote if `max(kappa * epsilon)` exceeds toxicity threshold

Acceptance:
- Engine unit tests pass.
- Grid paper runner can run `quote_engine=cartea_jaimungal` without touching live orders.
- Summary clearly separates `vol_obi` and `cartea_jaimungal` slots.

## Phase 2 - Lighter Parameter Estimator

Build estimators from Lighter market data.

Estimator details:
- Aggregate trade prints into market-order events by timestamp/side.
- Attach the latest pre-trade BBO mid.
- Convert trade walk depth into distance from mid.
- Fit survival curve `P(depth >= delta) ~= exp(-kappa * delta)`.
- Estimate arrival rate as events per second by side.
- Estimate epsilon from post-event markout over 100 ms, 1 s, 5 s, 30 s.
- Publish diagnostics: R2, points, event count, depth p95, stale age.

Minimum gates:
- `n_points_plus/minus >= 6`
- `r2_plus/minus >= 0.30`
- enough buy/sell events for epsilon
- snapshot age below configured threshold
- `max(kappa * epsilon)` below toxicity threshold

Implementation status:
- `lighter_estimators.py` now subscribes through the grid runner to Lighter public trades.
- `lambda+ / lambda-` are estimated from rolling taker buy/sell event rates.
- `kappa+ / kappa-` are fitted from the empirical survival curve of trade depth versus the pre-trade mid: `lambda(delta) ~= A exp(-kappa * delta)`.
- Each snapshot exposes `kappa_r2_plus/minus`, fit point counts, and an aggregate `kappa_fit_quality`; weak fits fall back to conservative defaults instead of pushing unstable quote depths.
- `epsilon+ / epsilon-` are estimated from rolling adverse markouts.
- `sigma2_per_sec` is estimated from realized mid-price variance and can add a volatility cushion to CJ quote depths.
- Dynamic CJ grid variants test multipliers around the live estimate instead of only static absolute values.

## Phase 3 - Replay And Paper Scoring

Replay/paper metrics to persist:
- realized PnL
- mark-to-market PnL
- fees
- maker/taker classification when available
- spread capture bps
- markout bps at 100 ms, 1 s, 5 s, 30 s
- time at max inventory
- stale quote cancels
- fill count by side/depth bucket
- adverse selection score

Implementation status:
- `DryRunEngine` persists spread capture, markout averages, adverse markout averages, maker/taker counts, inventory time, inventory boundary ratio, and max inventory seen.
- `check_grid_results.py` computes a risk-aware `quality_score` and delays engine comparison until at least 24h and 72h evidence windows are available.
- CJ slots persist estimator fit diagnostics. Low `kappa_fit_quality` penalizes the score so a strategy cannot win solely because the live estimator was noisy.

Winner score:

```text
score =
  pnl_score
  + spread_capture_score
  + fill_count_score
  - adverse_selection_penalty
  - inventory_boundary_penalty
  - stale_cancel_penalty
  - parameter_instability_penalty
```

Do not promote a raw PnL winner if it wins mainly by holding directional inventory.

## Phase 4 - Canary Live

Promotion requirements:
- paper run >= 72h or enough fills
- top candidate stable across sub-windows
- no toxicity gate failures
- acceptable markout
- no inventory boundary abuse
- live canary with tiny capital

Canary must validate:
- orders are maker/post-only or safely non-crossing
- no repeated stale create/drop loops
- no unexpected taker fills
- live fill rate close enough to paper expectation
- realized PnL not explained by accidental directional exposure

## Phase 5 - Continuous Improvement

Run recurring cycles:
- weekly parameter review
- monthly candidate sweep per major pair
- challenger against champion, not blind replacement
- revalidate champion after regime shifts

Pairs should be added only when the estimator says the market is non-toxic enough, not simply because volume is high.

## First Implementation Target

This repo should first add:
- `cartea_jaimungal.py`
- `tests/test_cartea_jaimungal.py`
- `configs/grid_btc_cj.json`
- grid runner support for `quote_engine=cartea_jaimungal`

After that, run a separate 72h BTC paper sweep with:

```bash
LOG_DIR=logs/grid_cj GRID_CONFIG=configs/grid_btc_cj.json bash run_grid_3d.sh
```

This keeps the existing `vol_obi` live service and existing `vol_obi` paper sweep untouched.
