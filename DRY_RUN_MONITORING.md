> **Note**: This is a historical log from the initial dry-run session (March 2026). For current monitoring, see `run_grid_3d.sh` and `GRID_QUICKSTART.md`.

# Dry-Run Monitoring Log

## Session Info
- **Started**: 2026-03-27 09:41 UTC
- **Symbol**: BTC
- **Initial Capital**: $1,000
- **Leverage**: 2x
- **Mode**: Dry-run (paper trading)
- **Monitor**: hourly checks for 3 days (72 checks)

## Bug Found & Fixed

### 1. `load_state` crash on session resume (CRITICAL)
- **File**: `market_maker_v2.py:3518` + `dry_run.py:190`
- **Symptom**: Bot crashes immediately on every restart when `dry_run_state.json` exists (i.e. every resume after first run).
- **Root cause**: `DryRunEngine.load_state()` received `state_path` twice — once as positional arg `path` (line 3511) and again as `state_path=_dr_state_path` in kwargs (line 3518). Inside `load_state`, line 196 sets `state_path=path`, then `**kwargs` also contains `state_path` — Python raises `TypeError: got multiple values for keyword argument 'state_path'`.
- **Fix**: Removed the duplicate `state_path=_dr_state_path` from the call at line 3518. `load_state` already forwards `path` as `state_path` internally.
- **Impact**: Without this fix, dry-run can never resume from saved state — it crashes on every restart unless `--capital` is passed to reset.

### 2. `$None` capital logged during state restore (cosmetic)
- **File**: `market_maker_v2.py:3452-3454`
- **Symptom**: Log shows `Received valid account capital: $None` when resuming from saved state.
- **Root cause**: When a saved state file exists but `--capital` is not passed, the code at line 3452 just logs "found saved state" without setting `state.account.available_capital`. The actual restore happens later in `DryRunEngine.load_state()` (~line 3510), but the log at line 3495 fires in between while capital is still `None`.
- **Fix**: Pre-load capital/portfolio_value/position from the JSON at line 3452 so the values are available before the log statement.

## Monitoring Checks

### Check 1 — 2026-03-27 11:25 (T+1h44m)
- Status: ALIVE
- Capital: $1,000.00 | Position: 0.0 | PnL: $0.00
- Fills: 0 | Volume: $0.00
- Errors: none
- Quoting: 2-sided, ~0.20% spread, BTC mid ~$67,693
- Notes: No fills yet — BTC ranging tightly ($67,700-$68,000). Spread ~$270. Need ~0.2% move to trigger fill. Normal for low-vol window.

---
*Subsequent checks auto-logged to `logs/monitor.log` by `monitor_dryrun.sh`*
*I will update this file with significant findings (fills, errors, restarts, PnL milestones)*
