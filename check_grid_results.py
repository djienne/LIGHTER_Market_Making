#!/usr/bin/env python3
"""Scan grid dry-run results and print summary statistics.

Usage:
    python check_grid_results.py              # scan logs/grid/
    python check_grid_results.py /path/to/grid/  # custom directory
    python check_grid_results.py --top 20     # show top 20 (default 10)
    python check_grid_results.py --sort fills # sort by fills instead of total_pnl
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone


def parse_float_suffix(token: str, prefix: str) -> float | None:
    if not token.startswith(prefix):
        return None
    try:
        return float(token[len(prefix):])
    except ValueError:
        return None


def split_symbol_param(parts: str) -> tuple[str, str]:
    """Split filename payload into symbol and param key.

    OBI keys start with ``v...`` while Cartea-Jaimungal keys start with
    ``cj_...``.  Keeping this centralized prevents the result scanner from
    silently grouping CJ files under a fake symbol.
    """
    for marker in ("_v", "_cj_"):
        idx = parts.find(marker)
        if idx >= 0:
            return parts[:idx], parts[idx + 1:]
    if "_" in parts:
        return parts.split("_", 1)
    return "?", parts


def parse_param_key(pk: str) -> dict:
    """Extract parameter values from a param_key string like 'v8_m4_s0.1_f2.0_c0.12_l2_c120.0'."""
    params = {"quote_engine": "CJ" if pk.startswith("cj_") else "OBI"}
    for token in pk.split("_"):
        if (value := parse_float_suffix(token, "lp")) is not None:
            params["lambda_plus"] = value
        elif (value := parse_float_suffix(token, "lm")) is not None:
            params["lambda_minus"] = value
        elif (value := parse_float_suffix(token, "kp")) is not None:
            params["kappa_plus"] = value
        elif (value := parse_float_suffix(token, "km")) is not None:
            params["kappa_minus"] = value
        elif (value := parse_float_suffix(token, "ep")) is not None:
            params["epsilon_plus"] = value
        elif (value := parse_float_suffix(token, "em")) is not None:
            params["epsilon_minus"] = value
        elif (value := parse_float_suffix(token, "ph")) is not None:
            params["phi"] = value
        elif (value := parse_float_suffix(token, "sm")) is not None:
            params["spread_multiplier"] = value
        elif (value := parse_float_suffix(token, "vs")) is not None:
            params["volatility_spread_multiplier"] = value
        elif (value := parse_float_suffix(token, "ls")) is not None:
            params["lambda_scale"] = value
        elif (value := parse_float_suffix(token, "ks")) is not None:
            params["kappa_scale"] = value
        elif (value := parse_float_suffix(token, "es")) is not None:
            params["epsilon_scale"] = value
        elif (value := parse_float_suffix(token, "ss")) is not None:
            params["sigma2_scale"] = value
        elif (value := parse_float_suffix(token, "v")) is not None:
            params["vol_to_half_spread"] = value
        elif (value := parse_float_suffix(token, "m")) is not None:
            params["min_half_spread_bps"] = value
        elif (value := parse_float_suffix(token, "s")) is not None:
            params["skew"] = value
        elif (value := parse_float_suffix(token, "f")) is not None:
            params["spread_factor"] = value
        elif token.startswith("l") and token[1:].isdigit():
            params["num_levels"] = int(token[1:])
    return params


def load_state_files(grid_dir: str) -> list[dict]:
    """Load all state JSON files and return list of slot data."""
    slots = []
    for fname in sorted(os.listdir(grid_dir)):
        if not fname.startswith("state_") or not fname.endswith(".json"):
            continue
        path = os.path.join(grid_dir, fname)
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        # Extract param_key from filename: state_BTC_<param_key>.json
        parts = fname[len("state_"):-len(".json")]
        symbol, pk = split_symbol_param(parts)

        params = parse_param_key(pk)
        data["param_key"] = pk
        data["symbol"] = symbol
        data["params"] = params
        data["file"] = fname
        slots.append(data)
    return slots


def load_trade_counts(grid_dir: str) -> dict[str, int]:
    """Count trade rows per param_key from CSV files."""
    counts = {}
    for fname in os.listdir(grid_dir):
        if not fname.startswith("trades_") or not fname.endswith(".csv"):
            continue
        path = os.path.join(grid_dir, fname)
        try:
            with open(path) as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                n = sum(1 for _ in reader)
            # Extract param_key
            parts = fname[len("trades_"):-len(".csv")]
            _, pk = split_symbol_param(parts)
            counts[pk] = n
        except OSError:
            continue
    return counts


def format_usd(val: float) -> str:
    return f"${val:>+9.4f}"


def parse_dt(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def quality_score(slot: dict) -> float:
    fills = slot.get("fill_count", 0)
    if fills <= 0:
        return -999.0
    total_pnl = slot.get("total_pnl", 0.0)
    volume = slot.get("total_volume", 0.0)
    pnl_bps = (total_pnl / volume * 10_000.0) if volume > 0 else 0.0
    spread = float(slot.get("spread_capture_bps_avg", 0.0) or 0.0)
    adverse = slot.get("adverse_markout_bps_avg", {}) or {}
    adverse_5s = float(adverse.get("5.0", adverse.get("5", 0.0)) or 0.0)
    boundary = float(slot.get("inventory_boundary_ratio", 0.0) or 0.0)
    fit_quality = slot.get("cj_kappa_fit_quality")
    fill_bonus = min(__import__("math").log1p(fills), 5.0)
    fit_penalty = 0.0
    if fit_quality is not None:
        try:
            fit_penalty = max(0.0, 0.30 - float(fit_quality)) * 15.0
        except (TypeError, ValueError):
            fit_penalty = 0.0
    return pnl_bps + 0.25 * spread + fill_bonus - 0.75 * adverse_5s - 25.0 * boundary - fit_penalty


def print_summary(slots: list[dict], trade_counts: dict, top_n: int, sort_key: str, maker_fee_rate: float):
    if not slots:
        print("No grid state files found.")
        return

    # Compute derived fields
    for s in slots:
        s["total_pnl"] = s.get("realized_pnl", 0) + (
            s.get("portfolio_value", 0) - s.get("initial_portfolio_value", 0) - s.get("realized_pnl", 0)
        )
        s["unrealized_pnl"] = s.get("portfolio_value", 0) - s.get("initial_portfolio_value", 0) - s.get("realized_pnl", 0)
        s["trade_count"] = trade_counts.get(s["param_key"], 0)
        fills = s.get("fill_count", 0)
        s["pnl_per_fill"] = s["total_pnl"] / fills if fills > 0 else 0
        vol = s.get("total_volume", 0)
        s["pnl_per_volume"] = (s["total_pnl"] / vol * 10000) if vol > 0 else 0  # bps
        s["fees_paid"] = vol * maker_fee_rate
        s["quality_score"] = quality_score(s)
        started = parse_dt(s.get("started_at", ""))
        updated = parse_dt(s.get("updated_at", ""))
        if started and updated:
            s["age_hours"] = max((updated - started).total_seconds() / 3600.0, 0.0)
        else:
            s["age_hours"] = 0.0

    # Sort
    sort_map = {
        "total_pnl": lambda s: s["total_pnl"],
        "pnl": lambda s: s["total_pnl"],
        "fills": lambda s: s.get("fill_count", 0),
        "volume": lambda s: s.get("total_volume", 0),
        "realized": lambda s: s.get("realized_pnl", 0),
        "pnl_per_fill": lambda s: s["pnl_per_fill"],
        "efficiency": lambda s: s["pnl_per_volume"],
        "score": lambda s: s["quality_score"],
    }
    key_fn = sort_map.get(sort_key, sort_map["total_pnl"])
    slots.sort(key=key_fn, reverse=True)

    # Overall stats
    total_slots = len(slots)
    active_slots = sum(1 for s in slots if s.get("fill_count", 0) > 0)
    total_fills = sum(s.get("fill_count", 0) for s in slots)
    total_volume = sum(s.get("total_volume", 0) for s in slots)
    total_realized = sum(s.get("realized_pnl", 0) for s in slots)
    profitable = sum(1 for s in slots if s["total_pnl"] > 0)
    losing = sum(1 for s in slots if s["total_pnl"] < 0)
    flat = total_slots - profitable - losing

    # Time range
    timestamps = []
    for s in slots:
        ts = s.get("updated_at", "")
        if ts:
            try:
                timestamps.append(datetime.fromisoformat(ts))
            except ValueError:
                pass
    latest = max(timestamps) if timestamps else None

    print("=" * 90)
    print("GRID DRY-RUN RESULTS SUMMARY")
    print("=" * 90)
    print(f"  Slots: {total_slots} total, {active_slots} with fills, {total_slots - active_slots} idle")
    print(f"  Fills: {total_fills:,}")
    print(f"  Volume: ${total_volume:,.2f}")
    print(f"  Maker fee: {maker_fee_rate*100:.4f}%")
    print(f"  Profitable: {profitable} | Losing: {losing} | Flat: {flat}")
    max_age = max((s.get("age_hours", 0.0) for s in slots), default=0.0)
    print(f"  Max evidence age: {max_age:.1f}h | comparison gate: {'OPEN' if max_age >= 24 else 'WAIT >=24h'}")
    if latest:
        age = datetime.now(timezone.utc) - latest
        print(f"  Last update: {latest.strftime('%Y-%m-%d %H:%M:%S UTC')} ({age.total_seconds()/3600:.1f}h ago)")
    print()

    # Top N
    print(f"TOP {top_n} (sorted by {sort_key}):")
    print(f"{'#':>3} {'eng':>3} {'shape':>7} {'sk/eps':>7} {'Fills':>6} {'Total':>10} {'Score':>8} {'Fit':>5} {'Sprd':>7} {'Adv5':>7} {'Age':>5} {'Volume':>10}")
    print("-" * 110)
    for i, s in enumerate(slots[:top_n]):
        p = s.get("params", {})
        engine = p.get("quote_engine", "?")
        shape = p.get("kappa_plus", p.get("vol_to_half_spread", "?"))
        skew_or_eps = p.get("epsilon_plus", p.get("skew", "?"))
        adverse = s.get("adverse_markout_bps_avg", {}) or {}
        adv5 = adverse.get("5.0", adverse.get("5", 0.0))
        fit = s.get("cj_kappa_fit_quality", "")
        fit_text = f"{float(fit):.2f}" if isinstance(fit, (int, float)) else "n/a"
        print(
            f"{i+1:>3} {engine:>3} {shape:>7} {skew_or_eps:>7} "
            f"{s.get('fill_count', 0):>6} "
            f"{format_usd(s['total_pnl'])} "
            f"{s['quality_score']:>8.2f} "
            f"{fit_text:>5} "
            f"{float(s.get('spread_capture_bps_avg', 0.0) or 0.0):>7.2f} "
            f"{float(adv5 or 0.0):>7.2f} "
            f"{s.get('age_hours', 0.0):>4.1f}h "
            f"${s.get('total_volume', 0):>9.2f}"
        )

    print()

    # Bottom N
    bottom = list(reversed(slots[-min(top_n, len(slots)):]))
    print(f"BOTTOM {len(bottom)} (worst performers):")
    print(f"{'#':>3} {'eng':>3} {'shape':>7} {'sk/eps':>7} {'Fills':>6} {'Total':>10} {'Score':>8} {'Fit':>5} {'Sprd':>7} {'Adv5':>7} {'Age':>5} {'Volume':>10}")
    print("-" * 110)
    for i, s in enumerate(bottom):
        p = s.get("params", {})
        engine = p.get("quote_engine", "?")
        shape = p.get("kappa_plus", p.get("vol_to_half_spread", "?"))
        skew_or_eps = p.get("epsilon_plus", p.get("skew", "?"))
        adverse = s.get("adverse_markout_bps_avg", {}) or {}
        adv5 = adverse.get("5.0", adverse.get("5", 0.0))
        fit = s.get("cj_kappa_fit_quality", "")
        fit_text = f"{float(fit):.2f}" if isinstance(fit, (int, float)) else "n/a"
        print(
            f"{i+1:>3} {engine:>3} {shape:>7} {skew_or_eps:>7} "
            f"{s.get('fill_count', 0):>6} "
            f"{format_usd(s['total_pnl'])} "
            f"{s['quality_score']:>8.2f} "
            f"{fit_text:>5} "
            f"{float(s.get('spread_capture_bps_avg', 0.0) or 0.0):>7.2f} "
            f"{float(adv5 or 0.0):>7.2f} "
            f"{s.get('age_hours', 0.0):>4.1f}h "
            f"${s.get('total_volume', 0):>9.2f}"
        )

    print()

    # Parameter analysis: average PnL by each axis
    print("PARAMETER ANALYSIS (average total PnL by axis):")
    print()

    has_cj = any(s.get("params", {}).get("quote_engine") == "CJ" for s in slots)
    axis_names = (
        [
            "lambda_scale", "kappa_scale", "epsilon_scale", "sigma2_scale",
            "spread_multiplier", "volatility_spread_multiplier",
            "kappa_plus", "epsilon_plus", "phi",
        ]
        if has_cj
        else ["vol_to_half_spread", "skew"]
    )

    for param_name in axis_names:
        by_val = defaultdict(list)
        for s in slots:
            val = s.get("params", {}).get(param_name)
            if val is not None:
                by_val[val].append(s["total_pnl"])

        if not by_val:
            continue

        print(f"  {param_name}:")
        for val in sorted(by_val.keys()):
            pnls = by_val[val]
            avg = sum(pnls) / len(pnls)
            best = max(pnls)
            worst = min(pnls)
            pos = sum(1 for p in pnls if p > 0)
            print(
                f"    {val:>6} | avg={format_usd(avg)} | best={format_usd(best)} | "
                f"worst={format_usd(worst)} | {pos}/{len(pnls)} profitable"
            )
        print()

    print("ENGINE COMPARISON GATE:")
    for min_age in (24.0, 72.0):
        mature = [s for s in slots if s.get("age_hours", 0.0) >= min_age and s.get("fill_count", 0) > 0]
        if not mature:
            print(f"  >= {min_age:.0f}h: not enough evidence yet")
            continue
        by_engine = defaultdict(list)
        for s in mature:
            by_engine[s.get("params", {}).get("quote_engine", "?")].append(s)
        print(f"  >= {min_age:.0f}h:")
        for engine, group in sorted(by_engine.items()):
            best = max(group, key=lambda s: s["quality_score"])
            avg_score = sum(s["quality_score"] for s in group) / len(group)
            avg_pnl = sum(s["total_pnl"] for s in group) / len(group)
            print(
                f"    {engine}: slots={len(group)} avg_score={avg_score:.2f} "
                f"avg_pnl={format_usd(avg_pnl)} best={best.get('file')} score={best['quality_score']:.2f}"
            )
    print()

    # Heatmap (if both axes present)
    v2hs_vals = sorted(set(s.get("params", {}).get("vol_to_half_spread") for s in slots if s.get("params", {}).get("vol_to_half_spread") is not None))
    skew_vals = sorted(set(s.get("params", {}).get("skew") for s in slots if s.get("params", {}).get("skew") is not None))

    if len(v2hs_vals) > 1 and len(skew_vals) > 1:
        # Build lookup
        lookup = {}
        for s in slots:
            p = s.get("params", {})
            v = p.get("vol_to_half_spread")
            sk = p.get("skew")
            if v is not None and sk is not None:
                lookup[(v, sk)] = s["total_pnl"]

        print("PnL HEATMAP (vol_to_half_spread x skew):")
        label = "v2hs\\skew"
        header = f"{label:>10}"
        for sk in skew_vals:
            header += f" {sk:>7}"
        print(header)
        print("-" * (10 + 8 * len(skew_vals)))
        for v in v2hs_vals:
            row = f"{v:>10}"
            for sk in skew_vals:
                pnl = lookup.get((v, sk))
                if pnl is not None:
                    if pnl > 0.5:
                        row += f" {pnl:>+7.2f}"
                    elif pnl < -0.5:
                        row += f" {pnl:>+7.2f}"
                    else:
                        row += f" {pnl:>+7.2f}"
                else:
                    row += "       -"
            print(row)
        print()


def main():
    parser = argparse.ArgumentParser(description="Check grid dry-run results")
    parser.add_argument("grid_dir", nargs="?", default="logs/grid",
                        help="Grid output directory (default: logs/grid)")
    parser.add_argument("--top", type=int, default=10, help="Number of top/bottom slots to show")
    parser.add_argument("--sort", default="total_pnl",
                        choices=["total_pnl", "pnl", "fills", "volume", "realized", "pnl_per_fill", "efficiency", "score"],
                        help="Sort key (default: total_pnl)")
    parser.add_argument("--fee", type=float, default=0.000_04,
                        help="Maker fee rate as fraction (default: 0.00004 = 0.004%%)")
    args = parser.parse_args()

    if not os.path.isdir(args.grid_dir):
        print(f"Directory not found: {args.grid_dir}")
        sys.exit(1)

    slots = load_state_files(args.grid_dir)
    trade_counts = load_trade_counts(args.grid_dir)
    print_summary(slots, trade_counts, args.top, args.sort, args.fee)


if __name__ == "__main__":
    main()
