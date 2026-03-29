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


def parse_param_key(pk: str) -> dict:
    """Extract parameter values from a param_key string like 'v8_m4_s0.1_f2.0_c0.12_l2_c120.0'."""
    params = {}
    for token in pk.split("_"):
        if token.startswith("v") and token[1:].replace(".", "").isdigit():
            params["vol_to_half_spread"] = float(token[1:])
        elif token.startswith("m") and token[1:].replace(".", "").isdigit():
            params["min_half_spread_bps"] = float(token[1:])
        elif token.startswith("s") and token[1:].replace(".", "").isdigit():
            params["skew"] = float(token[1:])
        elif token.startswith("f") and token[1:].replace(".", "").isdigit():
            params["spread_factor"] = float(token[1:])
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
        # Remove symbol prefix (e.g. "BTC_")
        idx = parts.find("_v")
        if idx >= 0:
            symbol = parts[:idx]
            pk = parts[idx + 1:]
        else:
            symbol = "?"
            pk = parts

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
            idx = parts.find("_v")
            pk = parts[idx + 1:] if idx >= 0 else parts
            counts[pk] = n
        except OSError:
            continue
    return counts


def format_usd(val: float) -> str:
    if val >= 0:
        return f" ${val:>8.4f}"
    return f"-${abs(val):>8.4f}"


def print_summary(slots: list[dict], trade_counts: dict, top_n: int, sort_key: str):
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

    # Sort
    sort_map = {
        "total_pnl": lambda s: s["total_pnl"],
        "pnl": lambda s: s["total_pnl"],
        "fills": lambda s: s.get("fill_count", 0),
        "volume": lambda s: s.get("total_volume", 0),
        "realized": lambda s: s.get("realized_pnl", 0),
        "pnl_per_fill": lambda s: s["pnl_per_fill"],
        "efficiency": lambda s: s["pnl_per_volume"],
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
    print(f"  Realized PnL: ${total_realized:,.4f}")
    print(f"  Profitable: {profitable} | Losing: {losing} | Flat: {flat}")
    if latest:
        age = datetime.now(timezone.utc) - latest
        print(f"  Last update: {latest.strftime('%Y-%m-%d %H:%M:%S UTC')} ({age.total_seconds()/3600:.1f}h ago)")
    print()

    # Top N
    print(f"TOP {top_n} (sorted by {sort_key}):")
    print(f"{'#':>3} {'v2hs':>5} {'skew':>5} {'Fills':>6} {'Realized':>10} {'Unrealzd':>10} {'Total':>10} {'$/Fill':>8} {'bps/Vol':>8} {'Volume':>10}")
    print("-" * 90)
    for i, s in enumerate(slots[:top_n]):
        p = s.get("params", {})
        print(
            f"{i+1:>3} {p.get('vol_to_half_spread', '?'):>5} {p.get('skew', '?'):>5} "
            f"{s.get('fill_count', 0):>6} "
            f"{format_usd(s.get('realized_pnl', 0))} "
            f"{format_usd(s['unrealized_pnl'])} "
            f"{format_usd(s['total_pnl'])} "
            f"{s['pnl_per_fill']:>8.4f} "
            f"{s['pnl_per_volume']:>8.2f} "
            f"${s.get('total_volume', 0):>9.2f}"
        )

    print()

    # Bottom N
    bottom = list(reversed(slots[-min(top_n, len(slots)):]))
    print(f"BOTTOM {len(bottom)} (worst performers):")
    print(f"{'#':>3} {'v2hs':>5} {'skew':>5} {'Fills':>6} {'Realized':>10} {'Unrealzd':>10} {'Total':>10} {'$/Fill':>8} {'bps/Vol':>8} {'Volume':>10}")
    print("-" * 90)
    for i, s in enumerate(bottom):
        p = s.get("params", {})
        print(
            f"{i+1:>3} {p.get('vol_to_half_spread', '?'):>5} {p.get('skew', '?'):>5} "
            f"{s.get('fill_count', 0):>6} "
            f"{format_usd(s.get('realized_pnl', 0))} "
            f"{format_usd(s['unrealized_pnl'])} "
            f"{format_usd(s['total_pnl'])} "
            f"{s['pnl_per_fill']:>8.4f} "
            f"{s['pnl_per_volume']:>8.2f} "
            f"${s.get('total_volume', 0):>9.2f}"
        )

    print()

    # Parameter analysis: average PnL by each axis
    print("PARAMETER ANALYSIS (average total PnL by axis):")
    print()

    for param_name in ["vol_to_half_spread", "skew"]:
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
                        choices=["total_pnl", "pnl", "fills", "volume", "realized", "pnl_per_fill", "efficiency"],
                        help="Sort key (default: total_pnl)")
    args = parser.parse_args()

    if not os.path.isdir(args.grid_dir):
        print(f"Directory not found: {args.grid_dir}")
        sys.exit(1)

    slots = load_state_files(args.grid_dir)
    trade_counts = load_trade_counts(args.grid_dir)
    print_summary(slots, trade_counts, args.top, args.sort)


if __name__ == "__main__":
    main()
