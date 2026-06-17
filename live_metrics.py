"""Live market-making observability helpers.

This module intentionally avoids exchange calls.  It receives fills and mid
prices from ``market_maker_v2`` and persists three small artifacts:

- ``live_state_{symbol}.json``: durable local PnL/accounting state.
- ``markouts_{symbol}.csv``: post-fill markouts at configured horizons.
- ``live_metrics_{symbol}.json``: rolling score and defensive adjustments.
"""

from __future__ import annotations

import csv
import json
import math
import os
import tempfile
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _finite(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _atomic_json_write(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp, path)
    finally:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass


class LiveStateStore:
    """Tiny durable JSON store for live fill accounting across restarts."""

    def __init__(self, log_dir: str, symbol: str):
        os.makedirs(log_dir, exist_ok=True)
        self.path = os.path.join(log_dir, f"live_state_{symbol}.json")
        self.symbol = symbol

    def load(self) -> dict[str, Any]:
        try:
            with open(self.path) as f:
                payload = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def save(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["symbol"] = self.symbol
        payload["updated_at"] = _utc_now()
        _atomic_json_write(self.path, payload)


@dataclass(slots=True)
class FillObservation:
    fill_id: str
    recorded_at: float
    timestamp: str
    side: str
    price: float
    size: float
    mid_at_fill: Optional[float]
    spread_capture_bps: Optional[float]
    position_after: float
    realized_delta_usd: float
    realized_pnl_cumulative: Optional[float]
    fill_source: str
    client_order_index: Optional[int | str] = None
    exchange_order_index: Optional[int | str] = None
    settled_horizons: set[float] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class QualityAdjustment:
    spread_multiplier: float = 1.0
    size_multiplier: float = 1.0
    adverse_bps: float = 0.0
    sample_count: int = 0
    reason: str = "neutral"


class LiveMetricsTracker:
    """Tracks markouts, live scoring and defensive quote adjustments."""

    def __init__(
        self,
        log_dir: str,
        symbol: str,
        *,
        horizons: list[float] | tuple[float, ...] = (5.0, 30.0, 60.0),
        window_seconds: float = 3600.0,
        adaptive_enabled: bool = True,
        adaptive_horizon: float = 30.0,
        adverse_threshold_bps: float = 2.0,
        spread_widen_per_bps: float = 0.05,
        max_spread_multiplier: float = 1.5,
        size_reduce_per_bps: float = 0.06,
        min_size_multiplier: float = 0.55,
        metrics_flush_seconds: float = 10.0,
    ):
        os.makedirs(log_dir, exist_ok=True)
        self.symbol = symbol
        self.horizons = tuple(sorted(float(h) for h in horizons if float(h) > 0))
        self.window_seconds = max(float(window_seconds), max(self.horizons, default=1.0))
        self.adaptive_enabled = bool(adaptive_enabled)
        self.adaptive_horizon = float(adaptive_horizon)
        self.adverse_threshold_bps = float(adverse_threshold_bps)
        self.spread_widen_per_bps = max(float(spread_widen_per_bps), 0.0)
        self.max_spread_multiplier = max(float(max_spread_multiplier), 1.0)
        self.size_reduce_per_bps = max(float(size_reduce_per_bps), 0.0)
        self.min_size_multiplier = min(max(float(min_size_multiplier), 0.05), 1.0)
        self.metrics_flush_seconds = max(float(metrics_flush_seconds), 1.0)

        self.markout_path = os.path.join(log_dir, f"markouts_{symbol}.csv")
        self.metrics_path = os.path.join(log_dir, f"live_metrics_{symbol}.json")
        self._pending: deque[FillObservation] = deque(maxlen=2_000)
        self._markouts: dict[float, deque[tuple[float, float, float]]] = defaultdict(lambda: deque(maxlen=10_000))
        self._markouts_by_side: dict[float, dict[str, deque[tuple[float, float, float]]]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=10_000))
        )
        self._spread_capture: deque[tuple[float, float]] = deque(maxlen=10_000)
        self._fill_events: deque[tuple[float, str, float, float]] = deque(maxlen=10_000)
        self._inventory_samples: deque[tuple[float, float, float]] = deque(maxlen=20_000)
        self._last_metrics_flush = 0.0
        self._last_adjustment = QualityAdjustment()
        self._ensure_markout_header()

    def _ensure_markout_header(self) -> None:
        header = [
            "timestamp",
            "symbol",
            "fill_id",
            "horizon_sec",
            "side",
            "fill_price",
            "size",
            "notional_usd",
            "mid_at_fill",
            "mid_at_markout",
            "markout_bps",
            "adverse_bps",
            "spread_capture_bps",
            "position_after",
            "realized_delta_usd",
            "realized_pnl_cumulative",
            "fill_source",
            "client_order_index",
            "exchange_order_index",
        ]
        if os.path.exists(self.markout_path) and os.path.getsize(self.markout_path) > 0:
            return
        with open(self.markout_path, "w", newline="") as f:
            csv.writer(f).writerow(header)

    def record_fill(
        self,
        *,
        fill_id: str,
        side: str,
        price: float,
        size: float,
        mid_at_fill: Optional[float],
        spread_capture_bps: Optional[float],
        position_after: float,
        realized_delta_usd: float,
        realized_pnl_cumulative: Optional[float],
        fill_source: str,
        client_order_index: Optional[int | str] = None,
        exchange_order_index: Optional[int | str] = None,
    ) -> None:
        if side not in {"buy", "sell"} or price <= 0 or size <= 0:
            return
        now = time.monotonic()
        self._pending.append(
            FillObservation(
                fill_id=fill_id,
                recorded_at=now,
                timestamp=_utc_now(),
                side=side,
                price=float(price),
                size=float(size),
                mid_at_fill=_finite(mid_at_fill),
                spread_capture_bps=_finite(spread_capture_bps),
                position_after=float(position_after),
                realized_delta_usd=float(realized_delta_usd or 0.0),
                realized_pnl_cumulative=_finite(realized_pnl_cumulative),
                fill_source=fill_source or "",
                client_order_index=client_order_index,
                exchange_order_index=exchange_order_index,
            )
        )
        if spread_capture_bps is not None:
            spread = _finite(spread_capture_bps)
            if spread is not None:
                self._spread_capture.append((now, spread))
        self._fill_events.append((now, side, price, size))

    def update(
        self,
        *,
        mid_price: Optional[float],
        position_size: float,
        max_pos_usd: Optional[float],
        realized_pnl_cumulative: Optional[float],
        portfolio_value: Optional[float],
        available_capital: Optional[float],
    ) -> QualityAdjustment:
        now = time.monotonic()
        mid = _finite(mid_price)
        if mid is not None and mid > 0:
            max_pos = _finite(max_pos_usd) or 0.0
            pos_value = abs(float(position_size or 0.0)) * mid
            boundary_ratio = (pos_value / max_pos) if max_pos > 0 else 0.0
            self._inventory_samples.append((now, pos_value, max(0.0, boundary_ratio)))
            self._settle_markouts(now, mid)
        self._prune(now)
        self._last_adjustment = self._compute_adjustment()
        if now - self._last_metrics_flush >= self.metrics_flush_seconds:
            self.flush_metrics(
                mid_price=mid,
                position_size=position_size,
                max_pos_usd=max_pos_usd,
                realized_pnl_cumulative=realized_pnl_cumulative,
                portfolio_value=portfolio_value,
                available_capital=available_capital,
            )
        return self._last_adjustment

    def _settle_markouts(self, now: float, mid: float) -> None:
        rows = []
        keep: deque[FillObservation] = deque(maxlen=2_000)
        for obs in self._pending:
            for horizon in self.horizons:
                if horizon in obs.settled_horizons or now - obs.recorded_at < horizon:
                    continue
                if obs.side == "buy":
                    markout_bps = (mid - obs.price) / obs.price * 10_000.0
                else:
                    markout_bps = (obs.price - mid) / obs.price * 10_000.0
                adverse_bps = max(0.0, -markout_bps)
                obs.settled_horizons.add(horizon)
                self._markouts[horizon].append((now, markout_bps, adverse_bps))
                self._markouts_by_side[horizon][obs.side].append((now, markout_bps, adverse_bps))
                rows.append([
                    _utc_now(),
                    self.symbol,
                    obs.fill_id,
                    f"{horizon:.3f}",
                    obs.side,
                    f"{obs.price:.10g}",
                    f"{obs.size:.8f}",
                    f"{obs.price * obs.size:.8f}",
                    "" if obs.mid_at_fill is None else f"{obs.mid_at_fill:.10g}",
                    f"{mid:.10g}",
                    f"{markout_bps:.6f}",
                    f"{adverse_bps:.6f}",
                    "" if obs.spread_capture_bps is None else f"{obs.spread_capture_bps:.6f}",
                    f"{obs.position_after:.8f}",
                    f"{obs.realized_delta_usd:.8f}",
                    "" if obs.realized_pnl_cumulative is None else f"{obs.realized_pnl_cumulative:.8f}",
                    obs.fill_source,
                    "" if obs.client_order_index is None else str(obs.client_order_index),
                    "" if obs.exchange_order_index is None else str(obs.exchange_order_index),
                ])
            if len(obs.settled_horizons) < len(self.horizons):
                keep.append(obs)
        self._pending = keep
        if rows:
            with open(self.markout_path, "a", newline="") as f:
                csv.writer(f).writerows(rows)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._spread_capture and self._spread_capture[0][0] < cutoff:
            self._spread_capture.popleft()
        while self._fill_events and self._fill_events[0][0] < cutoff:
            self._fill_events.popleft()
        while self._inventory_samples and self._inventory_samples[0][0] < cutoff:
            self._inventory_samples.popleft()
        for samples in self._markouts.values():
            while samples and samples[0][0] < cutoff:
                samples.popleft()
        for side_samples in self._markouts_by_side.values():
            for samples in side_samples.values():
                while samples and samples[0][0] < cutoff:
                    samples.popleft()

    def _samples_for_horizon(self, horizon: Optional[float] = None) -> deque[tuple[float, float, float]]:
        if not self._markouts:
            return deque()
        if horizon is None:
            horizon = self.adaptive_horizon
        nearest = min(self._markouts.keys(), key=lambda h: abs(h - horizon))
        return self._markouts[nearest]

    def _compute_adjustment(self) -> QualityAdjustment:
        if not self.adaptive_enabled:
            return QualityAdjustment(reason="disabled")
        samples = self._samples_for_horizon(self.adaptive_horizon)
        if len(samples) < 4:
            return QualityAdjustment(sample_count=len(samples), reason="insufficient_markouts")
        adverse = mean(value[2] for value in samples)
        long_horizon = max(self.horizons, default=self.adaptive_horizon)
        if long_horizon != self.adaptive_horizon:
            long_samples = self._samples_for_horizon(long_horizon)
            if len(long_samples) >= 4:
                long_adverse = mean(value[2] for value in long_samples)
                adverse = max(adverse, long_adverse * 0.75)
        excess = max(0.0, adverse - self.adverse_threshold_bps)
        if excess <= 0:
            return QualityAdjustment(adverse_bps=adverse, sample_count=len(samples), reason="healthy")
        spread_multiplier = min(
            self.max_spread_multiplier,
            1.0 + excess * self.spread_widen_per_bps,
        )
        size_multiplier = max(
            self.min_size_multiplier,
            1.0 - excess * self.size_reduce_per_bps,
        )
        return QualityAdjustment(
            spread_multiplier=spread_multiplier,
            size_multiplier=size_multiplier,
            adverse_bps=adverse,
            sample_count=len(samples),
            reason="adverse_markout",
        )

    def snapshot(
        self,
        *,
        mid_price: Optional[float],
        position_size: float,
        max_pos_usd: Optional[float],
        realized_pnl_cumulative: Optional[float],
        portfolio_value: Optional[float],
        available_capital: Optional[float],
    ) -> dict[str, Any]:
        fill_count = len(self._fill_events)
        buy_count = sum(1 for _, side, _, _ in self._fill_events if side == "buy")
        sell_count = sum(1 for _, side, _, _ in self._fill_events if side == "sell")
        volume_usd = sum(price * size for _, _, price, size in self._fill_events)
        spread_avg = mean(v for _, v in self._spread_capture) if self._spread_capture else 0.0

        markout_summary: dict[str, dict[str, float | int]] = {}
        for horizon in self.horizons:
            samples = self._markouts.get(horizon, deque())
            by_side: dict[str, dict[str, float | int]] = {}
            for side, side_samples in self._markouts_by_side.get(horizon, {}).items():
                by_side[side] = {
                    "count": len(side_samples),
                    "avg_bps": mean(v[1] for v in side_samples) if side_samples else 0.0,
                    "adverse_avg_bps": mean(v[2] for v in side_samples) if side_samples else 0.0,
                }
            markout_summary[str(int(horizon) if horizon.is_integer() else horizon)] = {
                "count": len(samples),
                "avg_bps": mean(v[1] for v in samples) if samples else 0.0,
                "adverse_avg_bps": mean(v[2] for v in samples) if samples else 0.0,
                "by_side": by_side,
            }

        inventory_avg = mean(v[1] for v in self._inventory_samples) if self._inventory_samples else 0.0
        boundary_avg = mean(min(v[2], 2.0) for v in self._inventory_samples) if self._inventory_samples else 0.0
        realized = _finite(realized_pnl_cumulative) or 0.0
        pnl_bps = (realized / volume_usd * 10_000.0) if volume_usd > 0 else 0.0
        target_samples = self._samples_for_horizon(self.adaptive_horizon)
        adverse = mean(v[2] for v in target_samples) if target_samples else 0.0
        score = pnl_bps + 0.25 * spread_avg - 0.75 * adverse - 15.0 * boundary_avg
        score = max(-100.0, min(100.0, score))

        return {
            "updated_at": _utc_now(),
            "symbol": self.symbol,
            "window_seconds": self.window_seconds,
            "fills": {
                "count": fill_count,
                "buy": buy_count,
                "sell": sell_count,
                "volume_usd": volume_usd,
            },
            "pnl": {
                "realized_usd": realized,
                "pnl_bps_on_volume": pnl_bps,
                "portfolio_value_usd": _finite(portfolio_value),
                "available_capital_usd": _finite(available_capital),
            },
            "spread_capture_bps_avg": spread_avg,
            "markouts": markout_summary,
            "inventory": {
                "position_size": float(position_size or 0.0),
                "position_value_usd": abs(float(position_size or 0.0)) * float(mid_price or 0.0),
                "avg_inventory_usd": inventory_avg,
                "avg_boundary_ratio": boundary_avg,
                "max_position_usd": _finite(max_pos_usd),
            },
            "quality": {
                "score": score,
                "adjustment": {
                    "spread_multiplier": self._last_adjustment.spread_multiplier,
                    "size_multiplier": self._last_adjustment.size_multiplier,
                    "adverse_bps": self._last_adjustment.adverse_bps,
                    "sample_count": self._last_adjustment.sample_count,
                    "reason": self._last_adjustment.reason,
                },
            },
            "files": {
                "markouts": self.markout_path,
                "metrics": self.metrics_path,
            },
        }

    def flush_metrics(
        self,
        *,
        mid_price: Optional[float],
        position_size: float,
        max_pos_usd: Optional[float],
        realized_pnl_cumulative: Optional[float],
        portfolio_value: Optional[float],
        available_capital: Optional[float],
    ) -> None:
        self._last_metrics_flush = time.monotonic()
        payload = self.snapshot(
            mid_price=mid_price,
            position_size=position_size,
            max_pos_usd=max_pos_usd,
            realized_pnl_cumulative=realized_pnl_cumulative,
            portfolio_value=portfolio_value,
            available_capital=available_capital,
        )
        _atomic_json_write(self.metrics_path, payload)
