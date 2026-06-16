"""Rolling Lighter microstructure estimators for market-making research."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
import time
from typing import Any


@dataclass(frozen=True)
class CJSnapshot:
    lambda_plus: float
    lambda_minus: float
    kappa_plus: float
    kappa_minus: float
    epsilon_plus: float
    epsilon_minus: float
    sigma2_per_sec: float
    trade_count_plus: int
    trade_count_minus: int
    markout_count_plus: int
    markout_count_minus: int
    depth_count_plus: int
    depth_count_minus: int
    kappa_r2_plus: float
    kappa_r2_minus: float
    kappa_points_plus: int
    kappa_points_minus: int
    window_seconds: float
    sample_count: int
    updated_at: float
    ready: bool

    @property
    def age_seconds(self) -> float:
        if self.updated_at <= 0:
            return math.inf
        return max(0.0, time.monotonic() - self.updated_at)

    @property
    def kappa_fit_quality(self) -> float:
        plus_points = min(max(self.kappa_points_plus, 0) / 4.0, 1.0)
        minus_points = min(max(self.kappa_points_minus, 0) / 4.0, 1.0)
        plus = max(self.kappa_r2_plus, 0.0) * plus_points
        minus = max(self.kappa_r2_minus, 0.0) * minus_points
        return min(max((plus + minus) / 2.0, 0.0), 1.0)


@dataclass
class _TradeEvent:
    side: str
    ts: float
    price: float
    size: float
    mid_at_trade: float
    depth: float
    resolved: bool = False
    deadline: float = 0.0


@dataclass(frozen=True)
class _KappaFit:
    kappa: float
    r2: float
    points: int
    depth_count: int


class LighterCJEstimator:
    """Estimate CJ inputs from live Lighter public trades and orderbook mids.

    Naming follows the CJ formulas used in ``cartea_jaimungal``:
    ``plus`` is ask-side execution pressure (public taker buys), while
    ``minus`` is bid-side execution pressure (public taker sells).
    """

    def __init__(
        self,
        *,
        window_seconds: float = 900.0,
        markout_seconds: float = 5.0,
        min_trades_per_side: int = 8,
        min_markouts_per_side: int = 4,
        kappa_min: float = 0.005,
        kappa_max: float = 0.25,
        min_kappa_points: int = 4,
        min_kappa_r2: float = 0.15,
        epsilon_floor: float = 0.0,
        epsilon_cap: float = 80.0,
        default_lambda: float = 0.30,
        default_kappa: float = 0.035,
        default_epsilon: float = 4.0,
        default_sigma2: float = 0.0,
    ) -> None:
        self.window_seconds = max(float(window_seconds), 1.0)
        self.markout_seconds = max(float(markout_seconds), 0.1)
        self.min_trades_per_side = max(int(min_trades_per_side), 1)
        self.min_markouts_per_side = max(int(min_markouts_per_side), 1)
        self.kappa_min = max(float(kappa_min), 1e-9)
        self.kappa_max = max(float(kappa_max), self.kappa_min)
        self.min_kappa_points = max(int(min_kappa_points), 2)
        self.min_kappa_r2 = min(max(float(min_kappa_r2), 0.0), 1.0)
        self.epsilon_floor = max(float(epsilon_floor), 0.0)
        self.epsilon_cap = max(float(epsilon_cap), self.epsilon_floor)
        self.default_lambda = max(float(default_lambda), 0.0)
        self.default_kappa = min(max(float(default_kappa), self.kappa_min), self.kappa_max)
        self.default_epsilon = min(max(float(default_epsilon), self.epsilon_floor), self.epsilon_cap)
        self.default_sigma2 = max(float(default_sigma2), 0.0)

        self._trades: deque[_TradeEvent] = deque()
        self._pending_markouts: deque[_TradeEvent] = deque()
        self._markouts_plus: deque[tuple[float, float]] = deque()
        self._markouts_minus: deque[tuple[float, float]] = deque()
        self._mid_samples: deque[tuple[float, float]] = deque()
        self._seen_trade_ids: set[str] = set()
        self._last_snapshot = self._fallback_snapshot(updated_at=0.0)

    def _fallback_snapshot(self, *, updated_at: float | None = None) -> CJSnapshot:
        return CJSnapshot(
            lambda_plus=self.default_lambda,
            lambda_minus=self.default_lambda,
            kappa_plus=self.default_kappa,
            kappa_minus=self.default_kappa,
            epsilon_plus=self.default_epsilon,
            epsilon_minus=self.default_epsilon,
            sigma2_per_sec=self.default_sigma2,
            trade_count_plus=0,
            trade_count_minus=0,
            markout_count_plus=0,
            markout_count_minus=0,
            depth_count_plus=0,
            depth_count_minus=0,
            kappa_r2_plus=0.0,
            kappa_r2_minus=0.0,
            kappa_points_plus=0,
            kappa_points_minus=0,
            window_seconds=self.window_seconds,
            sample_count=0,
            updated_at=time.monotonic() if updated_at is None else updated_at,
            ready=False,
        )

    @staticmethod
    def _float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            return out if math.isfinite(out) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _trade_id(trade: dict) -> str | None:
        for key in ("trade_id", "id", "tx_hash"):
            value = trade.get(key)
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _side_from_trade(trade: dict) -> str | None:
        raw = str(trade.get("type") or trade.get("side") or "").lower()
        if raw in {"buy", "bid", "taker_buy"}:
            return "plus"
        if raw in {"sell", "ask", "taker_sell"}:
            return "minus"
        if "is_maker_ask" in trade:
            # Lighter public trades often expose type="trade" and encode side
            # through the maker side. Maker ask means a taker buy hit the ask.
            return "plus" if bool(trade.get("is_maker_ask")) else "minus"
        return None

    def on_trade(self, trade: dict, mid_price: float | None = None) -> None:
        side = self._side_from_trade(trade)
        if side is None:
            return

        trade_id = self._trade_id(trade)
        if trade_id is not None:
            if trade_id in self._seen_trade_ids:
                return
            self._seen_trade_ids.add(trade_id)
            if len(self._seen_trade_ids) > 50_000:
                self._seen_trade_ids = set(list(self._seen_trade_ids)[-25_000:])

        price = self._float(trade.get("price"))
        size = self._float(trade.get("size"))
        if price <= 0 or size <= 0:
            return

        now = time.monotonic()
        mid = self._float(mid_price, price)
        if mid <= 0:
            mid = price

        if side == "plus":
            depth = max(0.0, price - mid)
        else:
            depth = max(0.0, mid - price)

        evt = _TradeEvent(
            side=side,
            ts=now,
            price=price,
            size=size,
            mid_at_trade=mid,
            depth=depth,
            deadline=now + self.markout_seconds,
        )
        self._trades.append(evt)
        self._pending_markouts.append(evt)
        self._prune(now)

    def on_book_update(self, mid_price: float) -> None:
        if mid_price <= 0:
            return
        now = time.monotonic()
        self._mid_samples.append((now, float(mid_price)))

        while self._pending_markouts and self._pending_markouts[0].deadline <= now:
            evt = self._pending_markouts.popleft()
            if evt.resolved:
                continue
            evt.resolved = True
            if evt.side == "plus":
                adverse = max(0.0, mid_price - evt.price)
                self._markouts_plus.append((now, adverse))
            else:
                adverse = max(0.0, evt.price - mid_price)
                self._markouts_minus.append((now, adverse))

        self._prune(now)
        self._last_snapshot = self._compute_snapshot(now)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._trades and self._trades[0].ts < cutoff:
            self._trades.popleft()
        while self._pending_markouts and self._pending_markouts[0].ts < cutoff:
            self._pending_markouts.popleft()
        while self._markouts_plus and self._markouts_plus[0][0] < cutoff:
            self._markouts_plus.popleft()
        while self._markouts_minus and self._markouts_minus[0][0] < cutoff:
            self._markouts_minus.popleft()
        while self._mid_samples and self._mid_samples[0][0] < cutoff:
            self._mid_samples.popleft()

    def _side_events(self, side: str) -> list[_TradeEvent]:
        return [evt for evt in self._trades if evt.side == side]

    def _estimate_lambda(self, events: list[_TradeEvent], now: float) -> float:
        if len(events) < 2:
            return self.default_lambda
        span = self._event_span(events, now)
        return len(events) / span

    def _event_span(self, events: list[_TradeEvent], now: float) -> float:
        if not events:
            return 1.0
        return max(events[-1].ts - events[0].ts, min(self.window_seconds, now - events[0].ts), 1.0)

    @staticmethod
    def _quantile(sorted_values: list[float], q: float) -> float:
        if not sorted_values:
            return 0.0
        idx = min(max(int(round((len(sorted_values) - 1) * q)), 0), len(sorted_values) - 1)
        return sorted_values[idx]

    def _fit_kappa(self, events: list[_TradeEvent], now: float) -> _KappaFit:
        depths = [evt.depth for evt in events if evt.depth > 0]
        if len(depths) < self.min_kappa_points:
            return _KappaFit(self.default_kappa, 0.0, 0, len(depths))

        depths.sort()
        span = self._event_span(events, now)
        # Fit log(lambda(delta)) = log(A) - kappa * delta using empirical
        # survival intensities. Quantile buckets keep the fit stable when most
        # trades happen near the touch and only a few walk the book.
        candidate_quantiles = (0.10, 0.25, 0.40, 0.55, 0.70, 0.85)
        points: list[tuple[float, float]] = []
        seen: set[float] = set()
        for q in candidate_quantiles:
            delta = self._quantile(depths, q)
            if delta <= 0 or delta in seen:
                continue
            seen.add(delta)
            survival_count = sum(1 for depth in depths if depth >= delta)
            if survival_count < 2:
                continue
            intensity = survival_count / span
            if intensity > 0:
                points.append((delta, math.log(intensity)))

        if len(points) < self.min_kappa_points:
            mean_depth = sum(depths) / len(depths)
            fallback = self.default_kappa if mean_depth <= 0 else 1.0 / mean_depth
            return _KappaFit(min(max(fallback, self.kappa_min), self.kappa_max), 0.0, len(points), len(depths))

        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        x_bar = sum(xs) / len(xs)
        y_bar = sum(ys) / len(ys)
        denom = sum((x - x_bar) ** 2 for x in xs)
        if denom <= 0:
            return _KappaFit(self.default_kappa, 0.0, len(points), len(depths))

        slope = sum((x - x_bar) * (y - y_bar) for x, y in points) / denom
        intercept = y_bar - slope * x_bar
        sse = sum((y - (intercept + slope * x)) ** 2 for x, y in points)
        sst = sum((y - y_bar) ** 2 for y in ys)
        r2 = 1.0 if sst <= 1e-12 else max(0.0, min(1.0, 1.0 - (sse / sst)))
        fitted = -slope
        if fitted <= 0 or r2 < self.min_kappa_r2:
            mean_depth = sum(depths) / len(depths)
            fallback = self.default_kappa if mean_depth <= 0 else 1.0 / mean_depth
            return _KappaFit(min(max(fallback, self.kappa_min), self.kappa_max), r2, len(points), len(depths))

        return _KappaFit(min(max(fitted, self.kappa_min), self.kappa_max), r2, len(points), len(depths))

    def _estimate_epsilon(self, markouts: deque[tuple[float, float]]) -> float:
        if not markouts:
            return self.default_epsilon
        values = [value for _, value in markouts]
        eps = sum(values) / len(values)
        return min(max(eps, self.epsilon_floor), self.epsilon_cap)

    def _estimate_sigma2(self) -> float:
        if len(self._mid_samples) < 3:
            return self.default_sigma2
        increments = []
        prev_t, prev_mid = self._mid_samples[0]
        for ts, mid in list(self._mid_samples)[1:]:
            dt = max(ts - prev_t, 1e-6)
            diff = mid - prev_mid
            increments.append((diff * diff) / dt)
            prev_t, prev_mid = ts, mid
        if not increments:
            return self.default_sigma2
        return max(sum(increments) / len(increments), 0.0)

    def _compute_snapshot(self, now: float) -> CJSnapshot:
        plus = self._side_events("plus")
        minus = self._side_events("minus")
        kappa_plus = self._fit_kappa(plus, now)
        kappa_minus = self._fit_kappa(minus, now)
        ready = (
            len(plus) >= self.min_trades_per_side
            and len(minus) >= self.min_trades_per_side
            and len(self._markouts_plus) >= self.min_markouts_per_side
            and len(self._markouts_minus) >= self.min_markouts_per_side
        )
        return CJSnapshot(
            lambda_plus=self._estimate_lambda(plus, now),
            lambda_minus=self._estimate_lambda(minus, now),
            kappa_plus=kappa_plus.kappa,
            kappa_minus=kappa_minus.kappa,
            epsilon_plus=self._estimate_epsilon(self._markouts_plus),
            epsilon_minus=self._estimate_epsilon(self._markouts_minus),
            sigma2_per_sec=self._estimate_sigma2(),
            trade_count_plus=len(plus),
            trade_count_minus=len(minus),
            markout_count_plus=len(self._markouts_plus),
            markout_count_minus=len(self._markouts_minus),
            depth_count_plus=kappa_plus.depth_count,
            depth_count_minus=kappa_minus.depth_count,
            kappa_r2_plus=kappa_plus.r2,
            kappa_r2_minus=kappa_minus.r2,
            kappa_points_plus=kappa_plus.points,
            kappa_points_minus=kappa_minus.points,
            window_seconds=self.window_seconds,
            sample_count=len(self._mid_samples),
            updated_at=now,
            ready=ready,
        )

    def snapshot(self) -> CJSnapshot:
        return self._last_snapshot


__all__ = ["CJSnapshot", "LighterCJEstimator"]
