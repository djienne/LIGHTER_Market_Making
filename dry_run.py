"""Dry-run / paper-trading engine for market_maker_v2.

Simulates limit-order fills against the live orderbook without sending
any transactions to the exchange.  Activated via ``--dry-run`` CLI flag.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SimulatedOrder:
    client_order_id: int
    side: str            # "buy" or "sell"
    price: float
    size: float          # remaining size (decremented on partial fill)
    original_size: float
    level: int
    created_at: float    # time.monotonic()
    synthetic_exchange_id: int
    eligible_at: float = 0.0        # not fillable until this monotonic time (create/modify latency)
    pending_cancel_at: float = 0.0  # if >0, order removed after this time (cancel latency)
    _prev_available: float = 0.0    # available liquidity seen on last check (for delta-fill)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class DryRunEngine:
    """Replaces the exchange interaction layer in dry-run mode.

    * ``process_batch`` is called instead of ``sign_and_send_batch``.
    * ``check_fills`` is called on every orderbook update for low-latency
      fill simulation.
    """

    def __init__(
        self,
        state,                          # MMState
        order_manager,                  # OrderManager
        client_to_exchange_id: dict,    # _client_to_exchange_id
        leverage: int,
        logger: logging.Logger,
        *,
        sim_latency_s: float = 0.050,  # simulated exchange latency (default 50 ms)
        log_interval: float = 60.0,
    ):
        self._state = state
        self._om = order_manager
        self._id_map = client_to_exchange_id
        self._leverage = max(leverage, 1)
        self._log = logger
        self._sim_latency = sim_latency_s
        self._log_interval = log_interval

        # Simulated order book
        self._live_orders: dict[int, SimulatedOrder] = {}
        self._next_exchange_id: int = 900_000_000  # clearly synthetic

        # PnL tracking (average-cost basis)
        self._position: float = 0.0
        self._entry_vwap: float = 0.0
        self._realized_pnl: float = 0.0
        self._total_volume: float = 0.0
        self._fill_count: int = 0
        self._initial_capital: float = 0.0

        # Periodic summary
        self._last_summary: float = 0.0

        # Set after initial capital captured – gates WS overwrites
        self.initialized: bool = False

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def capture_initial_state(self) -> None:
        """Snapshot the real account capital and position before trading starts."""
        cap = self._state.account.available_capital
        self._initial_capital = cap if cap is not None else 0.0

        # Inherit the live account position so skew/one-sided quoting is correct
        # from the first loop iteration.  We use mid as a rough VWAP proxy since
        # the real entry price is unknown.
        pos = self._state.account.position_size
        self._position = pos
        if abs(pos) > 1e-12 and self._state.market.mid_price is not None:
            self._entry_vwap = self._state.market.mid_price
        else:
            self._entry_vwap = 0.0

        self.initialized = True
        self._log.info(
            "DRY-RUN: captured initial capital $%.2f | position %.6f | "
            "leverage %dx | sim_latency=%dms",
            self._initial_capital, self._position,
            self._leverage, int(self._sim_latency * 1000),
        )

    # ------------------------------------------------------------------
    # Batch processing  (replaces sign_and_send_batch)
    # ------------------------------------------------------------------

    async def process_batch(self, ops: list) -> None:
        """Process a batch of ``BatchOp`` without touching the exchange."""
        if not ops:
            return

        for op in ops:
            if op.action == "create":
                self._handle_create(op)
            elif op.action == "modify":
                self._handle_modify(op)
            elif op.action == "cancel":
                self._handle_cancel(op)
            else:
                self._log.warning("DRY-RUN: unknown action %r", op.action)

        self._log.debug(
            "DRY-RUN batch: %d ops processed (%d live orders)",
            len(ops), len(self._live_orders),
        )

    def _handle_create(self, op) -> None:
        eid = self._next_exchange_id
        self._next_exchange_id += 1
        now = time.monotonic()

        sim = SimulatedOrder(
            client_order_id=op.order_id,
            side=op.side,
            price=op.price,
            size=op.size,
            original_size=op.size,
            level=op.level,
            created_at=now,
            synthetic_exchange_id=eid,
            eligible_at=now + self._sim_latency,
        )
        self._live_orders[op.order_id] = sim
        self._id_map[op.order_id] = eid
        self._om.bind_live(op.side, op.order_id, op.price, op.size, level=op.level)

        self._log.info(
            "DRY-RUN CREATE %s L%d: %.6f @ $%.2f  (cid=%d eid=%d)",
            op.side.upper(), op.level, op.size, op.price, op.order_id, eid,
        )

    def _handle_modify(self, op) -> None:
        sim = self._live_orders.get(op.order_id)
        if sim is None:
            self._log.warning("DRY-RUN MODIFY: cid=%d not found", op.order_id)
            return
        old_price = sim.price
        sim.price = op.price
        sim.size = op.size
        sim.eligible_at = time.monotonic() + self._sim_latency
        self._om.bind_live(op.side, op.order_id, op.price, op.size, level=op.level)

        self._log.info(
            "DRY-RUN MODIFY %s L%d: %.6f @ $%.2f -> $%.2f  (cid=%d)",
            op.side.upper(), op.level, op.size, old_price, op.price, op.order_id,
        )

    def _handle_cancel(self, op) -> None:
        sim = self._live_orders.get(op.order_id)
        if sim is None:
            self._om.clear_live(op.side, op.level)
            self._log.debug("DRY-RUN CANCEL: cid=%d already gone", op.order_id)
            return
        # Order stays fillable during the cancel latency window
        sim.pending_cancel_at = time.monotonic() + self._sim_latency
        self._log.info(
            "DRY-RUN CANCEL %s L%d  (cid=%d, effective in %dms)",
            op.side.upper(), op.level, op.order_id, int(self._sim_latency * 1000),
        )

    # ------------------------------------------------------------------
    # Fill simulation  (called from on_order_book_update)
    # ------------------------------------------------------------------

    def check_fills(self, bids, asks) -> None:
        """Check every live simulated order against the current orderbook.

        Called on every WS orderbook delta (~100 ms) for lowest latency.
        ``bids`` / ``asks`` are ``_BookSide`` (SortedDict-like) objects.
        """
        if not self._live_orders:
            return

        now = time.monotonic()

        # Track total available liquidity consumed by earlier orders in this
        # tick so that multiple simulated orders don't over-consume the same book.
        buy_consumed = 0.0
        sell_consumed = 0.0

        # Snapshot order list to allow mutation during iteration
        for sim in list(self._live_orders.values()):
            # Process matured pending cancels: order was fillable during the
            # cancel window but is now confirmed cancelled.
            if sim.pending_cancel_at > 0 and now >= sim.pending_cancel_at:
                self._live_orders.pop(sim.client_order_id, None)
                self._om.clear_live(sim.side, sim.level)
                continue

            # Skip orders still in-flight (create/modify latency)
            if now < sim.eligible_at:
                continue

            if sim.side == "buy":
                fill, buy_consumed = self._check_buy_fill(sim, asks, buy_consumed)
            else:
                fill, sell_consumed = self._check_sell_fill(sim, bids, sell_consumed)

            if fill > 0:
                self._process_fill(sim, fill, sim.price)

    def _check_buy_fill(self, sim: SimulatedOrder, asks, consumed: float) -> tuple[float, float]:
        """Return (fillable_size, updated_consumed) for a buy limit order.

        Uses delta-fill: only new liquidity appearing at/below our price since
        the last check is eligible. ``consumed`` tracks liquidity already
        claimed by earlier simulated orders in the same tick.
        """
        if not asks:
            sim._prev_available = 0.0
            return 0.0, consumed
        best_ask = asks.peekitem(0)[0]
        if best_ask > sim.price:
            sim._prev_available = 0.0
            return 0.0, consumed
        available = 0.0
        for ask_price, ask_size in asks.items():
            if ask_price > sim.price:
                break
            available += ask_size
        # Delta: only the increase since last check is new fillable liquidity
        new_liq = max(0.0, available - sim._prev_available) - consumed
        sim._prev_available = available
        if new_liq <= 0:
            return 0.0, consumed
        fill = min(sim.size, new_liq)
        return fill, consumed + fill

    def _check_sell_fill(self, sim: SimulatedOrder, bids, consumed: float) -> tuple[float, float]:
        """Return (fillable_size, updated_consumed) for a sell limit order.

        Uses delta-fill: only new liquidity appearing at/above our price since
        the last check is eligible. ``consumed`` tracks liquidity already
        claimed by earlier simulated orders in the same tick.
        """
        if not bids:
            sim._prev_available = 0.0
            return 0.0, consumed
        best_bid = bids.peekitem(-1)[0]
        if best_bid < sim.price:
            sim._prev_available = 0.0
            return 0.0, consumed
        available = 0.0
        for bid_price in reversed(bids):
            if bid_price < sim.price:
                break
            available += bids[bid_price]
        new_liq = max(0.0, available - sim._prev_available) - consumed
        sim._prev_available = available
        if new_liq <= 0:
            return 0.0, consumed
        fill = min(sim.size, new_liq)
        return fill, consumed + fill

    # ------------------------------------------------------------------
    # Fill processing & PnL
    # ------------------------------------------------------------------

    def _process_fill(self, sim: SimulatedOrder, fill_size: float, fill_price: float) -> None:
        """Apply a (possibly partial) fill to simulated state."""
        sim.size -= fill_size
        fully_filled = sim.size <= 1e-12

        # Update simulated position
        old_pos = self._position
        if sim.side == "buy":
            self._position += fill_size
        else:
            self._position -= fill_size

        # PnL: average-cost basis
        pnl_before = self._realized_pnl
        self._entry_price_before = self._entry_vwap  # snapshot for margin calc
        self._update_pnl(sim.side, fill_size, fill_price, old_pos)
        realized_delta = self._realized_pnl - pnl_before

        # Bookkeeping
        self._fill_count += 1
        self._total_volume += fill_size * fill_price

        # Capital impact: margin adjustment + realized PnL
        reducing = self._is_reducing(sim.side, old_pos)
        if reducing:
            # Release margin at the *entry* price (what was originally locked)
            # and credit the realized PnL separately.
            margin_release = fill_size * self._entry_price_before / self._leverage
            self._state.account.available_capital += margin_release + realized_delta
        else:
            # Consume margin for new/increased position
            margin_consumed = fill_size * fill_price / self._leverage
            self._state.account.available_capital -= margin_consumed

        # Keep portfolio_value in sync: initial_capital + realized + unrealized
        self._state.account.portfolio_value = (
            self._initial_capital + self._realized_pnl + self.unrealized_pnl
        )

        # Sync position into state for decision logic
        self._state.account.position_size = self._position

        # Append to recent trades
        self._state.account.recent_trades.append({
            "market_id": self._state.config.market_id,
            "price": fill_price,
            "size": fill_size,
            "type": sim.side,
            "timestamp": int(time.time() * 1000),
            "simulated": True,
        })

        # Update order state
        if fully_filled:
            self._live_orders.pop(sim.client_order_id, None)
            self._om.clear_live(sim.side, sim.level)
            label = "FILLED"
        else:
            self._om.bind_live(sim.side, sim.client_order_id, sim.price, sim.size, level=sim.level)
            label = "PARTIAL"

        self._log.info(
            "DRY-RUN %s %s L%d: %.6f @ $%.2f | pos=%.6f | realized=$%.4f | unrealized=$%.4f",
            label, sim.side.upper(), sim.level, fill_size, fill_price,
            self._position, self._realized_pnl, self.unrealized_pnl,
        )

    @staticmethod
    def _is_reducing(side: str, old_pos: float) -> bool:
        """True if this fill reduces the absolute position."""
        if side == "buy" and old_pos < 0:
            return True
        if side == "sell" and old_pos > 0:
            return True
        return False

    def _update_pnl(self, side: str, fill_size: float, fill_price: float, old_pos: float) -> None:
        """Average-cost-basis PnL tracking with position flips."""
        # Determine if reducing or increasing
        if self._is_reducing(side, old_pos):
            reduce_qty = min(fill_size, abs(old_pos))
            if old_pos > 0:
                # Was long, selling to reduce
                pnl = reduce_qty * (fill_price - self._entry_vwap)
            else:
                # Was short, buying to reduce
                pnl = reduce_qty * (self._entry_vwap - fill_price)
            self._realized_pnl += pnl

            # If we flipped, the excess starts a new position at fill_price
            excess = fill_size - reduce_qty
            if excess > 1e-12:
                self._entry_vwap = fill_price
            elif abs(self._position) < 1e-12:
                # Fully closed
                self._entry_vwap = 0.0
        else:
            # Increasing position: update VWAP
            abs_old = abs(old_pos)
            abs_new = abs(self._position)
            if abs_new > 1e-12:
                self._entry_vwap = (
                    self._entry_vwap * abs_old + fill_price * fill_size
                ) / abs_new

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    @property
    def unrealized_pnl(self) -> float:
        mid = self._state.market.mid_price
        if mid is None or abs(self._position) < 1e-12:
            return 0.0
        if self._position > 0:
            return self._position * (mid - self._entry_vwap)
        else:
            return abs(self._position) * (self._entry_vwap - mid)

    @property
    def total_pnl(self) -> float:
        return self._realized_pnl + self.unrealized_pnl

    def maybe_log_summary(self) -> None:
        """Log periodic PnL summary (call from main loop)."""
        now = time.monotonic()
        if now - self._last_summary < self._log_interval:
            return
        self._last_summary = now
        mid = self._state.market.mid_price or 0.0
        self._log.info(
            "DRY-RUN SUMMARY | pos=%.6f | entry=$%.2f | mid=$%.2f | "
            "realized=$%.4f | unrealized=$%.4f | total=$%.4f | "
            "fills=%d | volume=$%.2f | live_orders=%d",
            self._position, self._entry_vwap, mid,
            self._realized_pnl, self.unrealized_pnl, self.total_pnl,
            self._fill_count, self._total_volume, len(self._live_orders),
        )
