"""Dry-run / paper-trading engine for market_maker_v2.

Simulates limit-order fills against the live orderbook without sending
any transactions to the exchange.  Activated via ``--dry-run`` CLI flag.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from trade_log import TradeLogger


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
    _prev_by_price: dict = field(default_factory=dict)  # per-price liquidity seen on last check (for delta-fill)
    _arrival_checked: bool = False   # POST_ONLY recheck done at simulated arrival time
    _queue_ts: float = 0.0          # monotonic timestamp for same-price FIFO priority
    # Pending modify: old price remains fillable while the modify is in-flight.
    # When eligible_at expires, the pending price is promoted to the live price.
    _pending_price: Optional[float] = None
    _pending_size: Optional[float] = None
    _pending_prev_by_price: Optional[dict] = None


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
        trade_logger: Optional['TradeLogger'] = None,
        state_path: Optional[str] = None,
        rejection_callback=None,          # called on POST_ONLY rejects (for circuit breaker)
        maker_fee_rate: float = 0.0,      # maker fee as fraction (e.g. 0.00004 for 0.004%)
    ):
        self._state = state
        self._om = order_manager
        self._id_map = client_to_exchange_id
        self._leverage = max(leverage, 1)
        self._log = logger
        self._sim_latency = sim_latency_s
        self._log_interval = log_interval
        self._trade_logger = trade_logger
        self._state_path = state_path
        self._rejection_cb = rejection_callback
        self._maker_fee_rate = maker_fee_rate

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
        self._initial_portfolio_value: float = 0.0
        self._entry_price_before: float = 0.0

        # Periodic summary
        self._last_summary: float = 0.0

        # Serialize save_state across main thread and run_in_executor workers
        self._save_lock = threading.Lock()

        # Set after initial capital captured – gates WS overwrites
        self.initialized: bool = False

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def capture_initial_state(self) -> None:
        """Snapshot the real account capital and position before trading starts."""
        cap = self._state.account.available_capital
        self._initial_capital = cap if cap is not None else 0.0
        pv = self._state.account.portfolio_value
        self._initial_portfolio_value = pv if pv is not None else self._initial_capital

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
    # Persistence
    # ------------------------------------------------------------------

    def _build_save_data(self) -> dict:
        """Capture all wallet fields into a dict.

        Call on the event-loop thread so the snapshot is atomic w.r.t.
        ``_process_fill`` (which also runs on the event-loop thread).
        """
        return {
            "available_capital": self._state.account.available_capital,
            "portfolio_value": self._state.account.portfolio_value,
            "position": self._position,
            "entry_vwap": self._entry_vwap,
            "realized_pnl": self._realized_pnl,
            "fill_count": self._fill_count,
            "total_volume": self._total_volume,
            "initial_capital": self._initial_capital,
            "initial_portfolio_value": self._initial_portfolio_value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _write_save_data(self, data: dict) -> None:
        """Write a pre-captured data dict to disk (thread-safe)."""
        if self._state_path is None:
            return
        with self._save_lock:
            try:
                dir_name = os.path.dirname(self._state_path) or "."
                fd, tmp = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
                with os.fdopen(fd, "w") as f:
                    json.dump(data, f, indent=2)
                os.replace(tmp, self._state_path)
            except OSError as exc:
                self._log.warning("DRY-RUN: failed to save state: %s", exc)

    def save_state(self) -> None:
        """Atomically write engine state to JSON (tmp + rename)."""
        self._write_save_data(self._build_save_data())

    @classmethod
    def load_state(
        cls,
        path: str,
        state,
        order_manager,
        client_to_exchange_id: dict,
        leverage: int,
        logger: logging.Logger,
        **kwargs,
    ) -> Optional['DryRunEngine']:
        """Restore engine from saved JSON.  Returns None if file missing."""
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("DRY-RUN: could not load state from %s: %s", path, exc)
            return None

        engine = cls(
            state=state,
            order_manager=order_manager,
            client_to_exchange_id=client_to_exchange_id,
            leverage=leverage,
            logger=logger,
            state_path=path,
            **kwargs,
        )
        engine._initial_capital = data.get("initial_capital", 0.0)
        engine._initial_portfolio_value = data.get("initial_portfolio_value", 0.0)
        engine._position = data.get("position", 0.0)
        engine._entry_vwap = data.get("entry_vwap", 0.0)
        engine._realized_pnl = data.get("realized_pnl", 0.0)
        engine._fill_count = data.get("fill_count", 0)
        engine._total_volume = data.get("total_volume", 0.0)

        # Sync restored state into the global state object
        state.account.available_capital = data.get("available_capital", engine._initial_capital)
        state.account.portfolio_value = data.get("portfolio_value", engine._initial_portfolio_value)
        state.account.position_size = engine._position

        engine.initialized = True
        logger.info(
            "DRY-RUN: restored state from %s | capital=$%.2f | pos=%.6f | "
            "realized=$%.4f | fills=%d",
            path, state.account.available_capital, engine._position,
            engine._realized_pnl, engine._fill_count,
        )
        return engine

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

        # POST_ONLY check: reject if immediately marketable
        ob = self._state.market.local_order_book
        if op.side == "buy":
            asks = ob.get('asks')
            if asks and len(asks) > 0:
                best_ask = asks.peekitem(0)[0]
                if best_ask <= op.price:
                    self._log.info(
                        "DRY-RUN REJECT %s L%d: %.6f @ $%.2f (POST_ONLY: best_ask $%.2f <= price)",
                        op.side.upper(), op.level, op.size, op.price, best_ask,
                    )
                    if self._rejection_cb is not None:
                        self._rejection_cb("dry_run:create_post_only_reject")
                    return
        else:
            bids = ob.get('bids')
            if bids and len(bids) > 0:
                best_bid = bids.peekitem(-1)[0]
                if best_bid >= op.price:
                    self._log.info(
                        "DRY-RUN REJECT %s L%d: %.6f @ $%.2f (POST_ONLY: best_bid $%.2f >= price)",
                        op.side.upper(), op.level, op.size, op.price, best_bid,
                    )
                    if self._rejection_cb is not None:
                        self._rejection_cb("dry_run:create_post_only_reject")
                    return

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
            _queue_ts=now,
            # No in-flight window → submit-time check is sufficient
            _arrival_checked=(self._sim_latency <= 0),
        )

        # Snapshot current qualifying depth so first fill check after
        # eligible_at only sees genuinely new arrivals, not pre-existing depth.
        if op.side == "buy":
            asks = ob.get('asks')
            if asks:
                if hasattr(asks, 'apply_snapshot_from_wire'):
                    for ask_price, ask_size in asks.irange(max_price=op.price):
                        sim._prev_by_price[ask_price] = ask_size
                else:
                    for ask_price, ask_size in asks.items():
                        if ask_price > op.price:
                            break
                        sim._prev_by_price[ask_price] = ask_size
        else:
            bids = ob.get('bids')
            if bids:
                if hasattr(bids, 'apply_snapshot_from_wire'):
                    for bid_price, bid_size in bids.irange(min_price=op.price):
                        sim._prev_by_price[bid_price] = bid_size
                else:
                    for bid_price, bid_size in bids.items():
                        if bid_price >= op.price:
                            sim._prev_by_price[bid_price] = bid_size

        self._live_orders[op.order_id] = sim
        self._id_map[op.order_id] = eid
        self._om._bind_live(op.side, op.order_id, op.price, op.size, level=op.level)

        self._log.info(
            "DRY-RUN CREATE %s L%d: %.6f @ $%.2f  (cid=%d eid=%d)",
            op.side.upper(), op.level, op.size, op.price, op.order_id, eid,
        )

    def _handle_modify(self, op) -> None:
        sim = self._live_orders.get(op.order_id)
        if sim is None:
            self._log.warning("DRY-RUN MODIFY: cid=%d not found", op.order_id)
            return

        # POST_ONLY check for the new price — on rejection, keep old order live
        ob = self._state.market.local_order_book
        if op.side == "buy":
            asks = ob.get('asks')
            if asks and len(asks) > 0 and asks.peekitem(0)[0] <= op.price:
                self._log.info(
                    "DRY-RUN REJECT-MODIFY %s L%d: @ $%.2f -> $%.2f "
                    "(POST_ONLY: best_ask $%.2f <= new price; old order kept)",
                    op.side.upper(), op.level, sim.price, op.price,
                    asks.peekitem(0)[0],
                )
                if self._rejection_cb is not None:
                    self._rejection_cb("dry_run:modify_post_only_reject")
                return  # old order stays live at old price
        else:
            bids = ob.get('bids')
            if bids and len(bids) > 0 and bids.peekitem(-1)[0] >= op.price:
                self._log.info(
                    "DRY-RUN REJECT-MODIFY %s L%d: @ $%.2f -> $%.2f "
                    "(POST_ONLY: best_bid $%.2f >= new price; old order kept)",
                    op.side.upper(), op.level, sim.price, op.price,
                    bids.peekitem(-1)[0],
                )
                if self._rejection_cb is not None:
                    self._rejection_cb("dry_run:modify_post_only_reject")
                return  # old order stays live at old price

        old_price = sim.price
        now = time.monotonic()

        # Snapshot qualifying depth for the NEW price
        new_snapshot: dict = {}
        if op.side == "buy":
            asks = ob.get('asks')
            if asks:
                if hasattr(asks, 'apply_snapshot_from_wire'):
                    for ask_price, ask_size in asks.irange(max_price=op.price):
                        new_snapshot[ask_price] = ask_size
                else:
                    for ask_price, ask_size in asks.items():
                        if ask_price > op.price:
                            break
                        new_snapshot[ask_price] = ask_size
        else:
            bids = ob.get('bids')
            if bids:
                if hasattr(bids, 'apply_snapshot_from_wire'):
                    for bid_price, bid_size in bids.irange(min_price=op.price):
                        new_snapshot[bid_price] = bid_size
                else:
                    for bid_price, bid_size in bids.items():
                        if bid_price >= op.price:
                            new_snapshot[bid_price] = bid_size

        if self._sim_latency <= 0:
            # No latency: switch immediately
            sim.price = op.price
            sim.size = op.size
            sim._prev_by_price = new_snapshot
            sim._arrival_checked = True
            sim._queue_ts = now
            sim._pending_price = None
            sim._pending_size = None
            sim._pending_prev_by_price = None
            # Sync state.orders to new price immediately
            self._om._bind_live(op.side, op.order_id, op.price, op.size, level=op.level)
        else:
            # Latency > 0: old price stays fillable while modify is in-flight.
            # Store new price as pending; promoted in check_fills when eligible.
            # NOTE: don't reset _arrival_checked — the old price was already
            # validated. The new price gets its own arrival check in check_fills
            # when the pending modify is promoted.
            # NOTE: don't call bind_live here — state.orders must keep the old
            # price until the modify actually lands (or is rejected).
            sim._pending_price = op.price
            sim._pending_size = op.size
            sim._pending_prev_by_price = new_snapshot
            sim.eligible_at = now + self._sim_latency

        self._log.info(
            "DRY-RUN MODIFY %s L%d: %.6f @ $%.2f -> $%.2f  (cid=%d)",
            op.side.upper(), op.level, op.size, old_price, op.price, op.order_id,
        )

    def _handle_cancel(self, op) -> None:
        sim = self._live_orders.get(op.order_id)
        if sim is None:
            self._om._clear_live(op.side, op.level)
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

        # PASS 1: Promote all pending modifies whose latency expired.
        # This must happen before sorting so the sort sees final prices.
        for sim in list(self._live_orders.values()):
            if sim._pending_price is not None and now >= sim.eligible_at:
                rejected = False
                if sim.side == "buy" and asks and len(asks) > 0:
                    if asks.peekitem(0)[0] <= sim._pending_price:
                        self._log.info(
                            "DRY-RUN REJECT-MODIFY-AT-ARRIVAL %s L%d @ $%.2f "
                            "(book crossed; old order kept at $%.2f)",
                            sim.side.upper(), sim.level, sim._pending_price, sim.price,
                        )
                        rejected = True
                elif sim.side == "sell" and bids and len(bids) > 0:
                    if bids.peekitem(-1)[0] >= sim._pending_price:
                        self._log.info(
                            "DRY-RUN REJECT-MODIFY-AT-ARRIVAL %s L%d @ $%.2f "
                            "(book crossed; old order kept at $%.2f)",
                            sim.side.upper(), sim.level, sim._pending_price, sim.price,
                        )
                        rejected = True
                if rejected:
                    sim._pending_price = None
                    sim._pending_size = None
                    sim._pending_prev_by_price = None
                    self._om._bind_live(sim.side, sim.client_order_id,
                                       sim.price, sim.size, level=sim.level)
                    if self._rejection_cb is not None:
                        self._rejection_cb("dry_run:modify_arrival_reject")
                else:
                    sim.price = sim._pending_price
                    sim.size = sim._pending_size
                    sim._prev_by_price = sim._pending_prev_by_price or {}
                    sim._queue_ts = now
                    sim._pending_price = None
                    sim._pending_size = None
                    sim._pending_prev_by_price = None
                    self._om._bind_live(sim.side, sim.client_order_id,
                                       sim.price, sim.size, level=sim.level)

        # PASS 2: Sort by (now-final) price priority, then check fills.
        orders = list(self._live_orders.values())
        orders.sort(key=lambda s: (-s.price if s.side == "buy" else s.price, s._queue_ts))

        for sim in orders:
            # --- Skip orders in-flight for CREATE (not pending-modify) ---
            if now < sim.eligible_at and sim._pending_price is None:
                # Pure create in-flight: not fillable yet
                if sim.pending_cancel_at > 0 and now >= sim.pending_cancel_at:
                    self._live_orders.pop(sim.client_order_id, None)
                    self._om._clear_live(sim.side, sim.level)
                continue

            # POST_ONLY recheck at simulated arrival time (for creates)
            if not sim._arrival_checked:
                sim._arrival_checked = True
                if sim.side == "buy" and asks and len(asks) > 0:
                    if asks.peekitem(0)[0] <= sim.price:
                        self._log.info(
                            "DRY-RUN REJECT-AT-ARRIVAL %s L%d @ $%.2f "
                            "(book crossed during latency)",
                            sim.side.upper(), sim.level, sim.price,
                        )
                        self._live_orders.pop(sim.client_order_id, None)
                        self._om._clear_live(sim.side, sim.level)
                        if self._rejection_cb is not None:
                            self._rejection_cb("dry_run:create_arrival_reject")
                        continue
                elif sim.side == "sell" and bids and len(bids) > 0:
                    if bids.peekitem(-1)[0] >= sim.price:
                        self._log.info(
                            "DRY-RUN REJECT-AT-ARRIVAL %s L%d @ $%.2f "
                            "(book crossed during latency)",
                            sim.side.upper(), sim.level, sim.price,
                        )
                        self._live_orders.pop(sim.client_order_id, None)
                        self._om._clear_live(sim.side, sim.level)
                        if self._rejection_cb is not None:
                            self._rejection_cb("dry_run:create_arrival_reject")
                        continue

            # Check fills at current (possibly old) price.
            # Orders with a pending modify are still fillable at the old price.
            if sim.side == "buy":
                fill, buy_consumed = self._check_buy_fill(sim, asks, buy_consumed)
            else:
                fill, sell_consumed = self._check_sell_fill(sim, bids, sell_consumed)

            if fill > 0:
                self._process_fill(sim, fill, sim.price)

            # THEN process matured cancel (after fill opportunity)
            if sim.pending_cancel_at > 0 and now >= sim.pending_cancel_at:
                self._live_orders.pop(sim.client_order_id, None)
                self._om._clear_live(sim.side, sim.level)

    def _check_buy_fill(self, sim: SimulatedOrder, asks, consumed: float) -> tuple[float, float]:
        """Return (fillable_size, updated_consumed) for a buy limit order.

        Uses per-price delta-fill: only new or increased liquidity at each
        qualifying price since the last check is eligible.  This correctly
        detects replenished liquidity when it appears at a different price
        even if the aggregate depth stays flat.  ``consumed`` tracks
        liquidity already claimed by earlier simulated orders in the same tick.
        """
        if not asks:
            sim._prev_by_price = {}
            return 0.0, consumed
        best_ask = asks.peekitem(0)[0]
        if best_ask > sim.price:
            sim._prev_by_price = {}
            return 0.0, consumed
        current: dict[float, float] = {}
        if hasattr(asks, 'apply_snapshot_from_wire'):
            for ask_price, ask_size in asks.irange(max_price=sim.price):
                current[ask_price] = ask_size
        else:
            for ask_price, ask_size in asks.items():
                if ask_price > sim.price:
                    break
                current[ask_price] = ask_size
        prev = sim._prev_by_price
        new_liq = 0.0
        for price, size in current.items():
            delta = size - prev.get(price, 0.0)
            if delta > 0:
                new_liq += delta
        new_liq -= consumed
        sim._prev_by_price = current
        if new_liq <= 0:
            return 0.0, consumed
        fill = min(sim.size, new_liq)
        return fill, consumed + fill

    def _check_sell_fill(self, sim: SimulatedOrder, bids, consumed: float) -> tuple[float, float]:
        """Return (fillable_size, updated_consumed) for a sell limit order.

        Uses per-price delta-fill: only new or increased liquidity at each
        qualifying price since the last check is eligible.  This correctly
        detects replenished liquidity when it appears at a different price
        even if the aggregate depth stays flat.  ``consumed`` tracks
        liquidity already claimed by earlier simulated orders in the same tick.
        """
        if not bids:
            sim._prev_by_price = {}
            return 0.0, consumed
        best_bid = bids.peekitem(-1)[0]
        if best_bid < sim.price:
            sim._prev_by_price = {}
            return 0.0, consumed
        current: dict[float, float] = {}
        if hasattr(bids, 'apply_snapshot_from_wire'):
            for bid_price, bid_size in bids.irange(min_price=sim.price):
                current[bid_price] = bid_size
        else:
            for bid_price, bid_size in bids.items():
                if bid_price >= sim.price:
                    current[bid_price] = bid_size
        prev = sim._prev_by_price
        new_liq = 0.0
        for price, size in current.items():
            delta = size - prev.get(price, 0.0)
            if delta > 0:
                new_liq += delta
        new_liq -= consumed
        sim._prev_by_price = current
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

        # Maker fee
        fee = fill_size * fill_price * self._maker_fee_rate
        self._realized_pnl -= fee
        realized_delta -= fee

        # Bookkeeping
        self._fill_count += 1
        self._total_volume += fill_size * fill_price

        # Capital impact: margin adjustment + realized PnL
        # On a position flip (e.g. long 0.5 → sell 1.0), only reduce_qty
        # releases margin; the excess opens the opposite side and consumes margin.
        if self._is_reducing(sim.side, old_pos):
            reduce_qty = min(fill_size, abs(old_pos))
            excess = fill_size - reduce_qty
            # Release margin for the closing portion + credit realized PnL
            margin_release = reduce_qty * self._entry_price_before / self._leverage
            self._state.account.available_capital += margin_release + realized_delta
            # Consume margin for the flipping portion (new opposite position)
            if excess > 1e-12:
                self._state.account.available_capital -= excess * fill_price / self._leverage
        else:
            # Purely increasing position
            self._state.account.available_capital -= fill_size * fill_price / self._leverage + fee

        # Keep portfolio_value in sync: initial portfolio + realized + unrealized
        self._state.account.portfolio_value = (
            self._initial_portfolio_value + self._realized_pnl + self.unrealized_pnl
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
            self._om._clear_live(sim.side, sim.level)
            label = "FILLED"
        else:
            self._om._bind_live(sim.side, sim.client_order_id, sim.price, sim.size, level=sim.level)
            label = "PARTIAL"

        self._log.info(
            "DRY-RUN %s %s L%d: %.6f @ $%.2f | pos=%.6f | realized=$%.4f | unrealized=$%.4f",
            label, sim.side.upper(), sim.level, fill_size, fill_price,
            self._position, self._realized_pnl, self.unrealized_pnl,
        )

        # Trade log (buffer only — no disk I/O on hot path)
        if self._trade_logger is not None:
            self._trade_logger.log_fill(
                side=sim.side,
                price=fill_price,
                size=fill_size,
                level=sim.level,
                position_after=self._position,
                realized_pnl=self._realized_pnl,
                available_capital=self._state.account.available_capital,
                portfolio_value=self._state.account.portfolio_value,
                simulated=True,
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
        # Refresh mark-to-market portfolio value (unrealized changes with mid)
        self._state.account.portfolio_value = (
            self._initial_portfolio_value + self._realized_pnl + self.unrealized_pnl
        )
        mid = self._state.market.mid_price or 0.0
        self._log.info(
            "DRY-RUN SUMMARY | pos=%.6f | entry=$%.2f | mid=$%.2f | "
            "realized=$%.4f | unrealized=$%.4f | total=$%.4f | "
            "fills=%d | volume=$%.2f | live_orders=%d",
            self._position, self._entry_vwap, mid,
            self._realized_pnl, self.unrealized_pnl, self.total_pnl,
            self._fill_count, self._total_volume, len(self._live_orders),
        )
        # Schedule disk I/O on thread pool — never block the event loop
        self._flush_to_disk_async()

    def _flush_to_disk_sync(self) -> None:
        """Disk I/O: save state + flush trade log.  Meant to run on a worker thread."""
        self.save_state()
        if self._trade_logger is not None:
            self._trade_logger.flush()

    def _flush_to_disk_async(self) -> None:
        """Snapshot state on the event-loop thread, then dispatch I/O to executor.

        Capturing the data dict here (on the same thread as ``_process_fill``)
        guarantees the snapshot is internally consistent — no fill can
        interleave between field reads.
        """
        try:
            import asyncio
            loop = asyncio.get_running_loop()
            data = self._build_save_data()  # atomic w.r.t. _process_fill
            tl = self._trade_logger
            def _write():
                self._write_save_data(data)
                if tl is not None:
                    tl.flush()
            loop.run_in_executor(None, _write)
        except RuntimeError:
            # No running loop (e.g. during tests) — flush synchronously
            self._flush_to_disk_sync()
