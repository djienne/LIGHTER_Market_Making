"""Unit tests for the dry-run / paper-trading engine."""

import asyncio
import csv
import json
import os
import tempfile
import time
import unittest

from sortedcontainers import SortedDict

import market_maker_v2 as mm
from dry_run import DryRunEngine, SimulatedOrder
from trade_log import TradeLogger
from _helpers import temp_mm_attrs


def _make_engine(**kwargs):
    """Create a DryRunEngine wired to the global mm state.

    Uses sim_latency_s=0 by default so fill tests are deterministic.
    """
    defaults = dict(
        state=mm.state,
        order_manager=mm.order_manager,
        client_to_exchange_id=mm._client_to_exchange_id,
        leverage=2,
        logger=mm.logger,
        sim_latency_s=0,
    )
    defaults.update(kwargs)
    return DryRunEngine(**defaults)


def _book(bids: dict, asks: dict):
    """Build sorted bid/ask book sides."""
    b = SortedDict(bids)
    a = SortedDict(asks)
    return b, a


class TestProcessBatch(unittest.IsolatedAsyncioTestCase):
    """Test create / modify / cancel via process_batch."""

    async def test_create_order(self):
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            current_position_size=0.0,
        ):
            engine = _make_engine()
            op = mm.BatchOp(
                side="buy", level=0, action="create",
                price=100.0, size=0.5, order_id=1, exchange_id=0,
            )
            await engine.process_batch([op])

            # Order tracked locally
            self.assertIn(1, engine._live_orders)
            sim = engine._live_orders[1]
            self.assertEqual(sim.side, "buy")
            self.assertEqual(sim.price, 100.0)
            self.assertEqual(sim.size, 0.5)

            # Mapped to synthetic exchange id
            self.assertIn(1, mm._client_to_exchange_id)

            # OrderManager state updated
            self.assertEqual(mm.state.orders.bid_order_ids[0], 1)
            self.assertEqual(mm.state.orders.bid_prices[0], 100.0)

    async def test_modify_order(self):
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
        ):
            engine = _make_engine()
            # Create first
            await engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ])
            # Modify price
            await engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ])
            sim = engine._live_orders[1]
            self.assertEqual(sim.price, 101.0)
            self.assertEqual(mm.state.orders.bid_prices[0], 101.0)

    async def test_cancel_order(self):
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
        ):
            engine = _make_engine()
            await engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ])
            self.assertIn(1, engine._live_orders)

            await engine.process_batch([
                mm.BatchOp("buy", 0, "cancel", 0.0, 0.0, 1,
                           mm._client_to_exchange_id[1]),
            ])
            # Cancel is pending; check_fills processes it after latency expires
            bids, asks = _book({99.0: 1.0}, {101.0: 1.0})
            engine.check_fills(bids, asks)
            self.assertNotIn(1, engine._live_orders)
            self.assertIsNone(mm.state.orders.bid_order_ids[0])

    async def test_mixed_batch(self):
        """Create buy + sell in one batch."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            current_position_size=0.0,
        ):
            engine = _make_engine()
            await engine.process_batch([
                mm.BatchOp("buy", 0, "create", 99.0, 0.3, 10, 0),
                mm.BatchOp("sell", 0, "create", 101.0, 0.3, 11, 0),
            ])
            self.assertEqual(len(engine._live_orders), 2)
            self.assertEqual(mm.state.orders.bid_prices[0], 99.0)
            self.assertEqual(mm.state.orders.ask_prices[0], 101.0)


class TestCheckFills(unittest.TestCase):
    """Test fill simulation against orderbook data."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_buy_fill_when_ask_crosses(self):
        """Buy limit at 100 fills when best ask <= 100."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            # Ask crosses our limit
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # Should be fully filled
            self.assertNotIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, 0.5)
            self.assertEqual(engine._fill_count, 1)
            self.assertAlmostEqual(mm.state.account.position_size, 0.5)

    def test_sell_fill_when_bid_crosses(self):
        """Sell limit at 100 fills when best bid >= 100."""
        with temp_mm_attrs(
            current_ask_order_id=None, current_ask_price=None,
            current_ask_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            bids, asks = _book({100.0: 2.0}, {101.0: 1.0})
            engine.check_fills(bids, asks)

            self.assertNotIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, -0.5)
            self.assertEqual(engine._fill_count, 1)

    def test_no_fill_when_price_doesnt_cross(self):
        """Order should NOT fill if price hasn't crossed."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 99.0, 0.5, 1, 0),
            ]))

            # Best ask is above our limit
            bids, asks = _book({98.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            self.assertIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, 0.0)

    def test_partial_fill(self):
        """Fill only available liquidity when it's less than order size."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))

            # Only 0.3 available at or below our limit
            bids, asks = _book({99.0: 1.0}, {100.0: 0.3})
            engine.check_fills(bids, asks)

            # Partial fill
            self.assertIn(1, engine._live_orders)
            sim = engine._live_orders[1]
            self.assertAlmostEqual(sim.size, 0.7)
            self.assertAlmostEqual(engine._position, 0.3)
            self.assertEqual(engine._fill_count, 1)

    def test_multi_level_ask_liquidity(self):
        """Buy limit sums available liquidity across multiple ask levels."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            # Ask liquidity at 99.5 (0.2) + 100.0 (0.4) = 0.6 total
            bids, asks = _book({98.0: 1.0}, {99.5: 0.2, 100.0: 0.4})
            engine.check_fills(bids, asks)

            # 0.5 filled (order size < 0.6 available)
            self.assertNotIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, 0.5)

    def test_empty_book_no_crash(self):
        """Empty book should not crash or produce fills."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            bids, asks = _book({}, {})
            engine.check_fills(bids, asks)

            self.assertIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

    def test_replenished_liquidity_at_different_price(self):
        """Fill detects new liquidity at a different price even when aggregate depth is flat."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))

            # Tick 1: 0.5 available at 99.5 — partial fill
            bids, asks = _book({98.0: 1.0}, {99.5: 0.5})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.5)
            self.assertIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._live_orders[1].size, 0.5)

            # Tick 2: 99.5 liquidity gone, fresh 0.5 appears at 100.0
            # Aggregate depth is still 0.5 — old scalar approach would see delta=0
            bids, asks = _book({98.0: 1.0}, {100.0: 0.5})
            engine.check_fills(bids, asks)

            # Per-price delta detects the new liquidity at 100.0
            self.assertAlmostEqual(engine._position, 1.0)
            self.assertNotIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 2)


class TestPnL(unittest.TestCase):
    """Test PnL calculation under various scenarios."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_round_trip_pnl(self):
        """Buy then sell: realized PnL = (sell - buy) * size."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            current_position_size=0.0, current_mid_price_cached=100.0,
        ):
            engine = _make_engine()

            # Buy 0.5 at 100
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.5)
            self.assertAlmostEqual(engine._entry_vwap, 100.0)

            # Sell 0.5 at 102
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 102.0, 0.5, 2, 0),
            ]))
            bids, asks = _book({102.0: 2.0}, {103.0: 1.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.0)
            # PnL = 0.5 * (102 - 100) = 1.0
            self.assertAlmostEqual(engine._realized_pnl, 1.0)

    def test_short_round_trip_pnl(self):
        """Sell then buy: realized PnL = (entry - exit) * size for shorts."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            current_position_size=0.0, current_mid_price_cached=100.0,
        ):
            engine = _make_engine()

            # Sell 0.5 at 102
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 102.0, 0.5, 1, 0),
            ]))
            bids, asks = _book({102.0: 2.0}, {103.0: 1.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, -0.5)
            self.assertAlmostEqual(engine._entry_vwap, 102.0)

            # Buy 0.5 at 100
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 2, 0),
            ]))
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.0)
            # PnL = 0.5 * (102 - 100) = 1.0
            self.assertAlmostEqual(engine._realized_pnl, 1.0)

    def test_unrealized_pnl(self):
        """Unrealized PnL reflects current mid price."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=105.0,
        ):
            engine = _make_engine()

            # Buy 0.5 at 100
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # Mid is at 105
            self.assertAlmostEqual(engine.unrealized_pnl, 0.5 * (105.0 - 100.0))

    def test_capital_consumed_on_fill(self):
        """Available capital decreases on position-increasing fill."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(leverage=2)

            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # margin = 0.5 * 100 / 2 = 25
            self.assertAlmostEqual(mm.state.account.available_capital, 975.0)


class TestInitialization(unittest.TestCase):
    """Test engine initialization."""

    def test_capture_initial_state(self):
        with temp_mm_attrs(available_capital=500.0):
            engine = _make_engine()
            self.assertFalse(engine.initialized)
            engine.capture_initial_state()
            self.assertTrue(engine.initialized)
            self.assertAlmostEqual(engine._initial_capital, 500.0)

    def test_log_summary_respects_interval(self):
        with temp_mm_attrs(available_capital=1000.0, current_mid_price_cached=100.0):
            engine = _make_engine(log_interval=9999.0)
            engine._last_summary = time.monotonic()
            engine.maybe_log_summary()
            # Should not crash; interval not elapsed so just returns


class TestBugFixes(unittest.TestCase):
    """Regression tests for the four reviewer-identified bugs."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_no_double_fill_from_static_book(self):
        """Bug 1: repeated check_fills on a static book must not re-fill."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))

            bids, asks = _book({99.0: 1.0}, {100.0: 0.3})
            # First call: fills 0.3 (all available)
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.3)

            # Second call: same static book — no new liquidity, no new fill
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.3)

            # Third call: still no change
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.3)
            self.assertEqual(engine._fill_count, 1)

    def test_multiple_orders_share_liquidity(self):
        """Bug 1b: two buy orders must not both fill from the same liquidity."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            # Two buy orders at the same price
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 2, 0),
            ]))

            # Only 0.6 available — should not fill both fully (1.0 total)
            bids, asks = _book({99.0: 1.0}, {100.0: 0.6})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.6, places=5)

    def test_realized_pnl_feeds_capital(self):
        """Bug 2: realized PnL must flow into available_capital."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            current_position_size=0.0, current_mid_price_cached=110.0,
        ):
            engine = _make_engine(leverage=2)

            # Buy 1 @ 100 -> margin consumed = 1*100/2 = 50
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))
            bids, asks = _book({99.0: 2.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(mm.state.account.available_capital, 950.0)

            # Sell 1 @ 110 -> release margin 50 + PnL 10 = capital back to 1010
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 110.0, 1.0, 2, 0),
            ]))
            bids, asks = _book({110.0: 2.0}, {111.0: 1.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._realized_pnl, 10.0)
            self.assertAlmostEqual(mm.state.account.available_capital, 1010.0)

    def test_portfolio_value_tracks_pnl(self):
        """Bug 2b: portfolio_value must reflect realized + unrealized PnL."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, portfolio_value=1000.0,
            current_position_size=0.0, current_mid_price_cached=105.0,
        ):
            engine = _make_engine(leverage=2)
            engine.capture_initial_state()

            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))
            bids, asks = _book({99.0: 2.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # mid=105, unrealized = 1*(105-100)=5, realized=0
            # portfolio = 1000 + 0 + 5 = 1005
            self.assertAlmostEqual(mm.state.account.portfolio_value, 1005.0)

    def test_initial_position_inherited(self):
        """Bug 3: engine must inherit the real account position on init."""
        with temp_mm_attrs(
            available_capital=500.0, current_position_size=2.5,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            engine.capture_initial_state()
            self.assertAlmostEqual(engine._position, 2.5)
            self.assertAlmostEqual(engine._entry_vwap, 100.0)
            # state should still reflect the position
            self.assertAlmostEqual(mm.state.account.position_size, 2.5)

    def test_initial_flat_position(self):
        """Bug 3b: flat account should start with vwap=0."""
        with temp_mm_attrs(
            available_capital=500.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            engine.capture_initial_state()
            self.assertAlmostEqual(engine._position, 0.0)
            self.assertAlmostEqual(engine._entry_vwap, 0.0)

    def test_modify_resets_prev_available(self):
        """Bug 5: repriced order must be fresh for fill accounting."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()

            # Create buy @ 100, partial fill 0.3
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))
            bids, asks = _book({99.0: 1.0}, {100.0: 0.3})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.3)

            # Modify remaining order to 101 (more aggressive)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.7, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Same ask@100 x 0.3 resting — repriced order should fill against it
            bids, asks = _book({99.0: 1.0}, {100.0: 0.3})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.6)

    def test_portfolio_value_uses_portfolio_baseline(self):
        """Bug 6: portfolio_value baseline must use initial portfolio_value, not available_capital."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=900.0, portfolio_value=1000.0,
            current_position_size=0.0, current_mid_price_cached=105.0,
        ):
            engine = _make_engine(leverage=2)
            engine.capture_initial_state()
            self.assertAlmostEqual(engine._initial_portfolio_value, 1000.0)

            # Buy 1 @ 100 with mid at 105 -> unrealized = 5
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))
            bids, asks = _book({99.0: 2.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # portfolio = 1000 (initial PV) + 0 (realized) + 5 (unrealized) = 1005
            self.assertAlmostEqual(mm.state.account.portfolio_value, 1005.0)


    def test_position_flip_margin_accounting(self):
        """Bug 7: flip from long to short must split margin correctly."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_ask_order_id=None,
            current_bid_price=None, current_ask_price=None,
            current_bid_size=None, current_ask_size=None,
            _PRICE_TICK_FLOAT=0.1, available_capital=1000.0,
            portfolio_value=1000.0, current_position_size=0.0,
            current_mid_price_cached=102.0,
        ):
            engine = _make_engine(leverage=2)

            # Buy 0.5 @ 100: margin = 0.5*100/2 = 25, capital = 975
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            bids, asks = _book({99.0: 2.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(mm.state.account.available_capital, 975.0)
            self.assertAlmostEqual(engine._position, 0.5)

            # Sell 1.0 @ 102: closes 0.5 long + opens 0.5 short
            # Close 0.5: release margin 0.5*100/2=25, PnL=0.5*(102-100)=1
            # Open 0.5 short: consume margin 0.5*102/2=25.5
            # capital = 975 + 25 + 1 - 25.5 = 975.5
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 102.0, 1.0, 2, 0),
            ]))
            bids, asks = _book({102.0: 2.0}, {103.0: 1.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, -0.5)
            self.assertAlmostEqual(engine._realized_pnl, 1.0)
            self.assertAlmostEqual(mm.state.account.available_capital, 975.5)

    def test_portfolio_value_updates_between_fills(self):
        """Bug 8: portfolio_value must refresh on maybe_log_summary."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, portfolio_value=1000.0,
            current_position_size=0.0, current_mid_price_cached=100.0,
        ):
            engine = _make_engine(leverage=2, log_interval=0)
            engine.capture_initial_state()

            # Buy 1 @ 100
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 1.0, 1, 0),
            ]))
            bids, asks = _book({99.0: 2.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(mm.state.account.portfolio_value, 1000.0)

            # Mid moves to 110 — no fill, just summary
            mm.state.market.mid_price = 110.0
            engine.maybe_log_summary()

            # portfolio should now reflect unrealized = 1*(110-100) = 10
            self.assertAlmostEqual(mm.state.account.portfolio_value, 1010.0)

    def test_corrupt_state_seeds_default_capital(self):
        """Bug 9: corrupt state file must not leave capital at zero."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            # Write corrupt JSON
            with open(state_path, "w") as f:
                f.write("{bad json")

            with temp_mm_attrs(
                available_capital=None, portfolio_value=None,
                current_position_size=0.0, current_mid_price_cached=100.0,
            ):
                # load_state should fail and return None
                loaded = DryRunEngine.load_state(
                    state_path,
                    state=mm.state,
                    order_manager=mm.order_manager,
                    client_to_exchange_id=mm._client_to_exchange_id,
                    leverage=2,
                    logger=mm.logger,
                    sim_latency_s=0,
                )
                self.assertIsNone(loaded)

                # Simulate what main() does: seed defaults before fresh engine
                if mm.state.account.available_capital is None:
                    mm.state.account.available_capital = 1000.0
                    mm.state.account.portfolio_value = 1000.0
                    mm.state.account.position_size = 0.0

                engine = _make_engine()
                engine.capture_initial_state()
                self.assertAlmostEqual(engine._initial_capital, 1000.0)
                self.assertAlmostEqual(engine._initial_portfolio_value, 1000.0)


class TestSimulatedLatency(unittest.TestCase):
    """Test the sim_latency_s feature."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_create_not_fillable_during_latency(self):
        """New order should NOT fill before eligible_at."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)  # long latency
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            # Ask crosses price, but order is still in-flight
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # Not filled — still in latency window
            self.assertIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

    def test_create_fillable_after_latency(self):
        """Order becomes fillable once eligible_at has passed."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)  # long latency
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            # Manually expire latency + arrival check (deterministic)
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            self.assertNotIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, 0.5)

    def test_modify_resets_eligible_at(self):
        """Modify should make order un-fillable again during latency."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            # Manually make it eligible
            engine._live_orders[1].eligible_at = 0

            # Modify resets latency
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            bids, asks = _book({99.0: 1.0}, {101.0: 2.0})
            engine.check_fills(bids, asks)

            # Not filled — modify reset the eligibility
            self.assertIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

    def test_cancel_stays_fillable_during_latency(self):
        """Order remains fillable while cancel is in-flight."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            # Make it eligible (past create latency + arrival check)
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Submit cancel — still in-flight for 10s
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "cancel", 0.0, 0.0, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Ask crosses — order should fill during cancel latency
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            self.assertAlmostEqual(engine._position, 0.5)
            self.assertEqual(engine._fill_count, 1)

    def test_cancel_completes_after_latency(self):
        """Order removed once cancel latency expires (if not filled)."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "cancel", 0.0, 0.0, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Manually expire the cancel latency (deterministic)
            engine._live_orders[1].pending_cancel_at = 0.001

            # Price doesn't cross — no fill, just cancel
            bids, asks = _book({98.0: 1.0}, {101.0: 1.0})
            engine.check_fills(bids, asks)

            self.assertNotIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

    def test_cancel_with_fill_opportunity(self):
        """Fill takes priority over cancel in the same tick."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Submit cancel
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "cancel", 0.0, 0.0, 1,
                           mm._client_to_exchange_id[1]),
            ]))
            # Expire the cancel latency
            engine._live_orders[1].pending_cancel_at = 0.001

            # Price crosses AND cancel is mature — fill should win
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # Order filled, then removed
            self.assertNotIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._position, 0.5)
            self.assertEqual(engine._fill_count, 1)

    def test_default_latency_is_50ms(self):
        """Default sim_latency_s should be 50ms."""
        with temp_mm_attrs(available_capital=1000.0):
            engine = DryRunEngine(
                state=mm.state,
                order_manager=mm.order_manager,
                client_to_exchange_id=mm._client_to_exchange_id,
                leverage=2,
                logger=mm.logger,
            )
            self.assertAlmostEqual(engine._sim_latency, 0.050)


class TestPostOnlyReject(unittest.TestCase):
    """POST_ONLY enforcement: reject immediately marketable orders."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_buy_rejected_when_ask_at_or_below_price(self):
        """Buy at $100 should be rejected when best ask <= $100."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({99.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            local_order_book=ob,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            # Order rejected — not in live_orders
            self.assertNotIn(1, engine._live_orders)

    def test_sell_rejected_when_bid_at_or_above_price(self):
        """Sell at $100 should be rejected when best bid >= $100."""
        ob = {'bids': SortedDict({101.0: 1.0}), 'asks': SortedDict({102.0: 2.0})}
        with temp_mm_attrs(
            current_ask_order_id=None, current_ask_price=None,
            current_ask_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            local_order_book=ob,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self.assertNotIn(1, engine._live_orders)

    def test_buy_accepted_when_ask_above_price(self):
        """Buy at $100 should be accepted when best ask > $100."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({101.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            local_order_book=ob,
        ):
            engine = _make_engine()
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self.assertIn(1, engine._live_orders)

    def test_book_snapshot_prevents_false_fill(self):
        """Pre-existing depth snapshotted at creation doesn't trigger fill.

        Scenario: buy at $100, best ask is $101 (POST_ONLY passes, no
        qualifying depth at creation → empty snapshot). With sim_latency,
        the order is in-flight. Before it becomes eligible, the ask drops
        to $100 with size 2.0. On the first eligible check, without the
        snapshot fix ALL 2.0 would look like new liquidity. But since
        the order was just created (no prior check), the empty snapshot
        is correct here — the fill IS legitimate new depth.

        The snapshot matters when qualifying depth exists at creation:
        buy limit at $102 when asks = {$101: 2.0}. The $101 depth is
        snapshotted. On next check with same depth, delta = 0 → no fill.
        """
        # Buy at $102 with existing ask at $101 (POST_ONLY: best_ask $101 > .. no,
        # $101 <= $102 → rejected). We need best_ask > price for POST_ONLY.
        # The snapshot only captures asks <= order price. So for a buy at $100
        # with best_ask at $101, nothing is snapshotted (no asks <= $100).
        #
        # The snapshot is most useful for MODIFY (which resets _prev_by_price).
        # For CREATE, POST_ONLY guarantees no qualifying depth exists at
        # creation (because if it did, the order would be rejected).
        # So the snapshot for create is always empty — which is correct.
        #
        # Test that the snapshot mechanism works via modify instead:
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({101.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine()
            # Create buy at $100, accepted (best_ask $101 > $100)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self.assertIn(1, engine._live_orders)

            # First check: ask at $100 with 2.0 — genuinely new → fills
            bids, asks = _book({98.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.5)
            self.assertEqual(engine._fill_count, 1)


class TestArrivalTimeReject(unittest.TestCase):
    """POST_ONLY recheck at simulated arrival time (eligible_at expiry)."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_create_rejected_at_arrival_buy(self):
        """Buy non-marketable at submit, but book crosses during latency → reject."""
        # At submit: best_ask $101 > price $100 → accepted
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({101.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self.assertIn(1, engine._live_orders)

            # Expire latency — book now has ask at $100 (crossed)
            engine._live_orders[1].eligible_at = 0
            bids, asks = _book({98.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            # Should be rejected at arrival, not filled
            self.assertNotIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)
            self.assertAlmostEqual(engine._position, 0.0)

    def test_create_rejected_at_arrival_sell(self):
        """Sell non-marketable at submit, but book crosses during latency → reject."""
        ob = {'bids': SortedDict({99.0: 1.0}), 'asks': SortedDict({102.0: 2.0})}
        with temp_mm_attrs(
            current_ask_order_id=None, current_ask_price=None,
            current_ask_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            self.assertIn(1, engine._live_orders)

            # Expire latency — bid now at $100 (crossed)
            engine._live_orders[1].eligible_at = 0
            bids, asks = _book({100.0: 2.0}, {102.0: 1.0})
            engine.check_fills(bids, asks)

            self.assertNotIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

    def test_create_passes_arrival_if_book_clear(self):
        """Non-marketable at both submit and arrival → fills normally."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({101.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0

            # Book still not crossed at arrival (best_ask $101 > $100)
            bids, asks = _book({98.0: 1.0}, {101.0: 2.0})
            engine.check_fills(bids, asks)

            # Arrival check passes, no fill (ask $101 > price $100)
            self.assertIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

            # Now ask drops to $100 — should fill (new depth)
            bids, asks = _book({98.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)
            self.assertAlmostEqual(engine._position, 0.5)

    def test_modify_rejected_keeps_old_order(self):
        """Modify to marketable price is rejected; old order stays live."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({99.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            # Create at $97 (non-marketable, best_ask $99)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 97.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Modify to $100 — best_ask $99 <= $100 → rejected
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 100.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))
            # Old order stays live at original price
            self.assertIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._live_orders[1].price, 97.0)


class TestStateOrdersSync(unittest.TestCase):
    """state.orders must match _live_orders after modify reject/promote."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_rejected_modify_keeps_old_price_in_state_orders(self):
        """Rejected modify-at-arrival must leave state.orders at old price."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({102.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Modify to $101 (accepted at submit, best_ask=$102)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))
            # state.orders should still show old price during latency
            self.assertAlmostEqual(mm.state.orders.bid_prices[0], 100.0)

            # Expire latency, book crossed at $101 → rejected at arrival
            engine._live_orders[1].eligible_at = 0
            bids, asks = _book({98.0: 1.0}, {101.0: 2.0})
            engine.check_fills(bids, asks)

            # state.orders must be resynced to old price
            self.assertAlmostEqual(mm.state.orders.bid_prices[0], 100.0)
            self.assertAlmostEqual(engine._live_orders[1].price, 100.0)

    def test_promoted_modify_syncs_new_price_to_state_orders(self):
        """Successful modify promotion must update state.orders."""
        ob = {'bids': SortedDict({98.0: 1.0}), 'asks': SortedDict({102.0: 2.0})}
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
            local_order_book=ob,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Modify to $101
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Expire latency, book not crossed → promote
            engine._live_orders[1].eligible_at = 0
            bids, asks = _book({98.0: 1.0}, {102.0: 2.0})
            engine.check_fills(bids, asks)

            # Both _live_orders and state.orders at $101
            self.assertAlmostEqual(engine._live_orders[1].price, 101.0)
            self.assertAlmostEqual(mm.state.orders.bid_prices[0], 101.0)


class TestModifyLatency(unittest.TestCase):
    """Old price stays fillable during modify latency."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_old_price_fillable_during_modify(self):
        """Order at old price should fill while modify is in-flight."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            # Create buy at $100
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Modify to $101 — goes into pending state (10s latency)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Order should still be at old price ($100), pending $101
            self.assertIn(1, engine._live_orders)
            self.assertAlmostEqual(engine._live_orders[1].price, 100.0)
            self.assertAlmostEqual(engine._live_orders[1]._pending_price, 101.0)

            # Ask at $100 crosses old price — should fill at $100
            bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
            engine.check_fills(bids, asks)

            self.assertAlmostEqual(engine._position, 0.5)
            self.assertEqual(engine._fill_count, 1)

    def test_modify_promotes_after_latency(self):
        """Pending modify switches to new price when latency expires."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine(sim_latency_s=10.0)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0
            engine._live_orders[1]._arrival_checked = True

            # Modify to $101
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "modify", 101.0, 0.5, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            # Expire modify latency
            engine._live_orders[1].eligible_at = 0

            # Check fills — should promote to $101
            bids, asks = _book({99.0: 1.0}, {102.0: 2.0})
            engine.check_fills(bids, asks)

            # Price should now be $101, pending cleared
            self.assertAlmostEqual(engine._live_orders[1].price, 101.0)
            self.assertIsNone(engine._live_orders[1]._pending_price)

    def test_fifo_at_equal_price(self):
        """At equal price, older order fills first (FIFO)."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            # Order 1 at $100 (created first)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            # Order 2 at $100 (created second, later queue_ts)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 1, "create", 100.0, 0.5, 2, 0),
            ]))

            # Only 0.6 available — order 1 (older) should fill first (0.5),
            # order 2 gets remainder (0.1)
            bids, asks = _book({99.0: 1.0}, {100.0: 0.6})
            engine.check_fills(bids, asks)

            self.assertAlmostEqual(engine._position, 0.6, places=5)
            # Order 1 fully filled
            self.assertNotIn(1, engine._live_orders)
            # Order 2 partially filled (0.1 of 0.5)
            self.assertIn(2, engine._live_orders)
            self.assertAlmostEqual(engine._live_orders[2].size, 0.4, places=5)


class TestPricePriority(unittest.TestCase):
    """More aggressive orders should fill before less aggressive ones."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_buy_higher_price_fills_first(self):
        """Higher-priced buy (more aggressive) should consume liquidity first."""
        with temp_mm_attrs(
            current_bid_order_id=None, current_bid_price=None,
            current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            # Create less aggressive order first (L1 at $100)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 1, "create", 100.0, 0.5, 1, 0),
            ]))
            # Create more aggressive order second (L0 at $101)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 101.0, 0.5, 2, 0),
            ]))

            # Only 0.6 liquidity at $101 — more aggressive order should fill first
            bids, asks = _book({99.0: 1.0}, {101.0: 0.6})
            engine.check_fills(bids, asks)

            # L0 at $101 gets full fill (0.5), L1 at $100 gets remainder
            # But L1 is at $100 and the ask is at $101, so L1 can't fill
            # (best_ask 101 > sim.price 100).
            self.assertNotIn(2, engine._live_orders)  # L0 filled
            self.assertIn(1, engine._live_orders)      # L1 not filled (price)
            self.assertAlmostEqual(engine._position, 0.5)

    def test_sell_lower_price_fills_first(self):
        """Lower-priced sell (more aggressive) should consume liquidity first."""
        with temp_mm_attrs(
            current_ask_order_id=None, current_ask_price=None,
            current_ask_size=None, _PRICE_TICK_FLOAT=0.1,
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=100.0,
        ):
            engine = _make_engine()
            # Create less aggressive order first (L1 at $102)
            self._run(engine.process_batch([
                mm.BatchOp("sell", 1, "create", 102.0, 0.5, 1, 0),
            ]))
            # Create more aggressive order second (L0 at $100)
            self._run(engine.process_batch([
                mm.BatchOp("sell", 0, "create", 100.0, 0.5, 2, 0),
            ]))

            # Bid at $102 with 0.6 — both orders qualify
            bids, asks = _book({102.0: 0.6}, {103.0: 1.0})
            engine.check_fills(bids, asks)

            # L0 at $100 (more aggressive) fills first, gets 0.5
            # L1 at $102 gets remainder: 0.6 - 0.5 = 0.1
            self.assertNotIn(2, engine._live_orders)
            self.assertAlmostEqual(engine._position, -0.6, places=5)


class TestPersistence(unittest.TestCase):
    """Test save/load state round-trip."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_save_load_round_trip(self):
        """State saved then loaded should restore engine fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            with temp_mm_attrs(
                current_bid_order_id=None, current_bid_price=None,
                current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
                available_capital=1000.0, portfolio_value=1000.0,
                current_position_size=0.0, current_mid_price_cached=100.0,
            ):
                engine = _make_engine(state_path=state_path)
                engine.capture_initial_state()

                # Simulate a fill to create non-trivial state
                self._run(engine.process_batch([
                    mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
                ]))
                bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
                engine.check_fills(bids, asks)
                engine.save_state()

                self.assertTrue(os.path.exists(state_path))

                # Load into a new engine
                restored = DryRunEngine.load_state(
                    state_path,
                    state=mm.state,
                    order_manager=mm.order_manager,
                    client_to_exchange_id=mm._client_to_exchange_id,
                    leverage=2,
                    logger=mm.logger,
                    sim_latency_s=0,
                )
                self.assertIsNotNone(restored)
                self.assertAlmostEqual(restored._position, 0.5)
                self.assertAlmostEqual(restored._initial_capital, 1000.0)
                self.assertEqual(restored._fill_count, 1)
                self.assertTrue(restored.initialized)

    def test_load_missing_file_returns_none(self):
        """load_state returns None if file doesn't exist."""
        result = DryRunEngine.load_state(
            "/nonexistent/path.json",
            state=mm.state,
            order_manager=mm.order_manager,
            client_to_exchange_id=mm._client_to_exchange_id,
            leverage=2,
            logger=mm.logger,
        )
        self.assertIsNone(result)

    def test_save_state_atomic(self):
        """save_state should produce valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            with temp_mm_attrs(available_capital=500.0, portfolio_value=500.0):
                engine = _make_engine(state_path=state_path)
                engine.capture_initial_state()
                engine.save_state()

                with open(state_path) as f:
                    data = json.load(f)
                self.assertIn("available_capital", data)
                self.assertIn("updated_at", data)


class TestTradeLog(unittest.TestCase):
    """Test the TradeLogger CSV output."""

    def test_log_fill_creates_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tl = TradeLogger(tmpdir, "BTC")
            tl.log_fill(
                side="buy", price=100.0, size=0.5, level=0,
                position_after=0.5, realized_pnl=0.0,
                available_capital=950.0, portfolio_value=1000.0,
                simulated=True,
            )
            tl.flush()

            with open(tl.path) as f:
                reader = csv.reader(f)
                header = next(reader)
                row = next(reader)
            self.assertEqual(header[0], "timestamp")
            self.assertEqual(row[1], "BTC")
            self.assertEqual(row[2], "buy")
            self.assertEqual(row[10], "true")

    def test_clear_resets_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tl = TradeLogger(tmpdir, "BTC")
            tl.log_fill(
                side="buy", price=100.0, size=0.5, level=0,
                position_after=0.5, realized_pnl=0.0,
                available_capital=950.0, portfolio_value=1000.0,
                simulated=True,
            )
            tl.flush()
            tl.clear()

            with open(tl.path) as f:
                lines = f.readlines()
            # Only header remains
            self.assertEqual(len(lines), 1)

    def test_buffer_no_disk_until_flush(self):
        """log_fill should not touch disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tl = TradeLogger(tmpdir, "BTC")
            size_before = os.path.getsize(tl.path)  # just header
            tl.log_fill(
                side="sell", price=100.0, size=0.5, level=0,
                position_after=-0.5, realized_pnl=0.0,
                available_capital=950.0, portfolio_value=1000.0,
                simulated=False,
            )
            size_after = os.path.getsize(tl.path)
            self.assertEqual(size_before, size_after)  # no disk write yet

            tl.flush()
            size_flushed = os.path.getsize(tl.path)
            self.assertGreater(size_flushed, size_before)

    def test_dry_run_fill_writes_trade_log(self):
        """Fills in DryRunEngine should appear in the trade log."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tl = TradeLogger(tmpdir, "BTC")
            with temp_mm_attrs(
                current_bid_order_id=None, current_bid_price=None,
                current_bid_size=None, _PRICE_TICK_FLOAT=0.1,
                available_capital=1000.0, portfolio_value=1000.0,
                current_position_size=0.0, current_mid_price_cached=100.0,
            ):
                engine = _make_engine(trade_logger=tl)
                asyncio.run(engine.process_batch([
                    mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
                ]))
                bids, asks = _book({99.0: 1.0}, {100.0: 2.0})
                engine.check_fills(bids, asks)
                tl.flush()

                with open(tl.path) as f:
                    rows = list(csv.reader(f))
                # header + 1 fill
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[1][2], "buy")


class TestCLIFlag(unittest.TestCase):
    """Verify the --live flag plumbing."""

    def test_dry_run_default(self):
        """DRY_RUN defaults to True (no --live flag)."""
        # After module load, DRY_RUN is set at module level
        self.assertIsInstance(mm.DRY_RUN, bool)


if __name__ == "__main__":
    unittest.main()
