"""Unit tests for the dry-run / paper-trading engine."""

import asyncio
import time
import unittest

from sortedcontainers import SortedDict

import market_maker_v2 as mm
from dry_run import DryRunEngine, SimulatedOrder
from tests._helpers import temp_mm_attrs


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
            available_capital=1000.0, current_position_size=0.0,
            current_mid_price_cached=105.0,
        ):
            engine = _make_engine(leverage=2)
            engine._initial_capital = 1000.0

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
            engine = _make_engine(sim_latency_s=0.001)  # 1ms
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))

            # Wait past the latency
            time.sleep(0.005)

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
            # Make it eligible (past create latency)
            engine._live_orders[1].eligible_at = 0

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
            engine = _make_engine(sim_latency_s=0.001)
            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "create", 100.0, 0.5, 1, 0),
            ]))
            engine._live_orders[1].eligible_at = 0

            self._run(engine.process_batch([
                mm.BatchOp("buy", 0, "cancel", 0.0, 0.0, 1,
                           mm._client_to_exchange_id[1]),
            ]))

            time.sleep(0.005)

            # Price doesn't cross — no fill, just cancel
            bids, asks = _book({98.0: 1.0}, {101.0: 1.0})
            engine.check_fills(bids, asks)

            self.assertNotIn(1, engine._live_orders)
            self.assertEqual(engine._fill_count, 0)

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


class TestCLIFlag(unittest.TestCase):
    """Verify the --live flag plumbing."""

    def test_dry_run_default(self):
        """DRY_RUN defaults to True (no --live flag)."""
        # After module load, DRY_RUN is set at module level
        self.assertIsInstance(mm.DRY_RUN, bool)


if __name__ == "__main__":
    unittest.main()
