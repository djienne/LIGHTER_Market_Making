"""Regression tests for the 2026-07 code-review fixes.

Covers:
- A3: a hinted delta must not seed an uninitialized book
- A4: delta application is atomic on malformed input (Cython + fallback)
- A1/A2: Binance depth feed survives snapshot failures and re-syncs on gaps
- B1: CJ estimator snapshot is computed lazily, not per book tick
- B2: LiveMetricsTracker flush is guarded and re-buffers on failure
- D1: grid param keys are unique across every sweepable parameter
"""

import asyncio
import csv
import dataclasses
import time
import unittest
from unittest.mock import patch

import pytest

from orderbook import apply_orderbook_update


# ---------------------------------------------------------------------------
# A3 — uninitialized book + explicit delta hint
# ---------------------------------------------------------------------------

class TestUninitializedDeltaSkip(unittest.TestCase):
    def test_delta_hint_on_uninitialized_book_is_skipped(self):
        bids, asks = {}, {}
        bids_in = [{"price": "100", "size": "1"}]

        is_snapshot = apply_orderbook_update(
            bids, asks, False, bids_in, [], is_snapshot_hint=False,
        )

        self.assertFalse(is_snapshot)
        self.assertEqual(bids, {})   # nothing applied — a delta cannot seed a book
        self.assertEqual(asks, {})

    def test_snapshot_hint_on_uninitialized_book_still_applies(self):
        bids, asks = {}, {}
        is_snapshot = apply_orderbook_update(
            bids, asks, False,
            [{"price": "100", "size": "1"}], [{"price": "101", "size": "2"}],
            is_snapshot_hint=True,
        )
        self.assertTrue(is_snapshot)
        self.assertEqual(bids, {100.0: 1.0})
        self.assertEqual(asks, {101.0: 2.0})


# ---------------------------------------------------------------------------
# A4 — atomic delta application on malformed input
# ---------------------------------------------------------------------------

class TestAtomicDeltas(unittest.TestCase):
    def test_fallback_delta_with_invalid_level_leaves_book_unchanged(self):
        bids = {100.0: 1.0}
        asks = {101.0: 1.0}
        # First level valid, second invalid — nothing may be applied.
        bids_in = [
            {"price": "99", "size": "5"},
            {"price": "98", "size": "-1"},
        ]
        with self.assertRaises(ValueError):
            apply_orderbook_update(bids, asks, True, bids_in, [])
        self.assertEqual(bids, {100.0: 1.0})
        self.assertEqual(asks, {101.0: 1.0})

    def test_cbookside_delta_with_invalid_level_leaves_book_unchanged(self):
        _vol_obi_fast = pytest.importorskip("_vol_obi_fast")
        side = _vol_obi_fast.CBookSide()
        side.apply_snapshot_from_wire([{"price": "100", "size": "1"}])

        with self.assertRaises(ValueError):
            side.apply_delta_from_wire([
                {"price": "99", "size": "5"},      # valid
                {"price": "nan", "size": "1"},     # invalid — must abort whole delta
            ])

        self.assertEqual(len(side), 1)
        self.assertEqual(side[100.0], 1.0)
        self.assertNotIn(99.0, side)

    def test_cbookside_binance_delta_atomic(self):
        _vol_obi_fast = pytest.importorskip("_vol_obi_fast")
        side = _vol_obi_fast.CBookSide()
        side.apply_snapshot_from_binance([["100", "1"]])

        with self.assertRaises(ValueError):
            side.apply_delta_from_binance([["99", "5"], ["-1", "1"]])

        self.assertEqual(len(side), 1)
        self.assertNotIn(99.0, side)


# ---------------------------------------------------------------------------
# A1/A2 — Binance depth feed resilience
# ---------------------------------------------------------------------------

class _FakeWs:
    """Minimal fake websocket: recv() serves queued frames then blocks."""

    def __init__(self, frames):
        self._frames = list(frames)

    async def recv(self, *args, **kwargs):
        if self._frames:
            return self._frames.pop(0)
        await asyncio.Future()  # block until cancelled

    async def send(self, *_args, **_kwargs):
        pass

    async def close(self):
        pass


class _FakeConnectCM:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return False


class TestBinanceDepthResilience(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_failure_reconnects_instead_of_dying(self):
        """A transient REST snapshot failure must never terminate run()."""
        from binance_obi import BinanceDiffDepthClient, SharedAlpha

        client = BinanceDiffDepthClient(
            "btcusdt", SharedAlpha(),
            reconnect_base=0.01, reconnect_max=0.02,
        )
        connect_count = 0

        def _connect(*a, **kw):
            nonlocal connect_count
            connect_count += 1
            return _FakeConnectCM(_FakeWs([]))

        with patch("websockets.connect", side_effect=_connect), \
                patch.object(client, "_fetch_snapshot",
                             side_effect=RuntimeError("HTTP 503")):
            task = asyncio.create_task(client.run())
            await asyncio.sleep(0.4)
            still_running = not task.done()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertTrue(still_running, "run() exited — feed died permanently")
        self.assertGreaterEqual(connect_count, 2, "no reconnect attempted")

    async def test_gap_after_snapshot_triggers_resync_not_silent_apply(self):
        """First event with U > lastUpdateId+1 must resync, not corrupt the book."""
        from binance_obi import BinanceDiffDepthClient, SharedAlpha

        client = BinanceDiffDepthClient(
            "btcusdt", SharedAlpha(),
            reconnect_base=0.01, reconnect_max=0.02,
        )
        connect_count = 0
        # Gap event: U=150 > lastUpdateId+1=101
        gap_event = '{"U": 150, "u": 160, "pu": 140, "b": [["1.0", "9"]], "a": []}'

        def _connect(*a, **kw):
            nonlocal connect_count
            connect_count += 1
            return _FakeConnectCM(_FakeWs([gap_event]))

        snapshot = {"lastUpdateId": 100, "bids": [], "asks": []}
        with patch("websockets.connect", side_effect=_connect), \
                patch.object(client, "_fetch_snapshot", return_value=snapshot):
            task = asyncio.create_task(client.run())
            await asyncio.sleep(0.6)
            still_running = not task.done()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertTrue(still_running)
        self.assertGreaterEqual(connect_count, 2, "gap did not trigger a resync")
        # The gapped delta must never have been applied to the book
        self.assertNotIn(1.0, client._bids)


# ---------------------------------------------------------------------------
# B1 — CJ estimator lazy snapshot
# ---------------------------------------------------------------------------

class TestEstimatorLazySnapshot(unittest.TestCase):
    def test_on_book_update_does_not_recompute_snapshot(self):
        from lighter_estimators import LighterCJEstimator

        estimator = LighterCJEstimator(snapshot_min_interval=0.0)
        baseline = estimator._last_snapshot
        for i in range(50):
            estimator.on_book_update(100.0 + i * 0.01)
        # The cached snapshot object is untouched by book updates...
        self.assertIs(estimator._last_snapshot, baseline)
        # ...and only snapshot() computes a fresh one.
        fresh = estimator.snapshot()
        self.assertIsNot(fresh, baseline)
        self.assertEqual(fresh.sample_count, 50)

    def test_snapshot_respects_min_interval_cache(self):
        from lighter_estimators import LighterCJEstimator

        estimator = LighterCJEstimator(snapshot_min_interval=60.0)
        first = estimator.snapshot()
        estimator.on_book_update(100.0)
        second = estimator.snapshot()  # inside the interval → cached
        self.assertIs(first, second)


# ---------------------------------------------------------------------------
# B2 — guarded live-metrics flush
# ---------------------------------------------------------------------------

class TestLiveMetricsGuardedFlush(unittest.TestCase):
    def _make_tracker(self, tmpdir):
        from live_metrics import LiveMetricsTracker
        return LiveMetricsTracker(
            tmpdir, "TEST",
            horizons=[0.01], adaptive_horizon=0.01,
            metrics_flush_seconds=1,
        )

    def _record_and_settle(self, tracker):
        tracker.record_fill(
            fill_id="f1", side="buy", price=100.0, size=1.0,
            mid_at_fill=100.05, spread_capture_bps=5.0, position_after=1.0,
            realized_delta_usd=0.0, realized_pnl_cumulative=0.0,
            fill_source="test",
        )
        time.sleep(0.02)
        tracker.update(
            mid_price=99.9, position_size=1.0, max_pos_usd=200.0,
            realized_pnl_cumulative=0.0, portfolio_value=1000.0,
            available_capital=900.0,
        )

    def test_update_buffers_rows_without_disk_io(self, tmp_path=None):
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = self._make_tracker(tmpdir)
            self._record_and_settle(tracker)
            # Rows buffered in memory, not yet on disk
            self.assertEqual(len(tracker._markout_rows), 1)
            with open(tracker.markout_path) as f:
                self.assertEqual(len(list(csv.DictReader(f))), 0)

    def test_failed_flush_rebuffers_and_does_not_raise(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = self._make_tracker(tmpdir)
            self._record_and_settle(tracker)
            good_path = tracker.markout_path
            tracker.markout_path = tmpdir  # opening a directory → OSError

            # Must not raise (a locked/unwritable file must never stop quoting)
            tracker.flush_metrics(
                mid_price=99.9, position_size=1.0, max_pos_usd=200.0,
                realized_pnl_cumulative=0.0, portfolio_value=1000.0,
                available_capital=900.0,
            )
            self.assertEqual(len(tracker._markout_rows), 1)  # re-buffered

            # Restore the path — next flush writes the buffered row
            tracker.markout_path = good_path
            tracker.flush_metrics(
                mid_price=99.9, position_size=1.0, max_pos_usd=200.0,
                realized_pnl_cumulative=0.0, portfolio_value=1000.0,
                available_capital=900.0,
            )
            self.assertEqual(len(tracker._markout_rows), 0)
            with open(tracker.markout_path) as f:
                self.assertEqual(len(list(csv.DictReader(f))), 1)


# ---------------------------------------------------------------------------
# D1 — grid param key uniqueness
# ---------------------------------------------------------------------------

class TestGridParamKeyUniqueness(unittest.TestCase):
    def test_every_field_changes_the_key(self):
        from grid_dry_run import GridParams, _param_key

        base = GridParams()
        base_key = _param_key(base)

        alt_values = {
            "quote_engine": "cartea_jaimungal",
            "cj_solver_mode": "symmetric",
            "cj_use_estimator": True,
        }
        for f in dataclasses.fields(GridParams):
            if f.name == "label":
                continue
            value = getattr(base, f.name)
            if f.name in alt_values:
                new_value = alt_values[f.name]
            elif isinstance(value, bool):
                new_value = not value
            elif isinstance(value, int):
                new_value = value + 1
            elif isinstance(value, float):
                new_value = value + 0.1237
            else:
                self.fail(f"unhandled field type for {f.name}: {type(value)}")
            variant = dataclasses.replace(base, **{f.name: new_value})
            self.assertNotEqual(
                _param_key(variant), base_key,
                f"changing {f.name!r} did not change the param key "
                "(state files would collide)",
            )

    def test_label_does_not_change_the_key(self):
        from grid_dry_run import GridParams, _param_key

        a = GridParams(label="s000")
        b = GridParams(label="s001")
        self.assertEqual(_param_key(a), _param_key(b))


if __name__ == "__main__":
    unittest.main()
