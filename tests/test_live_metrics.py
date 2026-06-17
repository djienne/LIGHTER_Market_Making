import csv
import json
import time

from live_metrics import LiveMetricsTracker, LiveStateStore


def test_live_state_store_roundtrips(tmp_path):
    store = LiveStateStore(str(tmp_path), "BTC")
    store.save({"realized_pnl_cumulative": 1.23, "fill_count": 4})

    payload = store.load()

    assert payload["symbol"] == "BTC"
    assert payload["realized_pnl_cumulative"] == 1.23
    assert payload["fill_count"] == 4
    assert "updated_at" in payload


def test_markout_tracker_writes_adverse_markout_and_metrics(tmp_path):
    tracker = LiveMetricsTracker(
        str(tmp_path),
        "BTC",
        horizons=[0.01],
        adaptive_horizon=0.01,
        adverse_threshold_bps=1.0,
        spread_widen_per_bps=0.1,
        size_reduce_per_bps=0.1,
        metrics_flush_seconds=1,
    )
    for idx in range(4):
        tracker.record_fill(
            fill_id=f"fill-{idx}",
            side="buy",
            price=100.0,
            size=1.0,
            mid_at_fill=100.05,
            spread_capture_bps=5.0,
            position_after=1.0,
            realized_delta_usd=0.0,
            realized_pnl_cumulative=0.0,
            fill_source="test",
        )
    time.sleep(0.02)

    adjustment = tracker.update(
        mid_price=99.8,
        position_size=1.0,
        max_pos_usd=200.0,
        realized_pnl_cumulative=0.0,
        portfolio_value=1000.0,
        available_capital=900.0,
    )
    tracker.flush_metrics(
        mid_price=99.8,
        position_size=1.0,
        max_pos_usd=200.0,
        realized_pnl_cumulative=0.0,
        portfolio_value=1000.0,
        available_capital=900.0,
    )

    with open(tracker.markout_path) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 4
    assert float(rows[0]["markout_bps"]) < 0
    assert float(rows[0]["adverse_bps"]) > 0
    assert adjustment.reason == "adverse_markout"
    assert adjustment.spread_multiplier > 1.0
    assert adjustment.size_multiplier < 1.0

    with open(tracker.metrics_path) as f:
        metrics = json.load(f)
    assert metrics["fills"]["count"] == 4
    assert metrics["quality"]["adjustment"]["reason"] == "adverse_markout"
    assert metrics["markouts"]["0.01"]["count"] == 4
    assert metrics["markouts"]["0.01"]["by_side"]["buy"]["count"] == 4
    assert metrics["markouts"]["0.01"]["by_side"]["buy"]["adverse_avg_bps"] > 0
