import time

from lighter_estimators import LighterCJEstimator


def test_lighter_cj_estimator_becomes_ready_after_trades_and_markouts():
    estimator = LighterCJEstimator(
        window_seconds=60,
        markout_seconds=0.01,
        min_trades_per_side=2,
        min_markouts_per_side=2,
        default_lambda=0.3,
        default_kappa=0.035,
        default_epsilon=4.0,
    )

    estimator.on_book_update(100.0)
    estimator.on_trade({"trade_id": 1, "type": "buy", "price": "100.2", "size": "1"}, 100.0)
    estimator.on_trade({"trade_id": 2, "type": "buy", "price": "100.3", "size": "1"}, 100.0)
    estimator.on_trade({"trade_id": 3, "type": "sell", "price": "99.8", "size": "1"}, 100.0)
    estimator.on_trade({"trade_id": 4, "type": "sell", "price": "99.7", "size": "1"}, 100.0)
    time.sleep(0.12)
    estimator.on_book_update(100.4)

    snapshot = estimator.snapshot()

    assert snapshot.ready
    assert snapshot.lambda_plus > 0
    assert snapshot.lambda_minus > 0
    assert snapshot.kappa_plus > 0
    assert snapshot.kappa_minus > 0
    assert snapshot.kappa_points_plus >= 0
    assert snapshot.kappa_points_minus >= 0
    assert 0.0 <= snapshot.kappa_fit_quality <= 1.0
    assert snapshot.markout_count_plus == 2
    assert snapshot.markout_count_minus == 2


def test_lighter_cj_estimator_deduplicates_trade_ids():
    estimator = LighterCJEstimator(
        min_trades_per_side=1, min_markouts_per_side=1,
        snapshot_min_interval=0.0,  # this test asserts back-to-back snapshots
    )

    trade = {"trade_id": "abc", "type": "buy", "price": "100.1", "size": "1"}
    estimator.on_trade(trade, 100.0)
    estimator.on_trade(trade, 100.0)

    # Duplicate trade id is recorded exactly once
    snapshot = estimator.snapshot()
    assert snapshot.trade_count_plus == 1

    # Still deduplicated after more data flows through
    estimator.on_trade(trade, 100.0)
    estimator.on_book_update(100.2)
    snapshot = estimator.snapshot()
    assert snapshot.trade_count_plus == 1


def test_lighter_cj_estimator_infers_public_trade_side_from_maker_side():
    estimator = LighterCJEstimator(
        markout_seconds=0.01,
        min_trades_per_side=1,
        min_markouts_per_side=1,
    )

    estimator.on_book_update(100.0)
    estimator.on_trade(
        {
            "trade_id": "maker-ask",
            "type": "trade",
            "is_maker_ask": True,
            "price": "100.2",
            "size": "1",
        },
        100.0,
    )
    estimator.on_trade(
        {
            "trade_id": "maker-bid",
            "type": "trade",
            "is_maker_ask": False,
            "price": "99.8",
            "size": "1",
        },
        100.0,
    )
    time.sleep(0.02)
    estimator.on_book_update(100.1)

    snapshot = estimator.snapshot()

    assert snapshot.trade_count_plus == 1
    assert snapshot.trade_count_minus == 1


def test_lighter_cj_estimator_fits_kappa_from_depth_survival():
    estimator = LighterCJEstimator(
        window_seconds=60,
        markout_seconds=0.01,
        min_trades_per_side=2,
        min_markouts_per_side=1,
        min_kappa_points=4,
        min_kappa_r2=0.0,
        default_kappa=0.035,
    )

    estimator.on_book_update(100.0)
    depths = [0.05, 0.08, 0.11, 0.15, 0.22, 0.34, 0.51, 0.76, 1.10, 1.60]
    for i, depth in enumerate(depths):
        estimator.on_trade({"trade_id": f"b{i}", "type": "buy", "price": 100.0 + depth, "size": "1"}, 100.0)
        estimator.on_trade({"trade_id": f"s{i}", "type": "sell", "price": 100.0 - depth, "size": "1"}, 100.0)

    time.sleep(0.02)
    estimator.on_book_update(100.2)
    snapshot = estimator.snapshot()

    assert snapshot.kappa_points_plus >= 4
    assert snapshot.kappa_points_minus >= 4
    assert snapshot.kappa_r2_plus > 0
    assert snapshot.kappa_r2_minus > 0
    assert snapshot.kappa_plus != 0.035
    assert snapshot.kappa_minus != 0.035
