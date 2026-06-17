import math

import pytest

from cartea_jaimungal import (
    CarteaJaimungalCalculator,
    CarteaJaimungalParams,
    compute_hjb_surface,
    toxicity,
)


def test_hjb_surface_returns_inventory_depths():
    params = CarteaJaimungalParams(q_max=2)
    surface = compute_hjb_surface(params)

    assert list(surface.q_grid) == [-2, -1, 0, 1, 2]
    assert len(surface.delta_bid) == 5
    assert len(surface.delta_ask) == 5
    assert math.isinf(surface.delta_ask[0])
    assert math.isinf(surface.delta_bid[-1])
    assert all(math.isfinite(value) for value in surface.delta_bid[:-1])
    assert all(math.isfinite(value) for value in surface.delta_ask[1:])
    assert surface.method in {"matrix_exponential", "backward_euler_asymmetric"}


def test_asymmetric_solver_supports_side_specific_kappa():
    params = CarteaJaimungalParams(
        q_max=2,
        kappa_plus=0.025,
        kappa_minus=0.075,
        epsilon_plus=2.0,
        epsilon_minus=5.0,
        solver_mode="asymmetric",
        asym_n_steps=12,
        asym_max_iter=8,
    )
    surface = compute_hjb_surface(params)

    assert surface.method == "backward_euler_asymmetric"
    assert list(surface.q_grid) == [-2, -1, 0, 1, 2]
    assert math.isinf(surface.delta_ask[0])
    assert math.isinf(surface.delta_bid[-1])
    assert all(math.isfinite(value) for value in surface.delta_bid[:-1])
    assert all(math.isfinite(value) for value in surface.delta_ask[1:])


def test_calculator_quotes_after_warmup():
    calc = CarteaJaimungalCalculator(
        tick_size=0.1,
        params=CarteaJaimungalParams(min_half_spread_bps=4, max_half_spread_bps=80),
        min_warmup_samples=2,
    )

    assert not calc.warmed_up
    assert calc.quote(66_000.0, 0.0) == (None, None)

    calc.on_book_update(66_000.0, {65_999.9: 1.0}, {66_000.1: 1.0})
    calc.on_book_update(66_000.0, {65_999.9: 1.0}, {66_000.1: 1.0})

    bid, ask = calc.quote(66_000.0, 0.0)
    assert calc.warmed_up
    assert bid is not None and ask is not None
    assert bid < 66_000.0 < ask
    assert round(bid, 1) == bid
    assert round(ask, 1) == ask


def test_inventory_boundary_suppresses_risk_increasing_side():
    params = CarteaJaimungalParams(q_max=1, inventory_unit_base=0.01)
    calc = CarteaJaimungalCalculator(tick_size=0.1, params=params, min_warmup_samples=0)

    bid_at_max_long, ask_at_max_long = calc.quote(66_000.0, 0.01)
    bid_at_max_short, ask_at_max_short = calc.quote(66_000.0, -0.01)

    assert bid_at_max_long is None
    assert ask_at_max_long is not None
    assert bid_at_max_short is not None
    assert ask_at_max_short is None


def test_toxicity_gate_blocks_quotes():
    params = CarteaJaimungalParams(
        kappa_plus=1.0,
        kappa_minus=1.0,
        epsilon_plus=3.0,
        epsilon_minus=3.0,
        max_toxicity=1.5,
    )
    calc = CarteaJaimungalCalculator(tick_size=0.1, params=params, min_warmup_samples=0)

    assert toxicity(params) == pytest.approx(3.0)
    assert calc.quote(66_000.0, 0.0) == (None, None)


def test_min_spread_floor_is_applied():
    params = CarteaJaimungalParams(
        kappa_plus=100.0,
        kappa_minus=100.0,
        epsilon_plus=0.0,
        epsilon_minus=0.0,
        min_half_spread_bps=10.0,
        max_half_spread_bps=20.0,
        maker_fee_rate=0.0,
    )
    calc = CarteaJaimungalCalculator(tick_size=0.1, params=params, min_warmup_samples=0)
    bid, ask = calc.quote(1_000.0, 0.0)

    assert bid <= 999.0
    assert ask >= 1001.0


def test_sigma2_adds_volatility_cushion_to_quotes():
    base = CarteaJaimungalCalculator(
        tick_size=0.1,
        params=CarteaJaimungalParams(
            kappa_plus=100.0,
            kappa_minus=100.0,
            epsilon_plus=0.0,
            epsilon_minus=0.0,
            min_half_spread_bps=1.0,
            max_half_spread_bps=1_000.0,
            maker_fee_rate=0.0,
            sigma2_per_sec=0.0,
            volatility_spread_multiplier=0.0,
        ),
        min_warmup_samples=0,
    )
    volatile = CarteaJaimungalCalculator(
        tick_size=0.1,
        params=CarteaJaimungalParams(
            kappa_plus=100.0,
            kappa_minus=100.0,
            epsilon_plus=0.0,
            epsilon_minus=0.0,
            min_half_spread_bps=1.0,
            max_half_spread_bps=1_000.0,
            maker_fee_rate=0.0,
            sigma2_per_sec=4.0,
            horizon_seconds=60.0,
            volatility_spread_multiplier=0.5,
        ),
        min_warmup_samples=0,
    )

    base_bid, base_ask = base.quote(1_000.0, 0.0)
    vol_bid, vol_ask = volatile.quote(1_000.0, 0.0)

    assert vol_bid < base_bid
    assert vol_ask > base_ask


def test_max_position_dollar_suppresses_risk_increasing_side():
    params = CarteaJaimungalParams(inventory_unit_base=0.01)
    calc = CarteaJaimungalCalculator(tick_size=0.1, params=params, min_warmup_samples=0)
    calc.set_max_position_dollar(100.0)

    bid_long, ask_long = calc.quote(1_000.0, 0.11)
    bid_short, ask_short = calc.quote(1_000.0, -0.11)

    assert bid_long is None
    assert ask_long is not None
    assert bid_short is not None
    assert ask_short is None
