"""Cartea-Jaimungal style quote engine for Lighter paper challengers.

This module keeps the model independent from execution.  It exposes the same
minimal interface as ``VolObiCalculator`` so the grid dry-run can compare quote
engines without changing the live order-management path.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Literal

import numpy as np
from scipy.linalg import expm


@dataclass(frozen=True)
class CarteaJaimungalParams:
    lambda_plus: float = 0.30
    lambda_minus: float = 0.30
    kappa_plus: float = 0.035
    kappa_minus: float = 0.035
    epsilon_plus: float = 4.0
    epsilon_minus: float = 4.0
    sigma2_per_sec: float = 0.0
    alpha: float = 0.001
    phi: float = 0.0001
    horizon_seconds: float = 60.0
    q_max: int = 3
    spread_multiplier: float = 1.0
    min_half_spread_bps: float = 4.0
    max_half_spread_bps: float = 80.0
    maker_fee_rate: float = 0.00004
    inventory_unit_base: float = 0.0002
    max_toxicity: float = 1.5
    volatility_spread_multiplier: float = 0.0
    solver_mode: Literal["symmetric", "asymmetric"] = "asymmetric"
    asym_n_steps: int = 40
    asym_max_iter: int = 12
    asym_tol: float = 1e-7
    asym_damping: float = 0.7
    clip_deltas_at_zero: bool = False


@dataclass(frozen=True)
class HjbSurface:
    q_grid: np.ndarray
    h: np.ndarray
    delta_bid: np.ndarray
    delta_ask: np.ndarray
    kappa_symmetric: float
    method: str = "matrix_exponential"


def _validate_params(params: CarteaJaimungalParams) -> None:
    values = {
        "lambda_plus": params.lambda_plus,
        "lambda_minus": params.lambda_minus,
        "kappa_plus": params.kappa_plus,
        "kappa_minus": params.kappa_minus,
        "epsilon_plus": params.epsilon_plus,
        "epsilon_minus": params.epsilon_minus,
        "sigma2_per_sec": params.sigma2_per_sec,
        "alpha": params.alpha,
        "phi": params.phi,
        "horizon_seconds": params.horizon_seconds,
    }
    for name, value in values.items():
        if not math.isfinite(float(value)):
            raise ValueError(f"{name} must be finite")

    if params.lambda_plus < 0 or params.lambda_minus < 0:
        raise ValueError("lambda values must be non-negative")
    if params.kappa_plus <= 0 or params.kappa_minus <= 0:
        raise ValueError("kappa values must be positive")
    if params.epsilon_plus < 0 or params.epsilon_minus < 0:
        raise ValueError("epsilon values must be non-negative")
    if params.sigma2_per_sec < 0:
        raise ValueError("sigma2_per_sec must be non-negative")
    if params.alpha < 0 or params.phi < 0:
        raise ValueError("inventory penalties must be non-negative")
    if params.horizon_seconds <= 0:
        raise ValueError("horizon_seconds must be positive")
    if int(params.q_max) < 1:
        raise ValueError("q_max must be at least 1")
    if params.solver_mode not in {"symmetric", "asymmetric"}:
        raise ValueError("solver_mode must be 'symmetric' or 'asymmetric'")


def toxicity(params: CarteaJaimungalParams) -> float:
    return max(
        float(params.kappa_plus) * float(params.epsilon_plus),
        float(params.kappa_minus) * float(params.epsilon_minus),
    )


def _optimal_delta_and_value(
    lam: float,
    kappa: float,
    eps: float,
    dh: float,
    *,
    clip_at_zero: bool = False,
) -> tuple[float, float]:
    lam = max(float(lam), 0.0)
    kappa = max(float(kappa), 1e-12)
    eps = float(eps)
    dh = float(dh)

    delta_star = (1.0 / kappa) + eps - dh
    c = -eps + dh
    if clip_at_zero and delta_star <= 0.0:
        return 0.0, max(lam * c, 0.0)

    exponent = float(np.clip(-kappa * delta_star, -700.0, 700.0))
    value_star = (lam / kappa) * math.exp(exponent)
    return float(delta_star), float(value_star)


def _compute_hjb_surface_symmetric(params: CarteaJaimungalParams) -> HjbSurface:
    """Compute the matrix-exponential HJB surface.

    The closed form assumes symmetric kappa, so it uses the average of
    kappa+/kappa-. Lambda and epsilon stay side-specific.
    """
    q_max = int(params.q_max)
    q_grid = np.arange(-q_max, q_max + 1, dtype=float)
    n_states = len(q_grid)
    kappa = 0.5 * (float(params.kappa_plus) + float(params.kappa_minus))

    lambda_plus = float(params.lambda_plus)
    lambda_minus = float(params.lambda_minus)
    epsilon_plus = float(params.epsilon_plus)
    epsilon_minus = float(params.epsilon_minus)

    adjusted_plus = lambda_plus * math.exp(-1.0 - kappa * epsilon_plus)
    adjusted_minus = lambda_minus * math.exp(-1.0 - kappa * epsilon_minus)

    matrix = np.zeros((n_states, n_states), dtype=float)
    for idx, q in enumerate(q_grid):
        matrix[idx, idx] = (
            q * kappa * (lambda_plus * epsilon_plus - lambda_minus * epsilon_minus)
            - float(params.phi) * kappa * (q ** 2)
        )
        if idx > 0:
            matrix[idx, idx - 1] = adjusted_plus
        if idx < n_states - 1:
            matrix[idx, idx + 1] = adjusted_minus

    terminal = np.exp(-float(params.alpha) * kappa * (q_grid ** 2))
    omega = expm(matrix * float(params.horizon_seconds)).dot(terminal)
    omega = np.maximum(omega, 1e-300)
    log_omega = np.log(omega)
    log_omega = log_omega - np.max(log_omega)
    h = log_omega / kappa

    delta_bid = np.full(n_states, np.inf, dtype=float)
    delta_ask = np.full(n_states, np.inf, dtype=float)
    for idx in range(n_states):
        h_q = h[idx]
        if idx < n_states - 1:
            delta_bid[idx] = (1.0 / float(params.kappa_minus)) + epsilon_minus - (h[idx + 1] - h_q)
        if idx > 0:
            delta_ask[idx] = (1.0 / float(params.kappa_plus)) + epsilon_plus - (h[idx - 1] - h_q)

    return HjbSurface(
        q_grid=q_grid.astype(int),
        h=h,
        delta_bid=delta_bid,
        delta_ask=delta_ask,
        kappa_symmetric=kappa,
        method="matrix_exponential",
    )


def _compute_hjb_surface_asymmetric(params: CarteaJaimungalParams) -> HjbSurface:
    """Compute the asymmetric-kappa HJB surface with backward Euler."""
    q_max = int(params.q_max)
    q_grid = np.arange(-q_max, q_max + 1, dtype=float)
    n_states = len(q_grid)
    lambda_plus = float(params.lambda_plus)
    lambda_minus = float(params.lambda_minus)
    kappa_plus = float(params.kappa_plus)
    kappa_minus = float(params.kappa_minus)
    epsilon_plus = float(params.epsilon_plus)
    epsilon_minus = float(params.epsilon_minus)

    h = -float(params.alpha) * (q_grid ** 2)
    n_steps = max(int(params.asym_n_steps), 1)
    dt = max(float(params.horizon_seconds) / float(n_steps), 1e-6)
    max_iter = max(int(params.asym_max_iter), 1)
    tol = max(float(params.asym_tol), 1e-12)
    damping = min(max(float(params.asym_damping), 0.05), 1.0)
    fd_eps = 1e-6

    def compute_g(h_vec: np.ndarray) -> np.ndarray:
        out = np.zeros_like(h_vec)
        for idx, q in enumerate(q_grid):
            h_q = h_vec[idx]
            value_total = -float(params.phi) * (float(q) ** 2)

            if idx > 0:
                _, val_plus = _optimal_delta_and_value(
                    lambda_plus,
                    kappa_plus,
                    epsilon_plus,
                    h_vec[idx - 1] - h_q,
                    clip_at_zero=bool(params.clip_deltas_at_zero),
                )
                value_total += val_plus
            if idx < n_states - 1:
                _, val_minus = _optimal_delta_and_value(
                    lambda_minus,
                    kappa_minus,
                    epsilon_minus,
                    h_vec[idx + 1] - h_q,
                    clip_at_zero=bool(params.clip_deltas_at_zero),
                )
                value_total += val_minus

            drift = float(q) * (lambda_plus * epsilon_plus - lambda_minus * epsilon_minus)
            out[idx] = value_total + drift
        return out

    for _ in range(n_steps):
        h_old = h.copy()
        h_old = h_old - np.max(h_old)
        h_new = h_old.copy()

        for _it in range(max_iter):
            g = compute_g(h_new)
            residual = h_new - h_old - dt * g
            if float(np.max(np.abs(residual))) < tol:
                break

            jac_g = np.zeros((n_states, n_states), dtype=float)
            for col in range(n_states):
                h_pert = h_new.copy()
                h_pert[col] += fd_eps
                jac_g[:, col] = (compute_g(h_pert) - g) / fd_eps

            jac_residual = np.eye(n_states) - dt * jac_g
            try:
                step = np.linalg.solve(jac_residual, -residual)
            except np.linalg.LinAlgError:
                step = -residual

            trial = h_new + damping * step
            trial = trial - np.max(trial)
            if float(np.max(np.abs(trial - h_new))) < tol:
                h_new = trial
                break
            h_new = trial
        h = h_new

    delta_bid = np.full(n_states, np.inf, dtype=float)
    delta_ask = np.full(n_states, np.inf, dtype=float)
    for idx in range(n_states):
        h_q = h[idx]
        if idx < n_states - 1:
            raw_bid, _ = _optimal_delta_and_value(
                lambda_minus,
                kappa_minus,
                epsilon_minus,
                h[idx + 1] - h_q,
                clip_at_zero=bool(params.clip_deltas_at_zero),
            )
            delta_bid[idx] = max(0.0, raw_bid) if params.clip_deltas_at_zero else raw_bid
        if idx > 0:
            raw_ask, _ = _optimal_delta_and_value(
                lambda_plus,
                kappa_plus,
                epsilon_plus,
                h[idx - 1] - h_q,
                clip_at_zero=bool(params.clip_deltas_at_zero),
            )
            delta_ask[idx] = max(0.0, raw_ask) if params.clip_deltas_at_zero else raw_ask

    return HjbSurface(
        q_grid=q_grid.astype(int),
        h=h,
        delta_bid=delta_bid,
        delta_ask=delta_ask,
        kappa_symmetric=0.5 * (kappa_plus + kappa_minus),
        method="backward_euler_asymmetric",
    )


def compute_hjb_surface(params: CarteaJaimungalParams) -> HjbSurface:
    """Compute a compact CJ HJB inventory surface."""
    _validate_params(params)
    if params.solver_mode == "asymmetric":
        return _compute_hjb_surface_asymmetric(params)
    return _compute_hjb_surface_symmetric(params)


class CarteaJaimungalCalculator:
    """Inventory-aware quote calculator compatible with the grid runner."""

    def __init__(
        self,
        *,
        tick_size: float,
        params: CarteaJaimungalParams | None = None,
        min_warmup_samples: int = 100,
    ) -> None:
        if tick_size <= 0:
            raise ValueError("tick_size must be positive")
        self._tick_size = float(tick_size)
        self._params = params or CarteaJaimungalParams()
        self._surface = compute_hjb_surface(self._params)
        self._min_warmup_samples = max(0, int(min_warmup_samples))
        self._samples = 0
        self._warmed_up = self._min_warmup_samples == 0
        self._inventory_unit_base = max(float(self._params.inventory_unit_base), 1e-12)
        self._max_position_dollar: float | None = None

    def update_params(self, params: CarteaJaimungalParams) -> bool:
        if params == self._params:
            return False
        self._params = params
        self._surface = compute_hjb_surface(params)
        self._inventory_unit_base = max(float(params.inventory_unit_base), 1e-12)
        return True

    @property
    def warmed_up(self) -> bool:
        return self._warmed_up

    def reset(self) -> None:
        self._samples = 0
        self._warmed_up = self._min_warmup_samples == 0

    def set_alpha_override(self, _alpha: float | None) -> None:
        return

    def set_max_position_dollar(self, value: float) -> None:
        self._max_position_dollar = float(value) if value and value > 0 else None

    def set_inventory_unit_base(self, value: float) -> None:
        if value and value > 0:
            self._inventory_unit_base = max(float(value), 1e-12)

    def on_book_update(self, mid_price: float, bids, asks) -> None:
        if mid_price <= 0 or not bids or not asks:
            return
        self._samples += 1
        if not self._warmed_up and self._samples >= self._min_warmup_samples:
            self._warmed_up = True

    def _inventory_index(self, position_size: float) -> tuple[int, int]:
        q_raw = int(round(float(position_size) / self._inventory_unit_base))
        q_min = int(self._surface.q_grid[0])
        q_max = int(self._surface.q_grid[-1])
        q = max(q_min, min(q_max, q_raw))
        idx = int(np.where(self._surface.q_grid == q)[0][0])
        return q, idx

    def _apply_depth(self, mid_price: float, depth_model: float) -> float:
        if not math.isfinite(depth_model):
            return math.inf
        fee_cushion = float(self._params.maker_fee_rate) * mid_price
        vol_cushion = 0.0
        if self._params.volatility_spread_multiplier > 0 and self._params.sigma2_per_sec > 0:
            horizon = max(float(self._params.horizon_seconds), 1e-6)
            vol_cushion = (
                math.sqrt(float(self._params.sigma2_per_sec) * horizon)
                * float(self._params.volatility_spread_multiplier)
            )
        depth = (
            max(0.0, float(depth_model)) * float(self._params.spread_multiplier)
            + fee_cushion
            + vol_cushion
        )
        floor = float(self._params.min_half_spread_bps) / 10_000.0 * mid_price
        cap = float(self._params.max_half_spread_bps) / 10_000.0 * mid_price
        return min(max(depth, floor), cap)

    def quote(self, mid_price: float, position_size: float):
        if not self._warmed_up or mid_price <= 0:
            return None, None
        if toxicity(self._params) > float(self._params.max_toxicity):
            return None, None

        q, idx = self._inventory_index(position_size)
        q_min = int(self._surface.q_grid[0])
        q_max = int(self._surface.q_grid[-1])

        bid = None
        ask = None

        if q < q_max:
            bid_depth = self._apply_depth(mid_price, float(self._surface.delta_bid[idx]))
            if math.isfinite(bid_depth):
                bid = math.floor((mid_price - bid_depth) / self._tick_size) * self._tick_size

        if q > q_min:
            ask_depth = self._apply_depth(mid_price, float(self._surface.delta_ask[idx]))
            if math.isfinite(ask_depth):
                ask = math.ceil((mid_price + ask_depth) / self._tick_size) * self._tick_size

        if bid is not None and bid <= 0:
            bid = None
        if ask is not None and ask <= 0:
            ask = None
        if bid is not None and ask is not None and bid >= ask:
            bid = math.floor((mid_price - self._tick_size) / self._tick_size) * self._tick_size
            ask = math.ceil((mid_price + self._tick_size) / self._tick_size) * self._tick_size

        if self._max_position_dollar is not None:
            position_value = abs(float(position_size)) * float(mid_price)
            if position_value >= self._max_position_dollar:
                if position_size > 0:
                    bid = None
                elif position_size < 0:
                    ask = None

        return bid, ask


__all__ = [
    "CarteaJaimungalCalculator",
    "CarteaJaimungalParams",
    "HjbSurface",
    "compute_hjb_surface",
    "toxicity",
]
