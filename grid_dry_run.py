"""Parallel grid dry-run: N independent simulations sharing one WS feed.

Usage:
    python -u market_maker_v2.py --symbol BTC --grid grid_config.json

Each parameter combination gets its own wallet, PnL, position, and order
history.  State is persisted by *parameter values* (not slot index), so
changing the grid config recovers overlapping combos automatically.
"""

from __future__ import annotations

import asyncio
import csv
import itertools
import json
import logging
import math
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import lighter

from binance_obi import (
    BinanceBookTickerClient,
    BinanceDiffDepthClient,
    SharedAlpha,
    SharedBBO,
    lighter_to_binance_symbol,
)
from cartea_jaimungal import CarteaJaimungalCalculator, CarteaJaimungalParams
from dry_run import DryRunEngine
from lighter_estimators import CJSnapshot, LighterCJEstimator
from logging_config import setup_logging
from orderbook import apply_orderbook_update
from trade_log import TradeLogger
from utils import EPSILON, get_market_details_async, load_config_params
from vol_obi import VolObiCalculator
from ws_manager import ws_subscribe_fast

try:
    from _vol_obi_fast import CBookSide as _BookSide
    _CYTHON_AVAILABLE = True
except ImportError:
    from sortedcontainers import SortedDict as _BookSide
    _CYTHON_AVAILABLE = False

try:
    from _vol_obi_fast import price_change_bps_fast as _price_change_bps_c
except ImportError:
    _price_change_bps_c = None

# Re-import lightweight types from market_maker_v2 (class defs only, no state)
from market_maker_v2 import (
    BatchOp,
    OrderState,
    AccountState,
    MarketState,
    MarketConfig,
    VolObiState,
    OrderManagerState,
    SideOrderLifecycle,
    SideStatus,
    OrderManager,
)

logger = logging.getLogger(__name__)

_MAX_CLIENT_ORDER_INDEX = 281474976710655  # 2^48 - 1

# ---------------------------------------------------------------------------
# Config defaults (loaded from config.json same as market_maker_v2)
# ---------------------------------------------------------------------------
_config = load_config_params()
_trading = _config.get("trading", {})
_ws_cfg = _config.get("websocket", {})
_vol_obi_cfg = _trading.get("vol_obi", {})
_alpha_cfg = _trading.get("alpha", {})

# WS tuning (shared)
WEBSOCKET_URL = "wss://mainnet.zklighter.elliot.ai/stream"
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", _ws_cfg.get("ping_interval", 20)))
WS_RECV_TIMEOUT = float(os.getenv("WS_RECV_TIMEOUT", _ws_cfg.get("recv_timeout", 30.0)))
WS_RECONNECT_BASE = int(os.getenv("WS_RECONNECT_BASE_DELAY", _ws_cfg.get("reconnect_base_delay", 5)))
WS_RECONNECT_MAX = int(os.getenv("WS_RECONNECT_MAX_DELAY", _ws_cfg.get("reconnect_max_delay", 60)))

# Binance defaults
ALPHA_SOURCE = os.getenv("ALPHA_SOURCE", _alpha_cfg.get("source", "binance"))
BINANCE_STALE_SECONDS = float(os.getenv("BINANCE_STALE_SECONDS", _alpha_cfg.get("stale_seconds", 5.0)))
BINANCE_OBI_WINDOW = int(os.getenv("BINANCE_OBI_WINDOW", _alpha_cfg.get("window_size", 6000)))
BINANCE_OBI_MIN_SAMPLES = int(os.getenv("BINANCE_OBI_MIN_SAMPLES", _alpha_cfg.get("min_samples", 150)))
BINANCE_OBI_LOOKING_DEPTH = float(os.getenv("BINANCE_OBI_LOOKING_DEPTH", _alpha_cfg.get("looking_depth", 0.025)))
BINANCE_BBO_MIN_SAMPLES = int(os.getenv("BINANCE_BBO_MIN_SAMPLES", _alpha_cfg.get("bbo_min_samples", 10)))
BINANCE_BBO_STALE_SECONDS = float(os.getenv("BINANCE_BBO_STALE_SECONDS", _alpha_cfg.get("bbo_stale_seconds", 5.0)))
BINANCE_DEPTH_SNAPSHOT_LIMIT = int(os.getenv("BINANCE_DEPTH_SNAPSHOT_LIMIT", _alpha_cfg.get("depth_snapshot_limit", 1000)))

# Vol+OBI shared defaults (used when grid config doesn't override)
VOL_OBI_WINDOW_STEPS = int(os.getenv("VOL_OBI_WINDOW_STEPS", _vol_obi_cfg.get("window_steps", 6000)))
VOL_OBI_STEP_NS = int(os.getenv("VOL_OBI_STEP_NS", _vol_obi_cfg.get("step_ns", 100_000_000)))
VOL_OBI_LOOKING_DEPTH = float(os.getenv("VOL_OBI_LOOKING_DEPTH", _vol_obi_cfg.get("looking_depth", 0.025)))
VOL_OBI_MIN_WARMUP_SAMPLES = int(os.getenv("VOL_OBI_MIN_WARMUP_SAMPLES", _vol_obi_cfg.get("min_warmup_samples", 100)))
MIN_ORDER_VALUE_USD = float(_trading.get("min_order_value_usd", 14.5))

# ---------------------------------------------------------------------------
# Grid data structures
# ---------------------------------------------------------------------------

# Recognised parameter names that can appear in grid config "parameters" or "fixed"
_KNOWN_PARAMS = {
    "quote_engine",
    "vol_to_half_spread", "min_half_spread_bps", "skew",
    "spread_factor_level1", "capital_usage_percent", "num_levels", "c1_ticks",
    "cj_lambda", "cj_lambda_plus", "cj_lambda_minus",
    "cj_kappa", "cj_kappa_plus", "cj_kappa_minus",
    "cj_epsilon", "cj_epsilon_plus", "cj_epsilon_minus",
    "cj_sigma2_per_sec", "cj_sigma2_scale", "cj_volatility_spread_multiplier",
    "cj_alpha", "cj_phi", "cj_horizon_seconds", "cj_q_max",
    "cj_spread_multiplier", "cj_min_half_spread_bps", "cj_max_half_spread_bps",
    "cj_inventory_unit_base", "cj_max_toxicity", "cj_solver_mode",
    "cj_asym_n_steps", "cj_asym_max_iter", "cj_use_estimator",
    "cj_estimator_blend", "cj_lambda_scale", "cj_kappa_scale",
    "cj_epsilon_scale", "cj_refresh_seconds",
}


@dataclass
class GridParams:
    quote_engine: str = "vol_obi"
    vol_to_half_spread: float = 48.0
    min_half_spread_bps: float = 8.0
    skew: float = 3.0
    spread_factor_level1: float = 2.0
    capital_usage_percent: float = 0.12
    num_levels: int = 2
    c1_ticks: float = 20.0
    cj_lambda: float = 0.30
    cj_lambda_plus: float = 0.0
    cj_lambda_minus: float = 0.0
    cj_kappa: float = 0.035
    cj_kappa_plus: float = 0.0
    cj_kappa_minus: float = 0.0
    cj_epsilon: float = 4.0
    cj_epsilon_plus: float = 0.0
    cj_epsilon_minus: float = 0.0
    cj_sigma2_per_sec: float = 0.0
    cj_sigma2_scale: float = 1.0
    cj_volatility_spread_multiplier: float = 0.0
    cj_alpha: float = 0.001
    cj_phi: float = 0.0001
    cj_horizon_seconds: float = 60.0
    cj_q_max: int = 3
    cj_spread_multiplier: float = 1.0
    cj_min_half_spread_bps: float = 4.0
    cj_max_half_spread_bps: float = 80.0
    cj_inventory_unit_base: float = 0.0002
    cj_max_toxicity: float = 1.5
    cj_solver_mode: str = "asymmetric"
    cj_asym_n_steps: int = 40
    cj_asym_max_iter: int = 12
    cj_use_estimator: bool = False
    cj_estimator_blend: float = 1.0
    cj_lambda_scale: float = 1.0
    cj_kappa_scale: float = 1.0
    cj_epsilon_scale: float = 1.0
    cj_refresh_seconds: float = 300.0
    label: str = ""


def _param_key(p: GridParams) -> str:
    """Deterministic, human-readable key from parameter values."""
    if p.quote_engine == "cartea_jaimungal":
        dyn = "_dyn" if p.cj_use_estimator else ""
        return (
            f"cj{dyn}_{p.cj_solver_mode}"
            f"_lp{p.cj_lambda_plus or p.cj_lambda}_lm{p.cj_lambda_minus or p.cj_lambda}"
            f"_kp{p.cj_kappa_plus or p.cj_kappa}_km{p.cj_kappa_minus or p.cj_kappa}"
            f"_ep{p.cj_epsilon_plus or p.cj_epsilon}_em{p.cj_epsilon_minus or p.cj_epsilon}"
            f"_a{p.cj_alpha}_ph{p.cj_phi}_sm{p.cj_spread_multiplier}_vs{p.cj_volatility_spread_multiplier}"
            f"_ls{p.cj_lambda_scale}_ks{p.cj_kappa_scale}_es{p.cj_epsilon_scale}_ss{p.cj_sigma2_scale}"
            f"_c{p.capital_usage_percent}_l{p.num_levels}"
        )
    return (
        f"v{p.vol_to_half_spread}_m{p.min_half_spread_bps}"
        f"_s{p.skew}_f{p.spread_factor_level1}"
        f"_c{p.capital_usage_percent}_l{p.num_levels}"
        f"_c1{p.c1_ticks}"
    )


@dataclass
class SlotState:
    """MMState-like facade: shared market, per-slot account/orders."""
    market: MarketState
    config: MarketConfig
    account: AccountState
    orders: OrderState
    vol_obi_state: VolObiState
    binance_alpha: Optional[SharedAlpha] = None
    binance_bbo: Optional[SharedBBO] = None


class SlotOrderManager(OrderManager):
    """OrderManager that writes to a per-slot OrderState."""

    def __init__(self, lifecycle_state: OrderManagerState, slot_orders: OrderState, num_levels: int):
        super().__init__(lifecycle_state)
        self._slot_orders = slot_orders
        self._num_levels = num_levels

    def _bind_live(self, side: str, order_id: int, price: float, size: float, *, level: int = 0) -> None:
        orders = self._slot_orders
        if side == "buy":
            orders.bid_order_ids[level] = order_id
            orders.bid_prices[level] = price
            orders.bid_sizes[level] = size
        else:
            orders.ask_order_ids[level] = order_id
            orders.ask_prices[level] = price
            orders.ask_sizes[level] = size
        self.mark_status(side, SideStatus.LIVE, level=level, target_price=price, target_size=size)

    def _clear_live(self, side: str, level: Optional[int] = None) -> None:
        levels = range(self._num_levels) if level is None else [level]
        orders = self._slot_orders
        for lvl in levels:
            if side == "buy":
                orders.bid_order_ids[lvl] = None
                orders.bid_prices[lvl] = None
                orders.bid_sizes[lvl] = None
            else:
                orders.ask_order_ids[lvl] = None
                orders.ask_prices[lvl] = None
                orders.ask_sizes[lvl] = None
            self.mark_status(side, SideStatus.IDLE, level=lvl)


@dataclass
class GridSlot:
    index: int
    label: str
    param_key: str
    params: GridParams
    slot_state: SlotState
    order_manager: SlotOrderManager
    dry_engine: DryRunEngine
    trade_logger: TradeLogger
    client_to_exchange_id: dict
    spread_factors: list
    last_cid: int = 0
    last_cj_refresh: float = 0.0
    last_cj_estimator_ready: bool = False

    def next_client_order_index(self) -> int:
        new_id = time.time_ns() % _MAX_CLIENT_ORDER_INDEX
        if new_id <= self.last_cid:
            new_id = (self.last_cid + 1) % (_MAX_CLIENT_ORDER_INDEX + 1)
        self.last_cid = new_id
        return new_id


def _price_change_bps(old_price: Optional[float], new_price: Optional[float]) -> float:
    if old_price is None or new_price is None or old_price <= 0:
        return float("inf")
    if _price_change_bps_c is not None:
        return _price_change_bps_c(old_price, new_price)
    return abs(new_price - old_price) / old_price * 10000.0


# ---------------------------------------------------------------------------
# GridRunner
# ---------------------------------------------------------------------------

class GridRunner:
    """Orchestrates N parallel dry-run slots sharing one WS connection."""

    def __init__(self, config_path: str, symbol: str):
        with open(config_path) as f:
            cfg = json.load(f)

        self._symbol = symbol.upper()
        self._capital = float(cfg.get("capital", 1000))
        self._leverage = int(cfg.get("leverage", 2))
        self._warmup_seconds = float(cfg.get("warmup_seconds", 600))
        self._summary_interval = float(cfg.get("summary_interval_seconds", 60))
        self._sim_latency = float(cfg.get("sim_latency_s", 0.050))
        self._maker_fee_rate = float(cfg.get(
            "maker_fee_rate",
            _trading.get("maker_fee_rate", 0.000_04)))  # grid config > config.json > 0.004%

        # Build Cartesian product of parameter grid
        param_axes = cfg.get("parameters", {})
        fixed = cfg.get("fixed", {})
        for key in list(param_axes.keys()) + list(fixed.keys()):
            if key not in _KNOWN_PARAMS:
                raise ValueError(f"Unknown grid parameter: {key!r}. Known: {sorted(_KNOWN_PARAMS)}")

        # Defaults from fixed, overridden by each grid point
        defaults = {k: fixed[k] for k in fixed}
        axis_names = sorted(param_axes.keys())
        axis_values = [param_axes[k] for k in axis_names]
        if not axis_values:
            raise ValueError("Grid config 'parameters' must contain at least one axis")

        self._param_combos: list[GridParams] = []
        for i, combo in enumerate(itertools.product(*axis_values)):
            kw = dict(defaults)
            for name, val in zip(axis_names, combo):
                kw[name] = val
            kw["num_levels"] = int(kw.get("num_levels", 2))
            engine = str(kw.get("quote_engine", "vol_obi")).strip().lower()
            if engine in {"cj", "cartea", "cartea_jaimungal", "cartea-jaimungal"}:
                kw["quote_engine"] = "cartea_jaimungal"
            elif engine in {"vol_obi", "obi", "vol-obi"}:
                kw["quote_engine"] = "vol_obi"
            else:
                raise ValueError(f"Unknown quote_engine: {kw.get('quote_engine')!r}")
            kw["label"] = f"s{i:03d}"
            self._param_combos.append(GridParams(**kw))

        if len(self._param_combos) > 500:
            raise ValueError(f"Grid too large: {len(self._param_combos)} combos (max 500)")

        # Shared state (filled in run())
        self._shared_market: Optional[MarketState] = None
        self._shared_config: Optional[MarketConfig] = None
        self._shared_alpha: Optional[SharedAlpha] = None
        self._shared_bbo: Optional[SharedBBO] = None
        self._slots: list[GridSlot] = []
        self._book_seq = 0
        self._book_seq_event = asyncio.Event()
        self._ws_reconnect_event = asyncio.Event()
        self._log_dir = os.getenv("LOG_DIR", "logs")
        self._grid_dir = os.path.join(self._log_dir, "grid")
        self._start_time = 0.0
        estimator_cfg = cfg.get("cj_estimator", {}) if isinstance(cfg.get("cj_estimator", {}), dict) else {}
        self._cj_estimator_enabled = bool(
            estimator_cfg.get("enabled", False)
            or fixed.get("cj_use_estimator", False)
            or any(p.cj_use_estimator for p in self._param_combos)
        )
        self._cj_estimator: LighterCJEstimator | None = None
        if self._cj_estimator_enabled:
            self._cj_estimator = LighterCJEstimator(
                window_seconds=float(estimator_cfg.get("window_seconds", 900.0)),
                markout_seconds=float(estimator_cfg.get("markout_seconds", 5.0)),
                min_trades_per_side=int(estimator_cfg.get("min_trades_per_side", 8)),
                min_markouts_per_side=int(estimator_cfg.get("min_markouts_per_side", 4)),
                kappa_min=float(estimator_cfg.get("kappa_min", 0.005)),
                kappa_max=float(estimator_cfg.get("kappa_max", 0.25)),
                min_kappa_points=int(estimator_cfg.get("min_kappa_points", 4)),
                min_kappa_r2=float(estimator_cfg.get("min_kappa_r2", 0.15)),
                epsilon_floor=float(estimator_cfg.get("epsilon_floor", 0.0)),
                epsilon_cap=float(estimator_cfg.get("epsilon_cap", 80.0)),
                default_lambda=float(estimator_cfg.get("default_lambda", 0.30)),
                default_kappa=float(estimator_cfg.get("default_kappa", 0.035)),
                default_epsilon=float(estimator_cfg.get("default_epsilon", 4.0)),
            )

        logger.info(
            "Grid config: %d parameter combos, capital=$%.0f, leverage=%d",
            len(self._param_combos), self._capital, self._leverage,
        )

    # ------------------------------------------------------------------
    # Slot creation with persistence
    # ------------------------------------------------------------------

    def _base_cj_params(self, params: GridParams) -> CarteaJaimungalParams:
        lambda_plus = params.cj_lambda_plus or params.cj_lambda
        lambda_minus = params.cj_lambda_minus or params.cj_lambda
        kappa_plus = params.cj_kappa_plus or params.cj_kappa
        kappa_minus = params.cj_kappa_minus or params.cj_kappa
        epsilon_plus = params.cj_epsilon_plus or params.cj_epsilon
        epsilon_minus = params.cj_epsilon_minus or params.cj_epsilon
        solver_mode = str(params.cj_solver_mode or "asymmetric").strip().lower()
        if solver_mode not in {"symmetric", "asymmetric"}:
            solver_mode = "asymmetric"
        return CarteaJaimungalParams(
            lambda_plus=float(lambda_plus),
            lambda_minus=float(lambda_minus),
            kappa_plus=float(kappa_plus),
            kappa_minus=float(kappa_minus),
            epsilon_plus=float(epsilon_plus),
            epsilon_minus=float(epsilon_minus),
            sigma2_per_sec=float(params.cj_sigma2_per_sec),
            alpha=float(params.cj_alpha),
            phi=float(params.cj_phi),
            horizon_seconds=float(params.cj_horizon_seconds),
            q_max=int(params.cj_q_max),
            spread_multiplier=float(params.cj_spread_multiplier),
            min_half_spread_bps=float(params.cj_min_half_spread_bps),
            max_half_spread_bps=float(params.cj_max_half_spread_bps),
            maker_fee_rate=self._maker_fee_rate,
            inventory_unit_base=float(params.cj_inventory_unit_base),
            max_toxicity=float(params.cj_max_toxicity),
            volatility_spread_multiplier=float(params.cj_volatility_spread_multiplier),
            solver_mode=solver_mode,
            asym_n_steps=int(params.cj_asym_n_steps),
            asym_max_iter=int(params.cj_asym_max_iter),
        )

    @staticmethod
    def _blend(base: float, dynamic: float, blend: float) -> float:
        blend = min(max(float(blend), 0.0), 1.0)
        return (float(base) * (1.0 - blend)) + (float(dynamic) * blend)

    def _dynamic_cj_params(self, params: GridParams, snapshot: CJSnapshot | None) -> CarteaJaimungalParams:
        base = self._base_cj_params(params)
        if snapshot is None or not snapshot.ready:
            return base
        blend = float(params.cj_estimator_blend)
        return CarteaJaimungalParams(
            lambda_plus=self._blend(base.lambda_plus, snapshot.lambda_plus * params.cj_lambda_scale, blend),
            lambda_minus=self._blend(base.lambda_minus, snapshot.lambda_minus * params.cj_lambda_scale, blend),
            kappa_plus=self._blend(base.kappa_plus, snapshot.kappa_plus * params.cj_kappa_scale, blend),
            kappa_minus=self._blend(base.kappa_minus, snapshot.kappa_minus * params.cj_kappa_scale, blend),
            epsilon_plus=self._blend(base.epsilon_plus, snapshot.epsilon_plus * params.cj_epsilon_scale, blend),
            epsilon_minus=self._blend(base.epsilon_minus, snapshot.epsilon_minus * params.cj_epsilon_scale, blend),
            sigma2_per_sec=self._blend(base.sigma2_per_sec, snapshot.sigma2_per_sec * params.cj_sigma2_scale, blend),
            alpha=base.alpha,
            phi=base.phi,
            horizon_seconds=base.horizon_seconds,
            q_max=base.q_max,
            spread_multiplier=base.spread_multiplier,
            min_half_spread_bps=base.min_half_spread_bps,
            max_half_spread_bps=base.max_half_spread_bps,
            maker_fee_rate=base.maker_fee_rate,
            inventory_unit_base=base.inventory_unit_base,
            max_toxicity=base.max_toxicity,
            volatility_spread_multiplier=base.volatility_spread_multiplier,
            solver_mode=base.solver_mode,
            asym_n_steps=base.asym_n_steps,
            asym_max_iter=base.asym_max_iter,
        )

    def _create_quote_calculator(self, params: GridParams, tick: float):
        engine = str(params.quote_engine or "vol_obi").strip().lower()
        if engine in {"cj", "cartea", "cartea_jaimungal", "cartea-jaimungal"}:
            cj_params = self._base_cj_params(params)
            return CarteaJaimungalCalculator(
                tick_size=tick,
                params=cj_params,
                min_warmup_samples=VOL_OBI_MIN_WARMUP_SAMPLES,
            )

        if engine != "vol_obi":
            raise ValueError("Unknown quote_engine: %r" % params.quote_engine)

        return VolObiCalculator(
            tick_size=tick,
            window_steps=VOL_OBI_WINDOW_STEPS,
            step_ns=VOL_OBI_STEP_NS,
            vol_to_half_spread=params.vol_to_half_spread,
            min_half_spread_bps=params.min_half_spread_bps,
            c1_ticks=params.c1_ticks,
            skew=params.skew,
            looking_depth=VOL_OBI_LOOKING_DEPTH,
            min_warmup_samples=VOL_OBI_MIN_WARMUP_SAMPLES,
            max_position_dollar=500.0,
        )

    def _create_slots(self) -> list[GridSlot]:
        os.makedirs(self._grid_dir, exist_ok=True)
        slots = []
        tick = self._shared_config.price_tick_float
        amount_tick = self._shared_config.amount_tick_float

        # Quiet logger for per-slot engines: only WARNING+ to avoid
        # 128 slots x 4 orders x every tick flooding the log.
        # Fills and summaries are tracked via GridRunner's own logger + trade CSVs.
        engine_logger = logging.getLogger("grid_engine")
        engine_logger.setLevel(logging.WARNING)
        if not engine_logger.handlers:
            engine_logger.addHandler(logging.NullHandler())
        engine_logger.propagate = False

        for i, params in enumerate(self._param_combos):
            pk = _param_key(params)

            # Per-slot order state
            n_levels = params.num_levels
            order_state = OrderState(
                bid_order_ids=[None] * n_levels,
                ask_order_ids=[None] * n_levels,
                bid_prices=[None] * n_levels,
                ask_prices=[None] * n_levels,
                bid_sizes=[None] * n_levels,
                ask_sizes=[None] * n_levels,
            )

            # Per-slot account
            account = AccountState(
                available_capital=self._capital,
                portfolio_value=self._capital,
            )

            # Per-slot quote calculator.  The field name is historical; it can
            # hold either Vol+OBI or Cartea-Jaimungal compatible calculators.
            calc = self._create_quote_calculator(params, tick)

            slot_state = SlotState(
                market=self._shared_market,
                config=self._shared_config,
                account=account,
                orders=order_state,
                vol_obi_state=VolObiState(calculator=calc),
                binance_alpha=self._shared_alpha,
                binance_bbo=self._shared_bbo,
            )

            # Per-slot order manager
            om_state = OrderManagerState(
                bids=[SideOrderLifecycle() for _ in range(n_levels)],
                asks=[SideOrderLifecycle() for _ in range(n_levels)],
            )
            slot_om = SlotOrderManager(om_state, order_state, n_levels)

            # Per-slot trade logger
            trade_logger = TradeLogger(self._grid_dir, f"{self._symbol}_{pk}")

            # Per-slot id mapping
            client_to_exchange_id: dict = {}

            # Per-slot state path
            state_path = os.path.join(self._grid_dir, f"state_{self._symbol}_{pk}.json")

            # Try to restore from previous run
            engine = DryRunEngine.load_state(
                state_path,
                slot_state,
                slot_om,
                client_to_exchange_id,
                self._leverage,
                engine_logger,
                sim_latency_s=self._sim_latency,
                trade_logger=trade_logger,
                maker_fee_rate=self._maker_fee_rate,
            )
            if engine is not None:
                logger.info("Grid slot %s (%s): restored | capital=$%.2f pos=%.6f pnl=$%.4f fills=%d",
                            params.label, pk,
                            slot_state.account.available_capital,
                            engine._position, engine._realized_pnl, engine._fill_count)
            else:
                engine = DryRunEngine(
                    slot_state,
                    slot_om,
                    client_to_exchange_id,
                    self._leverage,
                    engine_logger,
                    sim_latency_s=self._sim_latency,
                    trade_logger=trade_logger,
                    state_path=state_path,
                    maker_fee_rate=self._maker_fee_rate,
                )
                engine.capture_initial_state()
                logger.info("Grid slot %s (%s): fresh | capital=$%.0f", params.label, pk, self._capital)

            # Precompute spread factors
            spread_factors = [params.spread_factor_level1 ** lvl for lvl in range(n_levels)]

            slots.append(GridSlot(
                index=i,
                label=params.label,
                param_key=pk,
                params=params,
                slot_state=slot_state,
                order_manager=slot_om,
                dry_engine=engine,
                trade_logger=trade_logger,
                client_to_exchange_id=client_to_exchange_id,
                spread_factors=spread_factors,
            ))

        return slots

    # ------------------------------------------------------------------
    # WS callbacks (shared → fan-out to all slots)
    # ------------------------------------------------------------------

    def _on_book_update(self, data):
        """WS message callback for orderbook channel."""
        msg_type = data.get("type", "")
        if msg_type not in ("update/order_book", "subscribed/order_book"):
            return
        payload = data.get("order_book")
        if payload is None:
            return

        ob = self._shared_market.local_order_book
        bids_in = payload.get("bids", [])
        asks_in = payload.get("asks", [])

        is_snapshot = apply_orderbook_update(
            ob["bids"], ob["asks"], ob["initialized"], bids_in, asks_in,
            is_snapshot_hint=(msg_type == "subscribed/order_book"),
        )
        if is_snapshot:
            ob["initialized"] = True

        self._shared_market.ws_connection_healthy = True
        self._shared_market.last_order_book_update = time.monotonic()

        if not ob["bids"] or not ob["asks"]:
            self._shared_market.mid_price = None
            return

        best_bid = ob["bids"].peekitem(-1)[0]
        best_ask = ob["asks"].peekitem(0)[0]
        mid = (best_bid + best_ask) / 2
        self._shared_market.mid_price = mid
        if self._cj_estimator is not None:
            self._cj_estimator.on_book_update(mid)

        # Fan-out: feed all slot calculators + check fills
        ba = self._shared_alpha
        alpha_override = None
        if ba is not None and ba.warmed_up and not ba.is_stale(BINANCE_STALE_SECONDS):
            alpha_override = ba.alpha

        for slot in self._slots:
            calc = slot.slot_state.vol_obi_state.calculator
            if calc is not None:
                calc.set_alpha_override(alpha_override)
                calc.on_book_update(mid, ob["bids"], ob["asks"])

            # Check fills
            slot.dry_engine.check_fills(ob["bids"], ob["asks"])

        self._book_seq += 1
        self._book_seq_event.set()

    def _on_ticker_message(self, data):
        """WS message callback for ticker channel."""
        msg_type = data.get("type", "")
        if "ticker" not in msg_type:
            return
        ticker = data.get("ticker")
        if ticker is None:
            return
        try:
            best_bid = float(ticker.get("best_bid_price", 0))
            best_ask = float(ticker.get("best_ask_price", 0))
            if best_bid > 0:
                self._shared_market.ticker_best_bid = best_bid
            if best_ask > 0:
                self._shared_market.ticker_best_ask = best_ask
            self._shared_market.ticker_updated_at = time.monotonic()
        except (ValueError, TypeError):
            pass

    def _on_trade_message(self, data):
        """WS callback for public Lighter trades used by the CJ estimator."""
        if self._cj_estimator is None:
            return
        msg_type = data.get("type", "")
        if "trade" not in msg_type:
            return
        trades = data.get("trades") or []
        if not isinstance(trades, list):
            return
        mid = self._shared_market.mid_price if self._shared_market is not None else None
        for trade in trades:
            if isinstance(trade, dict):
                self._cj_estimator.on_trade(trade, mid)

    def _on_ws_disconnect(self):
        self._shared_market.ws_connection_healthy = False
        self._shared_market.mid_price = None
        ob = self._shared_market.local_order_book
        ob["initialized"] = False
        ob["bids"].clear()
        ob["asks"].clear()
        for slot in self._slots:
            calc = slot.slot_state.vol_obi_state.calculator
            if calc is not None:
                calc.reset()

    async def _on_ws_connect(self):
        self._shared_market.ws_connection_healthy = True

    # ------------------------------------------------------------------
    # Per-slot quoting
    # ------------------------------------------------------------------

    def _compute_base_amount(self, mid: float, capital: float, cap_pct: float) -> Optional[float]:
        if mid <= 0 or capital is None or capital <= 0:
            return None
        usd = capital * cap_pct * self._leverage
        size = usd / mid
        tick = self._shared_config.amount_tick_float
        if tick > 0:
            size = round(size / tick) * tick
        # Exchange minimums
        min_base = self._shared_config.min_base_amount
        min_quote = self._shared_config.min_quote_amount
        if min_base > 0 and size < min_base:
            size = min_base
        if min_quote > 0 and size * mid < min_quote:
            size = min_quote / mid
            if tick > 0:
                size = math.ceil(size / tick) * tick
        if MIN_ORDER_VALUE_USD > 0 and size * mid < MIN_ORDER_VALUE_USD:
            size = MIN_ORDER_VALUE_USD / mid
            if tick > 0:
                size = math.ceil(size / tick) * tick
        return size

    def _compute_max_pos(self, mid: float, capital: float, base_amount: float, num_levels: int) -> float:
        if capital is None or capital <= 0 or mid <= 0:
            return 0.0
        raw = capital * self._leverage
        if base_amount > 0:
            raw -= 2.0 * num_levels * base_amount * mid
        return max(0.0, raw * 0.9)

    async def _tick_slot(self, slot: GridSlot, mid: float):
        params = slot.params
        ss = slot.slot_state
        capital = ss.account.available_capital
        if capital is None or capital <= 0 or mid <= 0:
            return

        base_amount = self._compute_base_amount(mid, capital, params.capital_usage_percent)
        if base_amount is None or base_amount <= 0:
            return

        max_pos = self._compute_max_pos(mid, capital, base_amount, params.num_levels)
        ss.account.precomputed_max_pos_usd = max_pos
        ss.account.precomputed_base_amount = base_amount
        slot.dry_engine.set_inventory_boundary_usd(max_pos)
        calc = ss.vol_obi_state.calculator
        if calc is None or not calc.warmed_up:
            return
        if (
            params.quote_engine == "cartea_jaimungal"
            and params.cj_use_estimator
            and self._cj_estimator is not None
        ):
            now = time.monotonic()
            if now - slot.last_cj_refresh >= max(float(params.cj_refresh_seconds), 1.0):
                snapshot = self._cj_estimator.snapshot()
                dynamic_params = self._dynamic_cj_params(params, snapshot)
                update_params = getattr(calc, "update_params", None)
                if callable(update_params):
                    try:
                        update_params(dynamic_params)
                        slot.last_cj_estimator_ready = bool(snapshot.ready)
                        slot.dry_engine.set_external_quality_metrics({
                            "cj_estimator_ready": bool(snapshot.ready),
                            "cj_kappa_fit_quality": round(snapshot.kappa_fit_quality, 6),
                            "cj_kappa_r2_plus": round(snapshot.kappa_r2_plus, 6),
                            "cj_kappa_r2_minus": round(snapshot.kappa_r2_minus, 6),
                            "cj_kappa_points_plus": snapshot.kappa_points_plus,
                            "cj_kappa_points_minus": snapshot.kappa_points_minus,
                            "cj_estimated_sigma2_per_sec": round(snapshot.sigma2_per_sec, 10),
                        })
                    except (ValueError, OverflowError):
                        logger.warning(
                            "CJ dynamic params rejected for %s; keeping previous surface",
                            slot.param_key,
                            exc_info=True,
                        )
                slot.last_cj_refresh = now
        if max_pos > 0:
            calc.set_max_position_dollar(max_pos)
        set_inventory_unit = getattr(calc, "set_inventory_unit_base", None)
        if callable(set_inventory_unit):
            set_inventory_unit(base_amount)

        try:
            buy_0, sell_0 = calc.quote(mid, ss.account.position_size)
        except (ValueError, ZeroDivisionError, OverflowError):
            return
        if buy_0 is None and sell_0 is None:
            return

        # Position limit suppression
        if max_pos > 0:
            pos_val = abs(ss.account.position_size) * mid
            if pos_val >= max_pos:
                if ss.account.position_size > 0:
                    buy_0 = None
                elif ss.account.position_size < 0:
                    sell_0 = None
                if buy_0 is None and sell_0 is None:
                    return

        # Build level prices
        tick = self._shared_config.price_tick_float
        levels = [(buy_0, sell_0)]
        bid_depth = (mid - buy_0) if buy_0 is not None else None
        ask_depth = (sell_0 - mid) if sell_0 is not None else None
        for lvl in range(1, params.num_levels):
            factor = slot.spread_factors[lvl]
            raw_bid = (mid - bid_depth * factor) if bid_depth is not None else None
            raw_ask = (mid + ask_depth * factor) if ask_depth is not None else None
            if raw_bid is not None and tick > 0:
                raw_bid = math.floor(raw_bid / tick) * tick
            if raw_ask is not None and tick > 0:
                raw_ask = math.ceil(raw_ask / tick) * tick
            levels.append((raw_bid, raw_ask))

        # Collect order ops
        ops = self._collect_slot_ops(slot, levels, base_amount)
        if ops:
            await slot.dry_engine.process_batch(ops)

    def _collect_slot_ops(self, slot: GridSlot, level_prices: list, base_amount: float) -> list:
        ops = []
        orders = slot.slot_state.orders
        threshold = 10.0  # fixed threshold (no quota pressure in dry-run)
        amount_tick = self._shared_config.amount_tick_float

        for level, (buy_price, sell_price) in enumerate(level_prices):
            for is_buy, new_price in [(True, buy_price), (False, sell_price)]:
                side = "buy" if is_buy else "sell"
                new_size = base_amount

                if new_price is None:
                    existing_id = orders.bid_order_ids[level] if is_buy else orders.ask_order_ids[level]
                    if existing_id is not None:
                        exchange_id = slot.client_to_exchange_id.get(existing_id)
                        if exchange_id is not None:
                            ops.append(BatchOp(
                                side=side, level=level, action="cancel",
                                price=0, size=0,
                                order_id=existing_id, exchange_id=exchange_id,
                            ))
                    continue
                if new_size is None or new_size <= 0 or new_price <= 0:
                    continue

                if is_buy:
                    existing_id = orders.bid_order_ids[level]
                    existing_price = orders.bid_prices[level]
                    existing_size = orders.bid_sizes[level]
                else:
                    existing_id = orders.ask_order_ids[level]
                    existing_price = orders.ask_prices[level]
                    existing_size = orders.ask_sizes[level]

                if existing_id is not None:
                    exchange_id = slot.client_to_exchange_id.get(existing_id)
                    if exchange_id is None:
                        continue
                    change_bps = _price_change_bps(existing_price, new_price)
                    size_changed = (
                        existing_size is None
                        or abs(existing_size - new_size) >= max(amount_tick if amount_tick > 0 else EPSILON, EPSILON)
                    )
                    if existing_price is not None and change_bps <= threshold and not size_changed:
                        continue
                    ops.append(BatchOp(
                        side=side, level=level, action="modify",
                        price=new_price, size=new_size,
                        order_id=existing_id, exchange_id=exchange_id,
                    ))
                else:
                    new_order_id = slot.next_client_order_index()
                    ops.append(BatchOp(
                        side=side, level=level, action="create",
                        price=new_price, size=new_size,
                        order_id=new_order_id, exchange_id=0,
                    ))
        return ops

    # ------------------------------------------------------------------
    # Summary logging
    # ------------------------------------------------------------------

    @staticmethod
    def _quality_score(total_pnl: float, fill_count: int, total_volume: float, quality: dict) -> float:
        if fill_count <= 0:
            return -999.0
        adverse_5s = float(quality.get("adverse_markout_bps_avg", {}).get("5.0", 0.0))
        spread_capture = float(quality.get("spread_capture_bps_avg", 0.0))
        boundary_ratio = float(quality.get("inventory_boundary_ratio", 0.0))
        fit_quality = quality.get("cj_kappa_fit_quality")
        pnl_bps = (total_pnl / total_volume * 10_000.0) if total_volume > 0 else 0.0
        fill_bonus = min(math.log1p(fill_count), 5.0)
        fit_penalty = 0.0
        if fit_quality is not None:
            fit_penalty = max(0.0, 0.30 - float(fit_quality)) * 15.0
        return (
            pnl_bps
            + 0.25 * spread_capture
            + fill_bonus
            - 0.75 * adverse_5s
            - 25.0 * boundary_ratio
            - fit_penalty
        )

    def _maybe_log_grid_summary(self):
        now = time.monotonic()
        if now - self._last_summary < self._summary_interval:
            return
        self._last_summary = now
        elapsed = now - self._start_time
        elapsed_str = f"{elapsed / 3600:.1f}h"

        mid = self._shared_market.mid_price or 0.0

        lines = [f"GRID SUMMARY ({len(self._slots)} slots, {elapsed_str} elapsed, mid=${mid:.2f})"]
        if self._cj_estimator is not None:
            snap = self._cj_estimator.snapshot()
            lines.append(
                "CJ estimator: ready=%s lp=%.3f lm=%.3f kp=%.4f km=%.4f "
                "r2=%.2f/%.2f fit=%.2f ep=%.2f em=%.2f sigma2=%.6f trades=%d/%d markouts=%d/%d"
                % (
                    snap.ready,
                    snap.lambda_plus,
                    snap.lambda_minus,
                    snap.kappa_plus,
                    snap.kappa_minus,
                    snap.kappa_r2_plus,
                    snap.kappa_r2_minus,
                    snap.kappa_fit_quality,
                    snap.epsilon_plus,
                    snap.epsilon_minus,
                    snap.sigma2_per_sec,
                    snap.trade_count_plus,
                    snap.trade_count_minus,
                    snap.markout_count_plus,
                    snap.markout_count_minus,
                )
            )
        lines.append(f"{'Slot':<5} | {'Engine':>5} | {'shape':>8} | {'sk/eps':>6} | {'Fills':>5} | {'Total':>9} | {'Score':>7} | {'Sprd':>6} | {'Adv5':>6} | {'Inv%':>5}")
        lines.append("-" * 94)

        best_slot = None
        best_pnl = float("-inf")
        for slot in self._slots:
            e = slot.dry_engine
            # Refresh portfolio value
            ss = slot.slot_state
            ss.account.portfolio_value = (
                e._initial_portfolio_value + e._realized_pnl + e.unrealized_pnl
            )
            total = e.total_pnl
            p = slot.params
            engine = "CJ" if p.quote_engine == "cartea_jaimungal" else "OBI"
            shape = f"k{p.cj_kappa_plus or p.cj_kappa:g}" if engine == "CJ" else f"v{p.vol_to_half_spread:g}"
            skew_or_eps = (p.cj_epsilon_plus or p.cj_epsilon) if engine == "CJ" else p.skew
            quality = e.quality_metrics()
            score = self._quality_score(total, e._fill_count, e._total_volume, quality)
            adv5 = float(quality.get("adverse_markout_bps_avg", {}).get("5.0", 0.0))
            lines.append(
                f"{p.label:<5} | {engine:>5} | {shape:>8} | {skew_or_eps:>6.1f} | "
                f"{e._fill_count:>5d} | ${total:>8.4f} | {score:>7.2f} | "
                f"{quality.get('spread_capture_bps_avg', 0.0):>6.2f} | {adv5:>6.2f} | "
                f"{quality.get('inventory_boundary_ratio', 0.0) * 100:>4.0f}%"
            )
            if total > best_pnl:
                best_pnl = total
                best_slot = slot

        if best_slot is not None:
            bp = best_slot.params
            if bp.quote_engine == "cartea_jaimungal":
                lines.append(
                    f"Best: {bp.label} (engine=CJ, kappa={bp.cj_kappa_plus or bp.cj_kappa}, "
                    f"epsilon={bp.cj_epsilon_plus or bp.cj_epsilon}, phi={bp.cj_phi}) total=${best_pnl:.4f}"
                )
            else:
                lines.append(
                    f"Best: {bp.label} (engine=OBI, v2hs={bp.vol_to_half_spread}, "
                    f"mhbp={bp.min_half_spread_bps}, skew={bp.skew}) total=${best_pnl:.4f}"
                )

        summary_text = "\n".join(lines)
        logger.info("\n%s", summary_text)

        # Also write to file
        summary_path = os.path.join(self._grid_dir, "summary.log")
        with open(summary_path, "a") as f:
            f.write(f"[{datetime.now(timezone.utc).isoformat()}]\n{summary_text}\n\n")

        # Flush state + trade logs for all slots (async to executor)
        loop = asyncio.get_event_loop()
        for slot in self._slots:
            loop.run_in_executor(None, slot.dry_engine._flush_to_disk_sync)

    def _log_final_summary(self):
        """Write final CSV with all slot results."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self._grid_dir, f"results_{self._symbol}_{ts}.csv")
        fieldnames = [
            "slot", "param_key", "quote_engine",
            "vol_to_half_spread", "min_half_spread_bps", "skew",
            "spread_factor_level1", "capital_usage_percent", "num_levels", "c1_ticks",
            "cj_lambda_plus", "cj_lambda_minus", "cj_kappa_plus", "cj_kappa_minus",
            "cj_epsilon_plus", "cj_epsilon_minus", "cj_sigma2_per_sec",
            "cj_sigma2_scale", "cj_volatility_spread_multiplier", "cj_alpha", "cj_phi",
            "cj_horizon_seconds", "cj_q_max", "cj_spread_multiplier",
            "fills", "realized_pnl", "unrealized_pnl", "total_pnl",
            "total_volume", "portfolio_value", "quality_score",
            "maker_fill_count", "taker_fill_count", "spread_capture_bps_avg",
            "markout_1s_bps_avg", "markout_5s_bps_avg", "markout_30s_bps_avg",
            "adverse_markout_1s_bps_avg", "adverse_markout_5s_bps_avg",
            "adverse_markout_30s_bps_avg", "inventory_boundary_ratio",
            "inventory_max_usd_seen", "cj_estimator_ready",
            "cj_kappa_fit_quality", "cj_kappa_r2_plus", "cj_kappa_r2_minus",
            "cj_kappa_points_plus", "cj_kappa_points_minus", "cj_estimated_sigma2_per_sec",
        ]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for slot in self._slots:
                e = slot.dry_engine
                p = slot.params
                quality = e.quality_metrics()
                markout = quality.get("markout_bps_avg", {})
                adverse = quality.get("adverse_markout_bps_avg", {})
                quality_score = self._quality_score(e.total_pnl, e._fill_count, e._total_volume, quality)
                writer.writerow({
                    "slot": p.label,
                    "param_key": slot.param_key,
                    "quote_engine": p.quote_engine,
                    "vol_to_half_spread": p.vol_to_half_spread,
                    "min_half_spread_bps": p.min_half_spread_bps,
                    "skew": p.skew,
                    "spread_factor_level1": p.spread_factor_level1,
                    "capital_usage_percent": p.capital_usage_percent,
                    "num_levels": p.num_levels,
                    "c1_ticks": p.c1_ticks,
                    "cj_lambda_plus": p.cj_lambda_plus or p.cj_lambda,
                    "cj_lambda_minus": p.cj_lambda_minus or p.cj_lambda,
                    "cj_kappa_plus": p.cj_kappa_plus or p.cj_kappa,
                    "cj_kappa_minus": p.cj_kappa_minus or p.cj_kappa,
                    "cj_epsilon_plus": p.cj_epsilon_plus or p.cj_epsilon,
                    "cj_epsilon_minus": p.cj_epsilon_minus or p.cj_epsilon,
                    "cj_sigma2_per_sec": p.cj_sigma2_per_sec,
                    "cj_sigma2_scale": p.cj_sigma2_scale,
                    "cj_volatility_spread_multiplier": p.cj_volatility_spread_multiplier,
                    "cj_alpha": p.cj_alpha,
                    "cj_phi": p.cj_phi,
                    "cj_horizon_seconds": p.cj_horizon_seconds,
                    "cj_q_max": p.cj_q_max,
                    "cj_spread_multiplier": p.cj_spread_multiplier,
                    "fills": e._fill_count,
                    "realized_pnl": round(e._realized_pnl, 6),
                    "unrealized_pnl": round(e.unrealized_pnl, 6),
                    "total_pnl": round(e.total_pnl, 6),
                    "total_volume": round(e._total_volume, 2),
                    "portfolio_value": round(slot.slot_state.account.portfolio_value or 0, 2),
                    "quality_score": round(quality_score, 6),
                    "maker_fill_count": quality.get("maker_fill_count", 0),
                    "taker_fill_count": quality.get("taker_fill_count", 0),
                    "spread_capture_bps_avg": round(quality.get("spread_capture_bps_avg", 0.0), 6),
                    "markout_1s_bps_avg": round(markout.get("1.0", 0.0), 6),
                    "markout_5s_bps_avg": round(markout.get("5.0", 0.0), 6),
                    "markout_30s_bps_avg": round(markout.get("30.0", 0.0), 6),
                    "adverse_markout_1s_bps_avg": round(adverse.get("1.0", 0.0), 6),
                    "adverse_markout_5s_bps_avg": round(adverse.get("5.0", 0.0), 6),
                    "adverse_markout_30s_bps_avg": round(adverse.get("30.0", 0.0), 6),
                    "inventory_boundary_ratio": round(quality.get("inventory_boundary_ratio", 0.0), 6),
                    "inventory_max_usd_seen": round(quality.get("inventory_max_usd_seen", 0.0), 6),
                    "cj_estimator_ready": slot.last_cj_estimator_ready,
                    "cj_kappa_fit_quality": quality.get("cj_kappa_fit_quality", ""),
                    "cj_kappa_r2_plus": quality.get("cj_kappa_r2_plus", ""),
                    "cj_kappa_r2_minus": quality.get("cj_kappa_r2_minus", ""),
                    "cj_kappa_points_plus": quality.get("cj_kappa_points_plus", ""),
                    "cj_kappa_points_minus": quality.get("cj_kappa_points_minus", ""),
                    "cj_estimated_sigma2_per_sec": quality.get("cj_estimated_sigma2_per_sec", ""),
                })
        logger.info("Final results written to %s", csv_path)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _main_loop(self):
        warmup_start = time.monotonic()
        warmup_logged = False
        last_seq = 0

        while True:
            # Warmup gate
            elapsed = time.monotonic() - warmup_start
            if elapsed < self._warmup_seconds:
                if not warmup_logged or int(elapsed) % 60 == 0:
                    logger.info("Grid warmup: %.0f/%.0f seconds", elapsed, self._warmup_seconds)
                    warmup_logged = True
                await asyncio.sleep(1.0)
                continue

            # Check all calculators warmed up (should be, since they all saw same data)
            any_ready = any(
                s.slot_state.vol_obi_state.calculator is not None
                and s.slot_state.vol_obi_state.calculator.warmed_up
                for s in self._slots
            )
            if not any_ready:
                await asyncio.sleep(0.5)
                continue

            # Wait for new book data
            if self._book_seq == last_seq:
                self._book_seq_event.clear()
                try:
                    await asyncio.wait_for(self._book_seq_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
            last_seq = self._book_seq

            mid = self._shared_market.mid_price
            if mid is None or mid <= 0:
                await asyncio.sleep(0.1)
                continue

            # Tick all slots
            for slot in self._slots:
                await self._tick_slot(slot, mid)

            # Periodic summary
            self._maybe_log_grid_summary()

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self):
        global logger
        logger = setup_logging(
            "grid_dry_run", log_dir=self._log_dir, log_filename="grid_debug.log",
            clear_file=True,
        )
        logger.info("=== GRID DRY-RUN starting: %s ===", self._symbol)
        self._start_time = time.monotonic()
        self._last_summary = self._start_time

        # Fetch market details
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        order_api = lighter.OrderApi(api_client)

        market_id, price_tick, amount_tick = await get_market_details_async(self._symbol)
        if market_id is None:
            logger.error("Could not retrieve market details for %s. Exiting.", self._symbol)
            return

        # Shared market state
        self._shared_config = MarketConfig(
            market_id=market_id,
            price_tick_size=Decimal(str(price_tick)),
            amount_tick_size=Decimal(str(amount_tick)) if amount_tick else Decimal(0),
            price_tick_float=float(price_tick),
            amount_tick_float=float(amount_tick) if amount_tick else 0.0,
        )

        # Fetch exchange minimums
        try:
            order_books_resp = await order_api.order_books()
            for ob in order_books_resp.order_books:
                if ob.market_id == market_id:
                    self._shared_config.min_base_amount = float(getattr(ob, "min_base_amount", 0) or 0)
                    self._shared_config.min_quote_amount = float(getattr(ob, "min_quote_amount", 0) or 0)
                    break
        except Exception as exc:
            logger.warning("Could not fetch min order sizes: %s", exc)

        self._shared_market = MarketState(
            local_order_book={"bids": _BookSide(), "asks": _BookSide(), "initialized": False},
        )

        logger.info(
            "Market %s: id=%s, tick(price)=%s, tick(amount)=%s",
            self._symbol, market_id, price_tick, amount_tick,
        )

        # Binance feeds (shared)
        binance_tasks = []
        if ALPHA_SOURCE == "binance":
            binance_sym = lighter_to_binance_symbol(self._symbol)
            if binance_sym is not None:
                self._shared_bbo = SharedBBO(min_samples=BINANCE_BBO_MIN_SAMPLES)
                bbo_client = BinanceBookTickerClient(
                    binance_symbol=binance_sym,
                    shared_bbo=self._shared_bbo,
                    stale_threshold=BINANCE_BBO_STALE_SECONDS,
                )
                binance_tasks.append(asyncio.create_task(bbo_client.run()))

                self._shared_alpha = SharedAlpha(min_samples=BINANCE_OBI_MIN_SAMPLES)
                depth_client = BinanceDiffDepthClient(
                    binance_symbol=binance_sym,
                    shared_alpha=self._shared_alpha,
                    window_size=BINANCE_OBI_WINDOW,
                    looking_depth=BINANCE_OBI_LOOKING_DEPTH,
                    stale_threshold=BINANCE_STALE_SECONDS,
                    snapshot_limit=BINANCE_DEPTH_SNAPSHOT_LIMIT,
                )
                binance_tasks.append(asyncio.create_task(depth_client.run()))
                logger.info("Binance feeds: %s@bookTicker + %s@depth@100ms", binance_sym, binance_sym)

        # Silence per-slot vol_obi warmup spam (128x "warmed up" messages)
        logging.getLogger("vol_obi").setLevel(logging.WARNING)
        logging.getLogger("_vol_obi_fast").setLevel(logging.WARNING)

        # Create slots (after shared state is ready, so SlotState refs are valid)
        self._slots = self._create_slots()
        logger.info("Created %d grid slots", len(self._slots))

        # WS tasks
        ob_task = asyncio.create_task(ws_subscribe_fast(
            channels=[f"order_book/{market_id}"],
            label="grid orderbook",
            on_message=self._on_book_update,
            url=WEBSOCKET_URL,
            ping_interval=WS_PING_INTERVAL,
            recv_timeout=WS_RECV_TIMEOUT,
            reconnect_base=WS_RECONNECT_BASE,
            reconnect_max=WS_RECONNECT_MAX,
            on_connect=self._on_ws_connect,
            on_disconnect=self._on_ws_disconnect,
            logger=logger,
            reconnect_event=self._ws_reconnect_event,
        ))
        ticker_task = asyncio.create_task(ws_subscribe_fast(
            channels=[f"ticker/{market_id}"],
            label="grid ticker",
            on_message=self._on_ticker_message,
            url=WEBSOCKET_URL,
            ping_interval=WS_PING_INTERVAL,
            recv_timeout=WS_RECV_TIMEOUT,
            reconnect_base=WS_RECONNECT_BASE,
            reconnect_max=WS_RECONNECT_MAX,
            logger=logger,
        ))
        trade_task = None
        if self._cj_estimator is not None:
            trade_task = asyncio.create_task(ws_subscribe_fast(
                channels=[f"trade/{market_id}"],
                label="grid public trades",
                on_message=self._on_trade_message,
                url=WEBSOCKET_URL,
                ping_interval=WS_PING_INTERVAL,
                recv_timeout=WS_RECV_TIMEOUT,
                reconnect_base=WS_RECONNECT_BASE,
                reconnect_max=WS_RECONNECT_MAX,
                logger=logger,
            ))
            logger.info("CJ estimator enabled: subscribed to trade/%s", market_id)

        main_task = asyncio.create_task(self._main_loop())

        all_tasks = [ob_task, ticker_task, main_task] + ([trade_task] if trade_task is not None else []) + binance_tasks

        # Signal handling
        loop = asyncio.get_running_loop()

        def _shutdown(sig):
            logger.info("Received %s — shutting down grid...", sig.name)
            for t in all_tasks:
                if not t.done():
                    t.cancel()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _shutdown, sig)
            except NotImplementedError:
                pass

        try:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            # Flush all slots' state and trade logs
            logger.info("Saving all slot states...")
            for slot in self._slots:
                try:
                    slot.dry_engine.save_state()
                    slot.trade_logger.flush()
                except Exception as exc:
                    logger.warning("Failed to save slot %s: %s", slot.label, exc)
            self._log_final_summary()
            self._maybe_log_grid_summary()
            logger.info("=== GRID DRY-RUN finished ===")
