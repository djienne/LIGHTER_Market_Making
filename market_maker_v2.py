# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement
# flake8: noqa: F401
# isort: skip_file

import asyncio
import logging
import lighter
import os
import time
import orjson as json
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple, Optional
from datetime import datetime
from lighter.exceptions import ApiException
from decimal import Decimal
import signal
from collections import deque
import argparse
import requests
import sys as _sys
import types as _types
from sortedcontainers import SortedDict
import websockets
from utils import EPSILON, get_market_details_async, load_config_params
from adjust_leverage import adjust_leverage
from orderbook import apply_orderbook_update
from ws_manager import ws_subscribe
from orderbook_sanity import check_orderbook_sanity
from vol_obi import VolObiCalculator
from binance_obi import BinanceObiClient, SharedAlpha, lighter_to_binance_symbol
from logging_config import setup_logging
from dotenv import load_dotenv

load_dotenv()

# =========================
# Env & constants (env var > config.json > hardcoded default)
# =========================
_config = load_config_params()
_trading = _config.get("trading", {})
_perf = _config.get("performance", {})
_ws = _config.get("websocket", {})
_safety = _config.get("safety", {})

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WEBSOCKET_URL = "wss://mainnet.zklighter.elliot.ai/stream"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "0"))

MARKET_SYMBOL = os.getenv("MARKET_SYMBOL", "BTC")
MARKET_ID = None
PRICE_TICK_SIZE = None
AMOUNT_TICK_SIZE = None

LEVERAGE = int(os.getenv("LEVERAGE", _trading.get("leverage", 1)))
MARGIN_MODE = os.getenv("MARGIN_MODE", _trading.get("margin_mode", "cross"))
POSITION_VALUE_THRESHOLD_USD = float(os.getenv(
    "POSITION_VALUE_THRESHOLD_USD",
    _trading.get("position_value_threshold_usd", 15.0)))
MIN_ORDER_VALUE_USD = float(_trading.get("min_order_value_usd", 14.5))

LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Trading config
BASE_AMOUNT = float(os.getenv(
    "BASE_AMOUNT",
    _trading.get("base_amount", 0.047)))
CAPITAL_USAGE_PERCENT = float(os.getenv(
    "CAPITAL_USAGE_PERCENT",
    _trading.get("capital_usage_percent", 0.12)))
ORDER_TIMEOUT = float(os.getenv(
    "ORDER_TIMEOUT",
    _trading.get("order_timeout_seconds", 5.0)))
DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS = float(os.getenv(
    "DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS",
    _trading.get("default_quote_update_threshold_bps", 10.0)))
QUOTE_UPDATE_THRESHOLD_BPS = DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS  # backward-compatible alias
SPREAD_FACTOR_LEVEL1 = float(os.getenv(
    "SPREAD_FACTOR_LEVEL1",
    _trading.get("spread_factor_level1", 2.0)))
MIN_LOOP_INTERVAL = float(os.getenv(
    "MIN_LOOP_INTERVAL",
    _perf.get("min_loop_interval", 0.1)))
STALE_ORDER_POLLER_INTERVAL_SEC = float(os.getenv(
    "STALE_ORDER_POLLER_INTERVAL_SEC",
    _safety.get("stale_order_poller_interval_sec", 3.0)))
STALE_ORDER_DEBOUNCE_COUNT = int(os.getenv(
    "STALE_ORDER_DEBOUNCE_COUNT",
    _safety.get("stale_order_debounce_count", 2)))
MAX_CONSECUTIVE_ORDER_REJECTIONS = int(os.getenv(
    "MAX_CONSECUTIVE_ORDER_REJECTIONS",
    _safety.get("max_consecutive_order_rejections", 5)))
CIRCUIT_BREAKER_COOLDOWN_SEC = float(os.getenv(
    "CIRCUIT_BREAKER_COOLDOWN_SEC",
    _safety.get("circuit_breaker_cooldown_sec", 60.0)))
ORDER_RECONCILE_TIMEOUT_SEC = float(os.getenv(
    "ORDER_RECONCILE_TIMEOUT_SEC",
    _safety.get("order_reconcile_timeout_sec", 2.0)))
MAX_LIVE_ORDERS_PER_MARKET = int(os.getenv(
    "MAX_LIVE_ORDERS_PER_MARKET",
    _safety.get("max_live_orders_per_market", 2)))

# Quota recovery config
_quota_recovery_cfg = _perf.get("quota_recovery", {})
_QR_ENABLED = bool(_quota_recovery_cfg.get("enabled", True))
_QR_TRIGGER = int(_quota_recovery_cfg.get("trigger_threshold", 5))
_QR_TARGET = int(_quota_recovery_cfg.get("target_quota", 50))
_QR_MAX_ATTEMPTS = int(_quota_recovery_cfg.get("max_attempts", 3))
_QR_MAX_LOSS = float(_quota_recovery_cfg.get("max_loss_usd", 2.0))
_QR_COOLDOWN = float(_quota_recovery_cfg.get("cooldown_seconds", 120))

# WebSocket tuning
WS_PING_INTERVAL = int(os.getenv(
    "WS_PING_INTERVAL",
    _ws.get("ping_interval", 20)))
WS_RECV_TIMEOUT = float(os.getenv(
    "WS_RECV_TIMEOUT",
    _ws.get("recv_timeout", 30.0)))
WS_RECONNECT_BASE_DELAY = int(os.getenv(
    "WS_RECONNECT_BASE_DELAY",
    _ws.get("reconnect_base_delay", 5)))
WS_RECONNECT_MAX_DELAY = int(os.getenv(
    "WS_RECONNECT_MAX_DELAY",
    _ws.get("reconnect_max_delay", 60)))
# Account channels are event-driven (only fire on fills/position changes),
# so they need a much longer watchdog timeout than market data channels.
WS_ACCOUNT_RECV_TIMEOUT = float(os.getenv(
    "WS_ACCOUNT_RECV_TIMEOUT",
    _ws.get("account_recv_timeout", 300.0)))

# Pre-computed tick sizes as floats (set once in main())
_PRICE_TICK_FLOAT = 0.0
_AMOUNT_TICK_FLOAT = 0.0


def _validate_config() -> None:
    """Validate configuration bounds at startup. Raises ValueError on bad config."""
    errors = []
    if CAPITAL_USAGE_PERCENT <= 0 or CAPITAL_USAGE_PERCENT > 0.5:
        errors.append(f"CAPITAL_USAGE_PERCENT={CAPITAL_USAGE_PERCENT} must be in (0, 0.5] (per-side; both sides = 2x)")
    if LEVERAGE < 1 or LEVERAGE > 20:
        errors.append(f"LEVERAGE={LEVERAGE} must be in [1, 20]")
    if MIN_LOOP_INTERVAL < 0.05:
        errors.append(f"MIN_LOOP_INTERVAL={MIN_LOOP_INTERVAL} must be >= 0.05")
    if MAX_CONSECUTIVE_ORDER_REJECTIONS < 1:
        errors.append(f"MAX_CONSECUTIVE_ORDER_REJECTIONS={MAX_CONSECUTIVE_ORDER_REJECTIONS} must be >= 1")
    if CIRCUIT_BREAKER_COOLDOWN_SEC < 1.0:
        errors.append(f"CIRCUIT_BREAKER_COOLDOWN_SEC={CIRCUIT_BREAKER_COOLDOWN_SEC} must be >= 1.0")
    if BASE_AMOUNT <= 0:
        errors.append(f"BASE_AMOUNT={BASE_AMOUNT} must be > 0")
    if ORDER_TIMEOUT <= 0:
        errors.append(f"ORDER_TIMEOUT={ORDER_TIMEOUT} must be > 0")
    if SPREAD_FACTOR_LEVEL1 < 1.0:
        errors.append(f"SPREAD_FACTOR_LEVEL1={SPREAD_FACTOR_LEVEL1} must be >= 1.0")
    if errors:
        raise ValueError("Invalid configuration:\n  " + "\n  ".join(errors))


@dataclass
class BatchOp:
    side: str          # "buy" or "sell"
    level: int
    action: str        # "create", "modify", "cancel"
    price: float       # target price (not used for cancel)
    size: float        # target size (not used for cancel)
    order_id: int      # existing order_id (for modify/cancel) or new client_order_index (for create)
    exchange_id: int   # resolved exchange order_index (for modify/cancel)


def _to_raw_price(price: float) -> int:
    """Convert a human-readable price to the raw integer the SDK expects."""
    tick = state.config.price_tick_float
    if tick <= 0:
        raise ValueError("price_tick_float not initialised")
    return int(round(price / tick))


def _to_raw_amount(amount: float) -> int:
    """Convert a human-readable base amount to the raw integer the SDK expects."""
    tick = state.config.amount_tick_float
    if tick <= 0:
        raise ValueError("amount_tick_float not initialised")
    return int(round(amount / tick))


def build_signer_client(url: str, account_index: int, private_key: str, api_key_index: int):
    """Build SignerClient across SDK versions with different constructor signatures."""
    if not private_key:
        raise ValueError("API private key is required to build SignerClient")

    # Newer lighter SDK signature.
    try:
        return lighter.SignerClient(
            url=url,
            account_index=account_index,
            api_private_keys={api_key_index: private_key},
        )
    except TypeError:
        pass

    # Older lighter SDK signature.
    try:
        return lighter.SignerClient(
            url=url,
            private_key=private_key,
            api_key_index=api_key_index,
            account_index=account_index,
            private_keys={api_key_index: private_key},
        )
    except TypeError as exc:
        raise RuntimeError(
            "Unsupported lighter.SignerClient constructor. "
            "Please upgrade/downgrade lighter SDK to a compatible version."
        ) from exc


async def _cancel_task_with_timeout(task: Optional[asyncio.Task], label: str, timeout: float = 3.0) -> None:
    """Cancel a background task without risking an unbounded await."""
    if task is None or task.done():
        return
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("Timeout while cancelling task '%s' (%.1fs)", label, timeout)
    except asyncio.CancelledError:
        pass

# Orderbook sanity check interval (seconds)
SANITY_CHECK_INTERVAL = int(os.getenv(
    "SANITY_CHECK_INTERVAL",
    _trading.get("sanity_check_interval", 10)))
SANITY_CHECK_TOLERANCE_PCT = float(os.getenv(
    "SANITY_CHECK_TOLERANCE_PCT",
    _trading.get("sanity_check_tolerance_pct", 0.5)))

_vol_obi_cfg = _trading.get("vol_obi", {})
VOL_OBI_WINDOW_STEPS = int(os.getenv(
    "VOL_OBI_WINDOW_STEPS", _vol_obi_cfg.get("window_steps", 6000)))
VOL_OBI_STEP_NS = int(os.getenv(
    "VOL_OBI_STEP_NS", _vol_obi_cfg.get("step_ns", 100_000_000)))
VOL_OBI_VOL_TO_HALF_SPREAD = float(os.getenv(
    "VOL_OBI_VOL_TO_HALF_SPREAD", _vol_obi_cfg.get("vol_to_half_spread", 0.8)))
VOL_OBI_MIN_HALF_SPREAD_BPS = float(os.getenv(
    "VOL_OBI_MIN_HALF_SPREAD_BPS", _vol_obi_cfg.get("min_half_spread_bps", 2.0)))
VOL_OBI_C1_TICKS = float(os.getenv(
    "VOL_OBI_C1_TICKS", _vol_obi_cfg.get("c1_ticks", 160.0)))
VOL_OBI_SKEW = float(os.getenv(
    "VOL_OBI_SKEW", _vol_obi_cfg.get("skew", 1.0)))
VOL_OBI_LOOKING_DEPTH = float(os.getenv(
    "VOL_OBI_LOOKING_DEPTH", _vol_obi_cfg.get("looking_depth", 0.025)))
VOL_OBI_MIN_WARMUP_SAMPLES = int(os.getenv(
    "VOL_OBI_MIN_WARMUP_SAMPLES", _vol_obi_cfg.get("min_warmup_samples", 100)))
VOL_OBI_MAX_POSITION_DOLLAR = float(os.getenv(
    "VOL_OBI_MAX_POSITION_DOLLAR", _vol_obi_cfg.get("max_position_dollar", 500.0)))
WARMUP_SECONDS = float(os.getenv(
    "WARMUP_SECONDS", _vol_obi_cfg.get("warmup_seconds", 600)))

# Binance alpha config
_alpha_cfg = _trading.get("alpha", {})
ALPHA_SOURCE = os.getenv("ALPHA_SOURCE", _alpha_cfg.get("source", "binance"))
BINANCE_STALE_SECONDS = float(os.getenv("BINANCE_STALE_SECONDS", _alpha_cfg.get("stale_seconds", 5.0)))
BINANCE_OBI_WINDOW = int(os.getenv("BINANCE_OBI_WINDOW", _alpha_cfg.get("window_size", 300)))
BINANCE_OBI_MIN_SAMPLES = int(os.getenv("BINANCE_OBI_MIN_SAMPLES", _alpha_cfg.get("min_samples", 150)))
BINANCE_OBI_LOOKING_DEPTH = float(os.getenv("BINANCE_OBI_LOOKING_DEPTH", _alpha_cfg.get("looking_depth", 0.025)))

# Global events and task refs (not part of state — asyncio primitives)
order_book_received = asyncio.Event()
account_state_received = asyncio.Event()
account_all_received = asyncio.Event()
ws_reconnect_event = asyncio.Event()
ws_client = None
ws_task = None
stale_order_task = None

# Serialize all SDK write operations (create/modify/cancel) to avoid nonce collisions.
# The Lighter SDK's nonce counter is not safe for concurrent async calls.
_sdk_write_lock = asyncio.Lock()

_WS_AUTH_TOKEN_TTL = int(os.getenv(
    "WS_AUTH_TOKEN_TTL",
    _ws.get("auth_token_ttl", 8 * 60)))   # seconds — refresh before 10-min server-side expiry
_account_orders_ws_ready = False
_account_orders_ws_connected = asyncio.Event()
RECONCILER_SLOW_INTERVAL_SEC = 60.0

# WS-based cancel confirmation: order_id -> asyncio.Event
_order_cancel_events: dict[int, asyncio.Event] = {}

# =========================
# Logging setup
# =========================
logger = setup_logging(__name__, log_dir=LOG_DIR, log_filename="market_maker_debug.txt")

# Propagate handlers to sub-module loggers so their messages appear in our output
for _sub_logger_name in ('binance_obi', 'vol_obi', 'ws_manager', 'orderbook_sanity'):
    _sub = logging.getLogger(_sub_logger_name)
    _sub.handlers = logger.handlers
    _sub.setLevel(logger.level)
    _sub.propagate = False

# =========================
# Data structures
# =========================
# =========================
# State objects
# =========================
NUM_LEVELS = int(_trading.get("levels_per_side", 2))  # number of order levels per side


@dataclass
class OrderState:
    bid_order_ids: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    ask_order_ids: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    bid_prices: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    ask_prices: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    bid_sizes: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    ask_sizes: list = field(default_factory=lambda: [None] * NUM_LEVELS)
    last_client_order_index: int = 0


@dataclass
class MarketState:
    mid_price: Optional[float] = None
    last_order_book_update: float = 0.0
    ws_connection_healthy: bool = False
    local_order_book: dict = field(default_factory=lambda: {
        'bids': SortedDict(), 'asks': SortedDict(), 'initialized': False
    })
    last_mid_price: Optional[float] = None
    ticker_best_bid: Optional[float] = None
    ticker_best_ask: Optional[float] = None
    ticker_updated_at: float = 0.0


@dataclass
class AccountState:
    available_capital: Optional[float] = None
    portfolio_value: Optional[float] = None
    position_size: float = 0.0
    positions: dict = field(default_factory=dict)
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=20))
    last_capital_update: float = 0.0  # monotonic timestamp of last capital update
    _cached_base_amount: Optional[float] = None
    _cached_base_amount_inputs: tuple = (None, None)


@dataclass
class VolObiState:
    calculator: Optional[VolObiCalculator] = None


@dataclass
class MarketConfig:
    market_id: Optional[int] = None
    price_tick_size: Optional[Decimal] = None
    amount_tick_size: Optional[Decimal] = None
    price_tick_float: float = 0.0
    amount_tick_float: float = 0.0
    min_base_amount: float = 0.0
    min_quote_amount: float = 0.0


class SideStatus(str, Enum):
    IDLE = "IDLE"
    PLACING = "PLACING"
    LIVE = "LIVE"
    MODIFYING = "MODIFYING"
    CANCELING = "CANCELING"
    PAUSED = "PAUSED"
    ERROR_COOLDOWN = "ERROR_COOLDOWN"


@dataclass
class SideOrderLifecycle:
    status: SideStatus = SideStatus.IDLE
    pending_order_id: Optional[int] = None
    pending_cancel_order_id: Optional[int] = None
    target_price: Optional[float] = None
    target_size: Optional[float] = None
    updated_at: float = 0.0


@dataclass
class OrderManagerState:
    bids: list = field(default_factory=lambda: [SideOrderLifecycle() for _ in range(NUM_LEVELS)])
    asks: list = field(default_factory=lambda: [SideOrderLifecycle() for _ in range(NUM_LEVELS)])


@dataclass
class RiskState:
    consecutive_rejections: int = 0
    paused_until: float = 0.0
    pause_reason: str = ""
    last_reconcile_ok: bool = True
    last_reconcile_reason: str = ""
    last_reconcile_time: float = 0.0
    mismatch_streak: int = 0
    pause_cancel_done: bool = False


@dataclass
class MMState:
    orders: OrderState = field(default_factory=OrderState)
    market: MarketState = field(default_factory=MarketState)
    account: AccountState = field(default_factory=AccountState)
    vol_obi_state: VolObiState = field(default_factory=VolObiState)
    config: MarketConfig = field(default_factory=MarketConfig)
    order_manager: OrderManagerState = field(default_factory=OrderManagerState)
    risk: RiskState = field(default_factory=RiskState)
    binance_alpha: Optional[SharedAlpha] = None


state = MMState()


class OrderManager:
    """Maintains per-side, per-level order lifecycle metadata and writes canonical order fields."""

    def __init__(self, lifecycle_state: OrderManagerState):
        self._state = lifecycle_state

    def lifecycle(self, side: str, level: int = 0) -> SideOrderLifecycle:
        return self._state.bids[level] if side == "buy" else self._state.asks[level]

    def mark_status(
        self,
        side: str,
        status: SideStatus,
        *,
        level: int = 0,
        pending_order_id: Optional[int] = None,
        pending_cancel_order_id: Optional[int] = None,
        target_price: Optional[float] = None,
        target_size: Optional[float] = None,
    ) -> None:
        s = self.lifecycle(side, level)
        s.status = status
        s.pending_order_id = pending_order_id
        s.pending_cancel_order_id = pending_cancel_order_id
        s.target_price = target_price
        s.target_size = target_size
        s.updated_at = time.monotonic()

    def bind_live(self, side: str, order_id: int, price: float, size: float, *, level: int = 0) -> None:
        orders = state.orders
        if side == "buy":
            orders.bid_order_ids[level] = order_id
            orders.bid_prices[level] = price
            orders.bid_sizes[level] = size
        else:
            orders.ask_order_ids[level] = order_id
            orders.ask_prices[level] = price
            orders.ask_sizes[level] = size
        self.mark_status(side, SideStatus.LIVE, level=level, target_price=price, target_size=size)

    def clear_live(self, side: str, level: Optional[int] = None) -> None:
        """Clear one level (if level given) or all levels for a side."""
        levels = range(NUM_LEVELS) if level is None else [level]
        orders = state.orders
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

    def clear_all(self) -> None:
        self.clear_live("buy")
        self.clear_live("sell")


class RiskController:
    """Circuit breaker and reconciliation health controller."""

    def __init__(self, risk_state: RiskState):
        self._state = risk_state

    def record_success(self) -> None:
        self._state.consecutive_rejections = 0

    def record_rejection(self, reason: str) -> None:
        self._state.consecutive_rejections += 1
        threshold = MAX_CONSECUTIVE_ORDER_REJECTIONS
        if threshold > 0 and self._state.consecutive_rejections >= threshold:
            self.trigger_pause(
                f"circuit_breaker: {self._state.consecutive_rejections} consecutive rejections ({reason})"
            )

    def trigger_pause(self, reason: str) -> None:
        until = time.monotonic() + max(0.0, CIRCUIT_BREAKER_COOLDOWN_SEC)
        if until > self._state.paused_until:
            self._state.paused_until = until
        self._state.pause_reason = reason
        self._state.pause_cancel_done = False
        logger.error("Trading paused: %s (cooldown %.1fs)", reason, CIRCUIT_BREAKER_COOLDOWN_SEC)

    def is_paused(self) -> bool:
        return time.monotonic() < self._state.paused_until

    def maybe_recover(self, *, websocket_healthy: bool) -> bool:
        if self.is_paused():
            return False
        if self._state.paused_until <= 0:
            return True
        if not self._state.last_reconcile_ok or not websocket_healthy:
            return False
        self._state.paused_until = 0.0
        self._state.pause_reason = ""
        self._state.consecutive_rejections = 0
        self._state.pause_cancel_done = False
        logger.info("Trading resumed after circuit-breaker cooldown.")
        return True

    def mark_reconcile(self, *, ok: bool, reason: str = "") -> None:
        self._state.last_reconcile_ok = ok
        self._state.last_reconcile_reason = reason
        self._state.last_reconcile_time = time.monotonic()
        self._state.mismatch_streak = 0 if ok else (self._state.mismatch_streak + 1)

    @property
    def mismatch_streak(self) -> int:
        return self._state.mismatch_streak

    @property
    def pause_cancel_done(self) -> bool:
        return self._state.pause_cancel_done

    @pause_cancel_done.setter
    def pause_cancel_done(self, value: bool) -> None:
        self._state.pause_cancel_done = value


order_manager = OrderManager(state.order_manager)
risk_controller = RiskController(state.risk)

# Backward-compatible attribute access for tests and external consumers.
# Intercepts both reads (getattr) and writes (setattr) of old flat global names
# and redirects them to the appropriate state sub-object.

# Mapping: name -> (sub_obj, attr) for scalars, or (sub_obj, attr, index) for list-indexed fields.
# List-indexed entries proxy level-0 by default for backward compatibility.
_ATTR_TO_STATE = {
    'current_bid_order_id':    ('orders', 'bid_order_ids', 0),
    'current_ask_order_id':    ('orders', 'ask_order_ids', 0),
    'current_bid_price':       ('orders', 'bid_prices', 0),
    'current_ask_price':       ('orders', 'ask_prices', 0),
    'current_bid_size':        ('orders', 'bid_sizes', 0),
    'current_ask_size':        ('orders', 'ask_sizes', 0),
    'last_client_order_index': ('orders', 'last_client_order_index'),
    'MARKET_ID':               ('config', 'market_id'),
    '_PRICE_TICK_FLOAT':       ('config', 'price_tick_float'),
    '_AMOUNT_TICK_FLOAT':      ('config', 'amount_tick_float'),
    'local_order_book':        ('market', 'local_order_book'),
    'current_mid_price_cached':('market', 'mid_price'),
    'ws_connection_healthy':   ('market', 'ws_connection_healthy'),
    'last_order_book_update':  ('market', 'last_order_book_update'),
    'available_capital':       ('account', 'available_capital'),
    'portfolio_value':         ('account', 'portfolio_value'),
    'current_position_size':   ('account', 'position_size'),
    'account_positions':       ('account', 'positions'),
    'recent_trades':           ('account', 'recent_trades'),
    'vol_obi_calc':            ('vol_obi_state', 'calculator'),
}


class _StateModule(_types.ModuleType):
    """Module class that proxies old flat global names to the state object."""

    def __getattr__(self, name):
        if name in _ATTR_TO_STATE:
            entry = _ATTR_TO_STATE[name]
            obj = getattr(state, entry[0])
            val = getattr(obj, entry[1])
            if len(entry) == 3:
                return val[entry[2]]
            return val
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    def __setattr__(self, name, value):
        if name in _ATTR_TO_STATE:
            entry = _ATTR_TO_STATE[name]
            obj = getattr(state, entry[0])
            if len(entry) == 3:
                getattr(obj, entry[1])[entry[2]] = value
            else:
                setattr(obj, entry[1], value)
        else:
            super().__setattr__(name, value)


_sys.modules[__name__].__class__ = _StateModule


# =========================
# Helpers
# =========================
def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


_MAX_CLIENT_ORDER_INDEX = 281474976710655  # 2^48 - 1 (exchange limit)


def next_client_order_index() -> int:
    new_id = time.time_ns() % _MAX_CLIENT_ORDER_INDEX
    if new_id <= state.orders.last_client_order_index:
        new_id = (state.orders.last_client_order_index + 1) % (_MAX_CLIENT_ORDER_INDEX + 1)
    state.orders.last_client_order_index = new_id
    return new_id


def price_change_bps(old_price: Optional[float], new_price: Optional[float]) -> float:
    if old_price is None or new_price is None or old_price <= 0:
        return float("inf")
    return abs(new_price - old_price) / old_price * 10000.0


def get_position_value_usd(position_size: float, mid_price: Optional[float]) -> float:
    if mid_price is None:
        return 0.0
    return abs(position_size) * mid_price


def position_label(position_size: float) -> str:
    if position_size > 0:
        return "long"
    if position_size < 0:
        return "short"
    return "flat"


def get_best_prices() -> Tuple[Optional[float], Optional[float]]:
    ob = state.market.local_order_book
    if ob['bids'] and ob['asks']:
        try:
            best_bid = ob['bids'].peekitem(-1)[0]
            best_ask = ob['asks'].peekitem(0)[0]
            return best_bid, best_ask
        except (ValueError, IndexError):
            pass
    return None, None


def is_position_significant(position_size: float, mid_price: Optional[float]) -> bool:
    if abs(position_size) < EPSILON:
        return False
    if mid_price is None or mid_price <= 0:
        return True
    return get_position_value_usd(position_size, mid_price) >= POSITION_VALUE_THRESHOLD_USD


async def emergency_close_position(client, reason: str = "startup") -> bool:
    """Detect and aggressively close any open position.

    Uses the locally-received position size and orderbook to place an
    aggressive limit order that crosses the spread, effectively acting
    as a market order.  Returns True if flat afterwards (or already flat).
    """
    pos = state.account.position_size
    mid = state.market.mid_price
    if abs(pos) < EPSILON:
        logger.info("emergency_close (%s): no open position — all clear.", reason)
        return True

    best_bid, best_ask = get_best_prices()
    if best_bid is None or best_ask is None or mid is None:
        logger.error("emergency_close (%s): no orderbook data to close position %.6f", reason, pos)
        return False

    tick = state.config.price_tick_float
    if tick <= 0:
        logger.error("emergency_close (%s): tick size not initialised", reason)
        return False

    close_size = abs(pos)
    # Place aggressive limit order well through the book to guarantee fill
    slippage = max(mid * 0.003, 10.0)  # 0.3% or $10, whichever is larger
    if pos > 0:
        # Long -> sell at best_bid - slippage
        close_price = best_bid - slippage
        close_price = math.floor(close_price / tick) * tick
        is_ask = True
        side_label = "SELL"
    else:
        # Short -> buy at best_ask + slippage
        close_price = best_ask + slippage
        close_price = math.ceil(close_price / tick) * tick
        is_ask = False
        side_label = "BUY"

    order_id = next_client_order_index()
    raw_price = _to_raw_price(close_price)
    raw_size = _to_raw_amount(close_size)

    logger.warning(
        "emergency_close (%s): closing %.6f position — %s %.6f @ %.2f (slippage=%.2f)",
        reason, pos, side_label, close_size, close_price, slippage,
    )

    for attempt in range(5):
        try:
            async with _sdk_write_lock:
                tx, tx_hash, err = await client.create_order(
                    market_index=state.config.market_id,
                    client_order_index=order_id,
                    base_amount=raw_size,
                    price=raw_price,
                    is_ask=is_ask,
                    order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                    time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                )
            if err:
                if _is_transient_error(Exception(str(err))):
                    wait = 30 * (attempt + 1)
                    logger.warning("emergency_close (%s): 429/transient, retry in %ds (attempt %d/5)", reason, wait, attempt + 1)
                    await asyncio.sleep(wait)
                    order_id = next_client_order_index()  # fresh order id
                    continue
                logger.error("emergency_close (%s): order FAILED: %s", reason, err)
                return False
            logger.info("emergency_close (%s): order placed, waiting for fill...", reason)
            break
        except Exception as e:
            if _is_transient_error(e):
                wait = 30 * (attempt + 1)
                logger.warning("emergency_close (%s): 429/transient, retry in %ds (attempt %d/5)", reason, wait, attempt + 1)
                await asyncio.sleep(wait)
                order_id = next_client_order_index()
                continue
            logger.error("emergency_close (%s): exception placing order: %s", reason, e)
            return False
    else:
        logger.error("emergency_close (%s): all 5 attempts exhausted — could not place order", reason)
        return False

    # Wait for the fill to be reflected via WS (up to 5s)
    for _ in range(25):
        await asyncio.sleep(0.2)
        if abs(state.account.position_size) < EPSILON:
            logger.info("emergency_close (%s): position closed successfully.", reason)
            # Clean up any leftover order
            await cancel_all_orders(client)
            return True

    # Didn't fill in time — cancel and report
    logger.error(
        "emergency_close (%s): position still open (%.6f) after 5s. Cancelling order.",
        reason, state.account.position_size,
    )
    await cancel_all_orders(client)
    return abs(state.account.position_size) < EPSILON


def _record_order_success() -> None:
    risk_controller.record_success()
    _reset_global_backoff()


def _record_order_rejection(reason: str) -> None:
    risk_controller.record_rejection(reason)


def _is_transient_error(exc: Exception) -> bool:
    """Return True for rate-limit (429) and nonce errors that should NOT
    trigger the circuit breaker — just a temporary backoff."""
    msg = str(exc).lower()
    return "429" in msg or "too many" in msg or ("not enough" in msg and "quota" in msg) or "invalid nonce" in msg

# ---------------------------------------------------------------------------
# Adaptive rate limiter: sliding-window token bucket
# ---------------------------------------------------------------------------
# "Default" tx-type limit: 40 ops per rolling 60s window (binding constraint)
_RL_OPS_PER_WINDOW = 40
_RL_WINDOW_SECONDS = 60.0
_RL_MIN_SEND_INTERVAL = float(_perf.get("rate_limit_send_interval", 0.1))  # floor between any two sends (seconds)
_RL_CANCEL_MIN_INTERVAL = 0.5    # cancels get a shorter floor

# Volume-quota adaptive thresholds
_RL_QUOTA_HIGH = 500
_RL_QUOTA_MEDIUM = 50
_RL_QUOTA_LOW = 10
_RL_FREE_SLOT_INTERVAL = 15.0    # 1 free tx every 15s (no quota consumed)

# Reduced 429 backoff (user accepts occasional 429s)
_RL_BACKOFF_BASE = 15.0
_RL_BACKOFF_MAX = 120.0
_RL_BACKOFF_RESET_AFTER = 2      # consecutive successes before reset

# State
_op_timestamps: deque = deque()  # monotonic times of each op sent
_last_send_time: float = 0.0
_volume_quota_remaining: Optional[int] = None  # None = unknown until first API response
_quota_warning_level: str = "ok"  # "ok" | "medium" | "low" | "critical"
_consecutive_successes: int = 0
_global_backoff_until: float = 0.0
_global_backoff_consecutive: int = 0

# Quota recovery state
_quota_recovery_in_progress: bool = False
_quota_recovery_last_attempt: float = 0.0  # monotonic time of last recovery
_trading_start_time: float = 0.0  # set when warmup completes; recovery blocked until grace period elapses
_QR_POST_WARMUP_GRACE: float = float(_quota_recovery_cfg.get("post_warmup_grace_seconds", 120))


def _extract_order_index(order: dict) -> Optional[int]:
    """Extract the exchange-assigned order_index from an order dict."""
    raw = order.get("order_index")
    if raw is None:
        raw = order.get("client_order_index")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _extract_client_order_index(order: dict) -> Optional[int]:
    """Extract the client_order_index we originally sent when placing the order."""
    raw = order.get("client_order_index")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Mapping: client_order_index -> exchange order_index
# Populated from WS account_orders updates and REST reconciliation responses.
# Needed because the SDK's cancel_order/modify_order require the exchange
# order_index, but we store client_order_index locally.
# ---------------------------------------------------------------------------
_client_to_exchange_id: dict[int, int] = {}


def _update_id_mapping_from_orders(orders: list[dict]) -> None:
    """Populate client_order_index -> order_index mapping from exchange data."""
    for o in orders:
        try:
            cid = o.get("client_order_index")
            eid = o.get("order_index")
            if cid is not None and eid is not None:
                _client_to_exchange_id[int(cid)] = int(eid)
        except (TypeError, ValueError):
            continue
    # Prevent unbounded growth
    if len(_client_to_exchange_id) > 200:
        to_keep = dict(sorted(_client_to_exchange_id.items())[-100:])
        _client_to_exchange_id.clear()
        _client_to_exchange_id.update(to_keep)


def _resolve_exchange_id(client_id: int) -> Optional[int]:
    """Look up exchange order_index for a client_order_index."""
    return _client_to_exchange_id.get(client_id)


def _extract_order_is_ask(order: dict) -> Optional[bool]:
    raw = order.get("is_ask")
    if raw is not None:
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            val = raw.strip().lower()
            if val in {"1", "true", "yes", "sell", "ask"}:
                return True
            if val in {"0", "false", "no", "buy", "bid"}:
                return False

    side = order.get("side")
    if isinstance(side, str):
        side_val = side.strip().lower()
        if side_val in {"sell", "ask"}:
            return True
        if side_val in {"buy", "bid"}:
            return False
    return None


def _extract_order_price(order: dict) -> Optional[float]:
    raw = order.get("price")
    try:
        price = float(raw)
        return price if price > 0 else None
    except (TypeError, ValueError):
        return None


def _extract_order_size(order: dict) -> Optional[float]:
    raw = order.get("remaining_base_amount")
    if raw is None:
        raw = order.get("size")
    if raw is None:
        raw = order.get("base_amount")
    try:
        size = float(raw)
        return size if size > 0 else None
    except (TypeError, ValueError):
        return None


def _has_exchange_id(client_id: Optional[int]) -> bool:
    return client_id is not None and client_id in _client_to_exchange_id


def _size_change_requires_update(existing_size: Optional[float], new_size: Optional[float]) -> bool:
    if new_size is None:
        return False
    if existing_size is None:
        return True
    tick = state.config.amount_tick_float
    tolerance = tick if tick > 0 else EPSILON
    return abs(existing_size - new_size) >= max(tolerance, EPSILON)


def _sync_tracked_order_from_remote(side: str, level: int, order: dict) -> None:
    cid = _extract_client_order_index(order)
    if cid is None:
        return

    is_ask = _extract_order_is_ask(order)
    if side == "buy" and is_ask is True:
        return
    if side == "sell" and is_ask is False:
        return

    if side == "buy":
        current_price = state.orders.bid_prices[level]
        current_size = state.orders.bid_sizes[level]
    else:
        current_price = state.orders.ask_prices[level]
        current_size = state.orders.ask_sizes[level]

    price = _extract_order_price(order)
    size = _extract_order_size(order)
    if price is None:
        price = current_price
    if size is None:
        size = current_size
    if price is None or size is None:
        order_manager.mark_status(side, SideStatus.LIVE, level=level)
        return
    order_manager.bind_live(side, cid, price, size, level=level)


def _refresh_local_orders_from_remote_orders(remote_orders: list[dict]) -> None:
    remote_by_client: dict[int, dict] = {}
    for order in remote_orders:
        cid = _extract_client_order_index(order)
        if cid is not None:
            remote_by_client[cid] = order

    orders = state.orders
    for level in range(NUM_LEVELS):
        bid_id = orders.bid_order_ids[level]
        if bid_id is not None and bid_id in remote_by_client:
            _sync_tracked_order_from_remote("buy", level, remote_by_client[bid_id])
        ask_id = orders.ask_order_ids[level]
        if ask_id is not None and ask_id in remote_by_client:
            _sync_tracked_order_from_remote("sell", level, remote_by_client[ask_id])


def _has_live_local_orders() -> bool:
    orders = state.orders
    return any(oid is not None for oid in (*orders.bid_order_ids, *orders.ask_order_ids))


def _orders_to_live_client_id_set(orders: list[dict]) -> set[int]:
    """Return the set of *client_order_index* values from the given orders.

    We store client_order_index locally, so matching must use this field.
    """
    live_ids: set[int] = set()
    for order in orders:
        idx = _extract_client_order_index(order)
        if idx is not None:
            live_ids.add(idx)
    return live_ids


async def _fetch_account_active_orders(
    client,
    market_id: Optional[int] = None,
    account_id: Optional[int] = None,
    timeout: float = 10.0,
) -> Optional[list[dict]]:
    """Fetch active orders from exchange using a short-lived auth token."""
    if client is None or not hasattr(client, "create_auth_token_with_expiry"):
        return None
    if market_id is None:
        market_id = state.config.market_id
    if account_id is None:
        account_id = getattr(client, "account_index", None) or ACCOUNT_INDEX
    if market_id is None:
        return None

    auth_token = _generate_ws_auth_token(client)
    if not auth_token:
        return None

    params = {
        "account_index": account_id,
        "market_id": market_id,
        "auth": auth_token,
    }

    loop = asyncio.get_running_loop()

    def _do_request() -> list[dict]:
        resp = requests.get(
            f"{BASE_URL}/api/v1/accountActiveOrders",
            params=params,
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders", [])
        if not isinstance(orders, list):
            raise ValueError("orders field is not a list")
        return orders

    try:
        return await asyncio.wait_for(loop.run_in_executor(None, _do_request), timeout=timeout)
    except asyncio.TimeoutError:
        logger.error("Active-orders fetch timed out after %.1fs", timeout)
        return None
    except (requests.RequestException, ValueError, KeyError, OSError) as exc:
        logger.error(f"Active-orders fetch failed: {exc}")
        return None


def _reconcile_local_orders_with_remote_orders(
    remote_orders: list[dict], source: str = "poller"
) -> tuple[bool, set[int]]:
    """Reconcile local order IDs/price/size against exchange open-order snapshot.

    Returns ``(ok, unknown_exchange_ids)`` where *unknown_exchange_ids* are
    exchange-assigned order_index values for live orders that are not tracked
    locally and should be cancelled by the caller.
    """
    # Populate the client_order_index -> order_index mapping
    _update_id_mapping_from_orders(remote_orders)

    # Build set of live client_order_index values (matching against local state)
    live_client_ids = _orders_to_live_client_id_set(remote_orders)
    # Build client_id -> exchange_id mapping for unknown-order cancellation
    client_to_exchange: dict[int, int] = {}
    for order in remote_orders:
        cid = _extract_client_order_index(order)
        eid = _extract_order_index(order)
        if cid is not None and eid is not None:
            client_to_exchange[cid] = eid

    orders = state.orders
    mismatch_reasons = []

    # Clear locally tracked orders that are no longer live on exchange
    for lvl in range(NUM_LEVELS):
        bid_id = orders.bid_order_ids[lvl]
        if bid_id is not None and bid_id not in live_client_ids:
            mismatch_reasons.append(f"missing_local_bid[{lvl}]:{bid_id}")
            order_manager.clear_live("buy", lvl)
        ask_id = orders.ask_order_ids[lvl]
        if ask_id is not None and ask_id not in live_client_ids:
            mismatch_reasons.append(f"missing_local_ask[{lvl}]:{ask_id}")
            order_manager.clear_live("sell", lvl)

    _refresh_local_orders_from_remote_orders(remote_orders)

    # NOTE: We intentionally do NOT rebind untracked remote orders to empty
    # local slots.  Rebinding creates a race condition: if the main loop has
    # already prepared a "create" op for the empty slot (between
    # collect_order_operations and sign_and_send_batch), the rebound order
    # becomes orphaned on the exchange.  Instead, untracked orders fall through
    # to the unknown_live_ids check and get cancelled; the main loop will
    # recreate what's needed on its next iteration.

    tracked_ids = {
        oid
        for oid in (*orders.bid_order_ids, *orders.ask_order_ids)
        if oid is not None
    }
    unknown_client_ids = live_client_ids - tracked_ids
    # Resolve to exchange order_index for cancellation via SDK
    unknown_exchange_ids = {
        client_to_exchange[cid]
        for cid in unknown_client_ids
        if cid in client_to_exchange
    }
    if unknown_exchange_ids:
        mismatch_reasons.append(f"unknown_live_ids:{sorted(unknown_exchange_ids)}")

    live_count = len(live_client_ids)
    if MAX_LIVE_ORDERS_PER_MARKET > 0 and live_count > MAX_LIVE_ORDERS_PER_MARKET:
        mismatch_reasons.append(
            f"too_many_live_orders:{live_count}>{MAX_LIVE_ORDERS_PER_MARKET}"
        )
        risk_controller.trigger_pause(
            f"exchange has {live_count} live orders (> {MAX_LIVE_ORDERS_PER_MARKET})"
        )

    ok = len(mismatch_reasons) == 0
    reason = ",".join(mismatch_reasons)
    risk_controller.mark_reconcile(ok=ok, reason=reason)
    if not ok:
        logger.warning("Order reconcile mismatch (%s): %s", source, reason)
    return ok, unknown_exchange_ids


async def reconcile_orders_with_exchange(
    client,
    market_id: Optional[int] = None,
    account_id: Optional[int] = None,
    source: str = "poller",
) -> bool:
    remote_orders = await _fetch_account_active_orders(
        client,
        market_id=market_id,
        account_id=account_id,
    )
    if remote_orders is None:
        risk_controller.mark_reconcile(ok=False, reason=f"{source}:fetch_failed")
        return False
    ok, unknown_ids = _reconcile_local_orders_with_remote_orders(remote_orders, source=source)

    # Auto-cancel orphaned orders that the bot doesn't track (batched)
    if unknown_ids:
        logger.warning("Cancelling %d orphaned orders (batch): %s", len(unknown_ids), sorted(unknown_ids))
        cancel_ops = []
        for oid in unknown_ids:
            cancel_ops.append(BatchOp(
                side="buy", level=0, action="cancel",
                price=0, size=0,
                order_id=oid, exchange_id=oid,  # these are already exchange order_index values
            ))
        if cancel_ops:
            if await _wait_for_write_slot(op_count=len(cancel_ops), cancel_only=True):
                await sign_and_send_batch(client, cancel_ops)

    return ok


async def _confirm_order_absent_on_exchange(client, order_id: int, timeout_sec: float) -> bool:
    """Best-effort confirmation that an order disappeared after cancel.

    Uses the account_orders WS feed for fast confirmation. Falls back to
    a single REST call on timeout (instead of polling every 0.2s).
    """
    if timeout_sec <= 0:
        return True

    # Fast path: wait for WS cancel event
    evt = asyncio.Event()
    _order_cancel_events[order_id] = evt
    try:
        await asyncio.wait_for(evt.wait(), timeout=timeout_sec)
        logger.debug("Cancel confirmed via WS for order %d", order_id)
        return True
    except asyncio.TimeoutError:
        pass
    finally:
        _order_cancel_events.pop(order_id, None)

    # Slow path: single REST fallback (1 call instead of up to 10)
    remote_orders = await _fetch_account_active_orders(client)
    if remote_orders is None:
        return False
    live_ids = _orders_to_live_client_id_set(remote_orders)
    _update_id_mapping_from_orders(remote_orders)
    if order_id not in live_ids:
        logger.debug("Cancel confirmed via REST fallback for order %d", order_id)
        return True
    return False


async def stale_order_reconciler_loop(client, market_id: int, account_id: int) -> None:
    """Background stale-order checker that keeps internal and exchange state synchronized.

    Uses a slow interval (60s) when the account_orders WS feed is healthy,
    falling back to the fast interval (3s) when WS auth is down.
    """
    fast_interval = max(STALE_ORDER_POLLER_INTERVAL_SEC, 0.5)
    while True:
        try:
            # Use slow interval when WS order feed is healthy
            if _account_orders_ws_connected.is_set() and _account_orders_ws_ready:
                interval = RECONCILER_SLOW_INTERVAL_SEC
            else:
                interval = fast_interval
            await asyncio.sleep(interval)
            logger.debug("stale_poller tick (interval=%.0fs, ws_healthy=%s)",
                         interval, _account_orders_ws_connected.is_set())
            ok = await reconcile_orders_with_exchange(
                client,
                market_id=market_id,
                account_id=account_id,
                source="stale_poller",
            )
            if not ok and risk_controller.mismatch_streak >= max(1, STALE_ORDER_DEBOUNCE_COUNT):
                risk_controller.trigger_pause(
                    f"order reconciliation mismatch for {risk_controller.mismatch_streak} polls"
                )
        except asyncio.CancelledError:
            raise
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            logger.error("Unexpected stale-order poller error: %s", exc, exc_info=True)


def on_order_book_update(market_id, payload):
    ob = state.market.local_order_book
    try:
        if market_id == state.config.market_id:
            bids_in = payload.get('bids', [])
            asks_in = payload.get('asks', [])

            is_snapshot = apply_orderbook_update(
                ob['bids'],
                ob['asks'],
                ob['initialized'],
                bids_in,
                asks_in,
            )
            if is_snapshot:
                ob['initialized'] = True
                logger.info("Initializing/snapshot local orderbook for market %d", market_id)
                # Note: vol_obi calculator is reset on WS *disconnect* (_on_disconnect),
                # NOT here. In-connection snapshots (server refreshes) should not discard
                # accumulated volatility/OBI data.

            # Calculate mid price using SortedDict O(1) peek
            state.market.ws_connection_healthy = True
            state.market.last_order_book_update = time.monotonic()

            if ob['bids'] and ob['asks']:
                best_bid = ob['bids'].peekitem(-1)[0]
                best_ask = ob['asks'].peekitem(0)[0]
                mid = (best_bid + best_ask) / 2
                state.market.mid_price = mid

                # Feed vol_obi calculator on every book update (hot path)
                calc = state.vol_obi_state.calculator
                if calc is not None:
                    # Inject Binance alpha if available and fresh
                    ba = state.binance_alpha
                    if ba is not None and ba.warmed_up and not ba.is_stale(BINANCE_STALE_SECONDS):
                        calc.set_alpha_override(ba.alpha)
                    else:
                        calc.set_alpha_override(None)
                    calc.on_book_update(mid, ob['bids'], ob['asks'])

                order_book_received.set()
            else:
                # Book is one-sided — clear stale mid_price to prevent
                # the trading loop from quoting at an outdated level.
                state.market.mid_price = None

    except (KeyError, IndexError, ValueError, TypeError, ZeroDivisionError) as e:
        logger.error(f"Error in order book callback: {e}", exc_info=True)
        state.market.ws_connection_healthy = False

def on_trade_update(market_id, trades):
    try:
        if market_id == state.config.market_id:
            for trade in trades:
                price = trade.get('price')
                size = trade.get('size')
                side = "SELL" if trade.get('is_maker_ask') else "BUY"
                logger.debug("Market Trade: %s %s @ %s", side, size, price)
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"❌ Error in trade callback: {e}", exc_info=True)


async def subscribe_to_market_data(market_id):
    """Connects to the websocket, subscribes to orderbook AND trades."""

    def _on_disconnect():
        state.market.ws_connection_healthy = False
        state.market.mid_price = None
        ob = state.market.local_order_book
        ob['initialized'] = False
        ob['bids'].clear()
        ob['asks'].clear()
        # Reset vol_obi calculator to avoid stale volatility data after reconnect
        calc = state.vol_obi_state.calculator
        if calc is not None:
            calc.reset()

    async def _on_connect():
        state.market.ws_connection_healthy = True

    def _on_message(data):
        msg_type = data.get("type", "")
        if msg_type in ("update/order_book", "subscribed/order_book"):
            if 'order_book' in data:
                on_order_book_update(market_id, data['order_book'])
        elif msg_type in ("update/trade", "subscribed/trade"):
            if 'trades' in data:
                on_trade_update(market_id, data['trades'])

    await ws_subscribe(
        channels=[f"order_book/{market_id}", f"trade/{market_id}"],
        label="market data",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        on_connect=_on_connect,
        on_disconnect=_on_disconnect,
        logger=logger,
        reconnect_event=ws_reconnect_event,
    )

def on_ticker_update(market_id, data):
    """Process ticker updates — provides real-time best bid/offer."""
    try:
        if market_id != state.config.market_id:
            return
        best_bid = data.get("best_bid")
        best_ask = data.get("best_ask")
        if best_bid is not None:
            state.market.ticker_best_bid = float(best_bid)
        if best_ask is not None:
            state.market.ticker_best_ask = float(best_ask)
        state.market.ticker_updated_at = time.monotonic()
    except (ValueError, TypeError, KeyError) as e:
        logger.error("Error in ticker callback: %s", e, exc_info=True)


async def subscribe_to_ticker(market_id):
    """Subscribe to ticker WS channel for real-time best bid/offer."""

    def _on_message(data):
        msg_type = data.get("type", "")
        if "ticker" in msg_type:
            on_ticker_update(market_id, data)

    await ws_subscribe(
        channels=[f"ticker/{market_id}"],
        label="ticker",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        logger=logger,
    )


def on_user_stats_update(account_id, stats_data):
    try:
        if account_id == ACCOUNT_INDEX:
            if not isinstance(stats_data, dict):
                logger.warning(f"Received user stats with unexpected payload: {stats_data}")
                return
            if "available_balance" not in stats_data or "portfolio_value" not in stats_data:
                logger.warning(f"Received user stats missing fields: {stats_data}")
                return

            new_available_capital = float(stats_data.get("available_balance"))
            new_portfolio_value = float(stats_data.get("portfolio_value"))

            if new_available_capital >= 0 and new_portfolio_value >= 0:
                state.account.available_capital = new_available_capital
                state.account.portfolio_value = new_portfolio_value
                state.account.last_capital_update = time.monotonic()
                logger.info(
                    f"Received user stats for account {account_id}: "
                    f"Available Capital=${state.account.available_capital}, Portfolio Value=${state.account.portfolio_value}"
                )
                account_state_received.set()
            else:
                logger.warning(
                    f"Received user stats with negative values: "
                    f"available_balance={stats_data.get('available_balance')}, "
                    f"portfolio_value={stats_data.get('portfolio_value')}"
                )
    except (ValueError, TypeError) as e:
        logger.error(f"Error processing user stats update: {e}", exc_info=True)

async def subscribe_to_user_stats(account_id):
    """Connects to the websocket, subscribes to user_stats, and updates global state."""

    def _on_message(data):
        msg_type = data.get("type")
        if msg_type in ("update/user_stats", "subscribed/user_stats"):
            on_user_stats_update(account_id, data.get("stats", {}))

    await ws_subscribe(
        channels=[f"user_stats/{account_id}"],
        label="user stats",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_ACCOUNT_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        logger=logger,
    )

def on_account_all_update(account_id, data):
    try:
        if account_id == ACCOUNT_INDEX:
            positions_updated = False
            if isinstance(data, dict) and "positions" in data:
                raw_positions = data.get("positions")
                if isinstance(raw_positions, dict):
                    new_positions = raw_positions
                    state.account.positions = new_positions
                    positions_updated = True

                    market_position = new_positions.get(str(state.config.market_id)) or new_positions.get(state.config.market_id)
                    new_size = 0.0
                    if market_position:
                        size = float(market_position.get("position", 0))
                        sign = int(market_position.get("sign", 1))
                        new_size = -size if sign == -1 else size
                    else:
                        new_size = 0.0

                    if new_size != state.account.position_size:
                        logger.info(
                            f"WebSocket position update for market {state.config.market_id}: "
                            f"{state.account.position_size} -> {new_size}"
                        )
                        state.account.position_size = new_size
                else:
                    logger.warning("Ignoring malformed account_all positions payload: %r", raw_positions)

            new_trades_by_market = data.get("trades", {}) if isinstance(data, dict) else {}
            if new_trades_by_market:
                all_new_trades = [trade for trades in new_trades_by_market.values() for trade in trades]
                all_new_trades.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                for trade in reversed(all_new_trades):
                    if trade not in state.account.recent_trades:
                        state.account.recent_trades.append(trade)
                        logger.info(
                            f"WebSocket trade update: Market {trade.get('market_id')}, "
                            f"Type {trade.get('type')}, Size {trade.get('size')}, "
                            f"Price {trade.get('price')}"
                        )

            if positions_updated and not account_all_received.is_set():
                account_all_received.set()

    except (ValueError, TypeError) as e:
        logger.error(f"Error processing account_all update: {e}", exc_info=True)

async def subscribe_to_account_all(account_id):
    """Connects to the websocket, subscribes to account_all, and updates global state."""

    def _on_message(data):
        msg_type = data.get("type")
        if msg_type in ("update/account_all", "update/account", "subscribed/account_all"):
            on_account_all_update(account_id, data)

    await ws_subscribe(
        channels=[f"account_all/{account_id}"],
        label="account data",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_ACCOUNT_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        logger=logger,
    )

def _generate_ws_auth_token(client) -> str | None:
    """Generate a short-lived WS auth token via SignerClient; returns None on any failure."""
    try:
        auth, err = client.create_auth_token_with_expiry()
        if err or not auth:
            logger.warning(f"WS auth token generation failed: {err}")
            return None
        return auth
    except Exception as e:
        logger.warning(f"WS auth token generation exception: {e}")
        return None


def on_account_orders_update(account_id: int, market_id: int, data: dict) -> None:
    """Process account order updates from authenticated WS.

    The first message after subscription is a **snapshot** (all orders).
    Subsequent messages are **incremental** (only the changed order(s)).
    For snapshots we do full reconciliation; for incremental updates we only
    clear orders that are explicitly marked as dead (filled/cancelled).
    """
    global _account_orders_ws_ready
    try:
        orders_by_market = data.get("orders", {})
        raw = orders_by_market.get(str(market_id)) or orders_by_market.get(market_id, [])

        LIVE = {"open", "partial_filled", "pending"}

        # Always update the client_order_index -> order_index mapping
        _update_id_mapping_from_orders(raw)

        if not _account_orders_ws_ready:
            # --- SNAPSHOT (first message after subscribe) ---
            _account_orders_ws_ready = True
            _account_orders_ws_connected.set()
            live_orders = [o for o in raw if o.get("status", "open") in LIVE]
            active_client_ids: set[int] = set()
            for o in live_orders:
                cid = o.get("client_order_index")
                if cid is not None:
                    try:
                        active_client_ids.add(int(cid))
                    except (TypeError, ValueError):
                        pass
            logger.info("account_orders WS ready (snapshot) — %d live orders for market %d",
                        len(active_client_ids), market_id)

            # Authoritative: clear any local orders not in the snapshot
            orders = state.orders
            for lvl in range(NUM_LEVELS):
                bid_id = orders.bid_order_ids[lvl]
                if bid_id is not None and bid_id not in active_client_ids:
                    logger.info("Bid[%d] %d not in WS snapshot — clearing", lvl, bid_id)
                    order_manager.clear_live("buy", lvl)
                ask_id = orders.ask_order_ids[lvl]
                if ask_id is not None and ask_id not in active_client_ids:
                    logger.info("Ask[%d] %d not in WS snapshot — clearing", lvl, ask_id)
                    order_manager.clear_live("sell", lvl)

            _refresh_local_orders_from_remote_orders(live_orders)
            risk_controller.mark_reconcile(ok=True, reason="account_orders_ws_snapshot")
            return

        # --- INCREMENTAL UPDATE ---
        # Only clear orders that are explicitly confirmed dead.
        # Do NOT assume absence means cancellation (the message only
        # contains the changed order, not all orders).
        orders = state.orders
        live_orders = []
        for o in raw:
            cid_raw = o.get("client_order_index")
            if cid_raw is None:
                continue
            try:
                cid = int(cid_raw)
            except (TypeError, ValueError):
                continue
            status = o.get("status", "open")
            if status not in LIVE:
                # Order is dead (filled / cancelled / expired)
                for lvl in range(NUM_LEVELS):
                    if orders.bid_order_ids[lvl] == cid:
                        logger.info("Bid[%d] %d status=%s — clearing", lvl, cid, status)
                        order_manager.clear_live("buy", lvl)
                    if orders.ask_order_ids[lvl] == cid:
                        logger.info("Ask[%d] %d status=%s — clearing", lvl, cid, status)
                        order_manager.clear_live("sell", lvl)
                # Signal any pending cancel confirmation waiters
                evt = _order_cancel_events.pop(cid, None)
                if evt is not None:
                    evt.set()
                # Also check by exchange_id
                eid_raw = o.get("order_index")
                if eid_raw is not None:
                    try:
                        eid = int(eid_raw)
                        evt2 = _order_cancel_events.pop(eid, None)
                        if evt2 is not None:
                            evt2.set()
                    except (TypeError, ValueError):
                        pass
            else:
                live_orders.append(o)

        if live_orders:
            _refresh_local_orders_from_remote_orders(live_orders)

        # Account-orders feed is authoritative for "what is still alive".
        risk_controller.mark_reconcile(ok=True, reason="account_orders_ws")

    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"on_account_orders_update error: {e}", exc_info=True)


async def subscribe_to_account_orders(client, market_id: int, account_id: int) -> None:
    """Authenticated WS subscription for real-time order fill/cancel events.

    Falls back gracefully (returns) if auth is unavailable — no impact on the core bot.
    Regenerates the auth token every 8 min before the 10-min server expiry.
    """
    global _account_orders_ws_ready
    channel = f"account_orders/{market_id}/{account_id}"

    def _on_message(data):
        msg_type = data.get("type", "")
        if "account_orders" in msg_type:
            on_account_orders_update(account_id, market_id, data)

    while True:
        auth_token = _generate_ws_auth_token(client)
        if not auth_token:
            logger.warning("account_orders: auth token unavailable — authenticated order feed disabled")
            return  # graceful exit; core public channels unaffected

        _account_orders_ws_ready = False   # reset on each reconnect/token refresh
        _account_orders_ws_connected.clear()

        inner = asyncio.create_task(
            ws_subscribe(
                channels=[channel],
                label="account orders",
                on_message=_on_message,
                url=WEBSOCKET_URL,
                ping_interval=WS_PING_INTERVAL,
                recv_timeout=WS_ACCOUNT_RECV_TIMEOUT,
                reconnect_base=WS_RECONNECT_BASE_DELAY,
                reconnect_max=WS_RECONNECT_MAX_DELAY,
                channel_auths={channel: auth_token},
                logger=logger,
            )
        )
        try:
            await asyncio.wait_for(asyncio.shield(inner), timeout=_WS_AUTH_TOKEN_TTL)
        except asyncio.TimeoutError:
            inner.cancel()
            try:
                await inner
            except asyncio.CancelledError:
                pass
            logger.info("account_orders: refreshing auth token (8-min TTL)")
            # Loop → regenerate token
        except asyncio.CancelledError:
            inner.cancel()
            try:
                await inner
            except asyncio.CancelledError:
                pass
            raise


async def restart_websocket():
    global ws_task
    logger.info("🔄 Restarting websocket connection...")
    await _cancel_task_with_timeout(ws_task, label="market_data_ws_restart")
    ws_task = None

    # Reset market data state (order state is managed by account_orders WS)
    ob = state.market.local_order_book
    order_book_received.clear()
    state.market.mid_price = None
    ob['initialized'] = False
    ob['bids'].clear()
    ob['asks'].clear()

    # Start new task
    ws_task = asyncio.create_task(subscribe_to_market_data(state.config.market_id))

    try:
        logger.info("⏳ Waiting for websocket reconnection...")
        await asyncio.wait_for(order_book_received.wait(), timeout=15.0)
        logger.info("✅ Websocket reconnected successfully")
        return True
    except asyncio.TimeoutError:
        logger.error("❌ Websocket reconnection failed - timeout.")
        await _cancel_task_with_timeout(ws_task, label="market_data_ws_restart_timeout")
        ws_task = None
        return False

def check_websocket_health():
    if not state.market.ws_connection_healthy:
        return False
    if time.monotonic() - state.market.last_order_book_update > WS_RECV_TIMEOUT:
        return False
    if state.market.mid_price is None:
        return False
    return True


_CAPITAL_STALE_SECONDS = 3600.0  # user_stats WS is event-driven; updates only on balance changes


def calculate_dynamic_base_amount(mid_price, capital=None):
    if mid_price is None or mid_price <= 0:
        return None

    effective_capital = capital if capital is not None else state.account.available_capital
    if effective_capital is None:
        return BASE_AMOUNT

    # Fall back to static size if capital data is stale
    if state.account.last_capital_update > 0:
        age = time.monotonic() - state.account.last_capital_update
        if age > _CAPITAL_STALE_SECONDS:
            logger.warning(
                "Capital data is %.0fs stale (threshold: %.0fs); using fallback BASE_AMOUNT=%.6f",
                age, _CAPITAL_STALE_SECONDS, BASE_AMOUNT,
            )
            return BASE_AMOUNT

    try:
        # Cache key: capital rounded to nearest dollar, mid to 4 decimals
        rounded_capital = round(effective_capital)
        rounded_mid = round(mid_price, 4)
        cache_key = (rounded_capital, rounded_mid)

        if cache_key == state.account._cached_base_amount_inputs and state.account._cached_base_amount is not None:
            return state.account._cached_base_amount

        usd_amount = effective_capital * CAPITAL_USAGE_PERCENT * LEVERAGE
        size = usd_amount / mid_price

        if state.config.amount_tick_float > 0:
             size = round(size / state.config.amount_tick_float) * state.config.amount_tick_float

        # Enforce exchange minimums
        min_base = state.config.min_base_amount
        min_quote = state.config.min_quote_amount
        if min_base > 0 and size < min_base:
            size = min_base
        if min_quote > 0 and size * mid_price < min_quote:
            size = min_quote / mid_price
            if state.config.amount_tick_float > 0:
                size = math.ceil(size / state.config.amount_tick_float) * state.config.amount_tick_float

        # Enforce minimum order value for quota generation (each $7 volume = +1 quota point)
        if MIN_ORDER_VALUE_USD > 0 and size * mid_price < MIN_ORDER_VALUE_USD:
            size = MIN_ORDER_VALUE_USD / mid_price
            if state.config.amount_tick_float > 0:
                size = math.ceil(size / state.config.amount_tick_float) * state.config.amount_tick_float

        state.account._cached_base_amount = size
        state.account._cached_base_amount_inputs = cache_key
        return size
    except Exception as exc:
        logger.warning("calculate_dynamic_base_amount failed (%s); using fallback BASE_AMOUNT=%.6f", exc, BASE_AMOUNT)
        return BASE_AMOUNT

# ---------------------------------------------------------------------------
# Adaptive rate-limiter helpers
# ---------------------------------------------------------------------------

def _prune_op_window() -> int:
    """Evict ops older than the rolling window; return current op count."""
    cutoff = time.monotonic() - _RL_WINDOW_SECONDS
    while _op_timestamps and _op_timestamps[0] < cutoff:
        _op_timestamps.popleft()
    return len(_op_timestamps)


def _ops_available() -> int:
    """How many ops can we send right now within the 40/60s window."""
    return max(0, _RL_OPS_PER_WINDOW - _prune_op_window())


def _time_until_ops_free(n: int) -> float:
    """Seconds until *n* ops become available (0.0 if already available)."""
    _prune_op_window()
    if len(_op_timestamps) + n <= _RL_OPS_PER_WINDOW:
        return 0.0
    # Need to wait for the (len - (budget - n))-th oldest op to expire
    idx = len(_op_timestamps) - (_RL_OPS_PER_WINDOW - n)
    if idx < 0:
        return 0.0
    if idx >= len(_op_timestamps):
        idx = len(_op_timestamps) - 1
    expires_at = _op_timestamps[idx] + _RL_WINDOW_SECONDS
    return max(0.0, expires_at - time.monotonic())


def _quota_pace_multiplier() -> float:
    """Return a pacing multiplier based on volume_quota_remaining.
    1.0 = full speed, higher = slower, inf = wait for free slot."""
    if _volume_quota_remaining is None or _volume_quota_remaining >= _RL_QUOTA_HIGH:
        return 1.0
    if _volume_quota_remaining >= _RL_QUOTA_MEDIUM:
        return 1.5
    if _volume_quota_remaining >= _RL_QUOTA_LOW:
        return 3.0
    return float('inf')  # wait for free 15s slot


def _adaptive_threshold_bps() -> float:
    """Return quote update threshold (bps) scaled by quota pressure."""
    base = QUOTE_UPDATE_THRESHOLD_BPS
    if _volume_quota_remaining is None or _volume_quota_remaining >= _RL_QUOTA_HIGH:
        return base          # >= 500: normal
    if _volume_quota_remaining >= _RL_QUOTA_MEDIUM:
        return base * 2.0    # 50-499: 2x
    if _volume_quota_remaining >= _RL_QUOTA_LOW:
        return base * 3.5    # 10-49: 3.5x
    return base * 5.0        # 0-9: 5x


def _record_ops_sent(count: int) -> None:
    """Record *count* operations sent at the current time."""
    global _last_send_time
    now = time.monotonic()
    _op_timestamps.extend([now] * count)
    _last_send_time = now


def _update_volume_quota(raw) -> None:
    """Parse volume_quota_remaining from an exchange response."""
    global _volume_quota_remaining, _quota_warning_level
    if raw is None or raw == "?":
        return
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return
    _volume_quota_remaining = val

    if val <= 0 and _quota_warning_level != "critical":
        logger.warning("QUOTA EXHAUSTED (0 remaining) — free-slot pacing only (1 tx / 15s)")
        _quota_warning_level = "critical"
    elif 0 < val < _RL_QUOTA_LOW and _quota_warning_level not in ("critical", "low"):
        logger.warning("QUOTA LOW: %d remaining (< %d) — 3x pacing", val, _RL_QUOTA_LOW)
        _quota_warning_level = "low"
    elif _RL_QUOTA_LOW <= val < _RL_QUOTA_MEDIUM and _quota_warning_level not in ("critical", "low", "medium"):
        logger.warning("QUOTA MEDIUM: %d remaining (< %d) — 1.5x pacing", val, _RL_QUOTA_MEDIUM)
        _quota_warning_level = "medium"
    elif val >= _RL_QUOTA_MEDIUM and _quota_warning_level != "ok":
        logger.info("QUOTA RECOVERED: %d remaining — full speed", val)
        _quota_warning_level = "ok"


async def _wait_for_write_slot(op_count: int = 4, cancel_only: bool = False) -> bool:
    """Adaptive rate-limit gate. Returns True if OK to proceed, False to skip.

    Phases:
    1. Global 429 backoff (if within 2s of expiry, sleep instead of skip)
    2. Sliding-window capacity (40 ops / 60s)
    3. Minimum send-interval floor (1.5s normal, 0.5s cancel-only)
    4. Volume-quota pacing (skip for cancel-only)
    """
    global _last_send_time
    now = time.monotonic()

    # Phase 1: Global 429 backoff
    if now < _global_backoff_until:
        remaining = _global_backoff_until - now
        if remaining <= 2.0:
            # Close to expiry — wait it out
            await asyncio.sleep(remaining)
        else:
            logger.warning("RATE LIMIT: global backoff active (%.0fs remaining) — skipping cycle", remaining)
            return False

    # Phase 2: Sliding window capacity
    avail = _ops_available()
    if avail < op_count:
        wait_time = _time_until_ops_free(op_count)
        if wait_time > 30.0:
            logger.warning("RATE LIMIT: window full (%d/%d ops), need %.1fs — skipping cycle",
                           _prune_op_window(), _RL_OPS_PER_WINDOW, wait_time)
            return False
        if wait_time > 0:
            logger.warning("RATE LIMIT: window capacity low, waiting %.1fs for %d ops", wait_time, op_count)
            await asyncio.sleep(wait_time)

    # Phase 3: Minimum send-interval floor
    floor = _RL_CANCEL_MIN_INTERVAL if cancel_only else _RL_MIN_SEND_INTERVAL
    elapsed = time.monotonic() - _last_send_time
    if elapsed < floor:
        await asyncio.sleep(floor - elapsed)

    # Phase 4: Volume-quota pacing (skip for cancel-only batches)
    if not cancel_only:
        mult = _quota_pace_multiplier()
        if mult == float('inf'):
            # Quota critically low — wait for the free 15s slot
            since_last = time.monotonic() - _last_send_time
            if since_last < _RL_FREE_SLOT_INTERVAL:
                wait_free = _RL_FREE_SLOT_INTERVAL - since_last
                logger.warning("QUOTA: low (%d remaining), waiting %.1fs for free slot",
                               _volume_quota_remaining, wait_free)
                await asyncio.sleep(wait_free)
        elif mult > 1.0:
            # Slow down by stretching the interval
            extra = floor * (mult - 1.0)
            elapsed2 = time.monotonic() - _last_send_time
            if elapsed2 < floor + extra:
                await asyncio.sleep(floor + extra - elapsed2)

    return True


def _trigger_global_backoff():
    """Set a global cooldown after hitting 429. Escalates with consecutive failures.
    Reduced: 15s -> 30s -> 60s -> 120s max."""
    global _global_backoff_until, _global_backoff_consecutive, _consecutive_successes
    _global_backoff_consecutive += 1
    _consecutive_successes = 0
    duration = min(_RL_BACKOFF_BASE * (2 ** (_global_backoff_consecutive - 1)), _RL_BACKOFF_MAX)
    _global_backoff_until = time.monotonic() + duration
    logger.warning("429 rate limit — global backoff for %.0fs (attempt #%d)",
                   duration, _global_backoff_consecutive)


def _reset_global_backoff():
    """After a successful write, require N consecutive successes before
    resetting escalation counter."""
    global _global_backoff_consecutive, _consecutive_successes
    _consecutive_successes += 1
    if _global_backoff_consecutive > 0:
        if _consecutive_successes >= _RL_BACKOFF_RESET_AFTER:
            logger.info("SDK write succeeded — resetting backoff after %d consecutive successes "
                        "(was %d)", _consecutive_successes, _global_backoff_consecutive)
            _global_backoff_consecutive = 0
            _consecutive_successes = 0
        else:
            logger.debug("SDK write succeeded (%d/%d for backoff reset)",
                         _consecutive_successes, _RL_BACKOFF_RESET_AFTER)

def _is_quota_error(exc_or_msg) -> bool:
    """Return True if the error is specifically a volume-quota exhaustion."""
    msg = str(exc_or_msg).lower()
    return ("not enough" in msg and "quota" in msg) or ("quota" in msg and "exhausted" in msg)


async def cancel_all_orders(client, _retries_left: int = 3):
    global _volume_quota_remaining
    try:
        async with _sdk_write_lock:
            try:
                tx, response, err = await client.cancel_all_orders(
                    time_in_force=lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
                    timestamp_ms=0,
                )
            except TypeError:
                tx, response, err = await client.cancel_all_orders(
                    time_in_force=lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
                    time=0,
                )
        if err:
            err_str = str(err)
            if _is_quota_error(err_str):
                _volume_quota_remaining = 0
                client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
                if _retries_left > 0:
                    logger.warning("cancel_all_orders: quota exhausted — waiting 16s for free slot then retrying (%d left)", _retries_left)
                    await asyncio.sleep(16)
                    return await cancel_all_orders(client, _retries_left - 1)
                logger.warning("cancel_all_orders: quota exhausted — no retries left, skipping")
                return
            if _is_transient_error(Exception(err_str)) and _retries_left > 0:
                logger.warning(f"cancel_all_orders hit rate limit (will retry, {_retries_left} left): {err}")
                await asyncio.sleep(_RL_BACKOFF_BASE)
                return await cancel_all_orders(client, _retries_left - 1)
            logger.error(f"❌ Failed to cancel all orders: {err}")
        else:
            logger.info("🗑️ Cancelled all orders")
            if response is not None:
                _update_volume_quota(getattr(response, 'volume_quota_remaining', None))
    except Exception as e:
        if _is_quota_error(e):
            _volume_quota_remaining = 0
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
            if _retries_left > 0:
                logger.warning("cancel_all_orders: quota exhausted — waiting 16s for free slot then retrying (%d left)", _retries_left)
                await asyncio.sleep(16)
                return await cancel_all_orders(client, _retries_left - 1)
            logger.warning("cancel_all_orders: quota exhausted — no retries left, skipping")
            return
        if _is_transient_error(e) and _retries_left > 0:
            logger.warning(f"cancel_all_orders hit rate limit (will retry, {_retries_left} left): {e}")
            await asyncio.sleep(_RL_BACKOFF_BASE)
            return await cancel_all_orders(client, _retries_left - 1)
        logger.error(f"❌ Failed to cancel orders: {e}", exc_info=True)


async def _attempt_quota_recovery(client) -> bool:
    """Send small IOC market orders to generate volume and recover quota.

    Returns True if quota reached _QR_TARGET, False otherwise.
    Safeguards: max attempts, max cumulative loss, PnL monitoring, verify quota increases.
    """
    global _quota_recovery_in_progress, _quota_recovery_last_attempt

    # Guard: cooldown
    if time.monotonic() - _quota_recovery_last_attempt < _QR_COOLDOWN:
        return False
    if _quota_recovery_in_progress:
        return False
    if _volume_quota_remaining is None:
        return False  # can't recover if we don't know current quota

    _quota_recovery_in_progress = True
    _quota_recovery_last_attempt = time.monotonic()
    cumulative_loss = 0.0

    # Snapshot portfolio value before recovery to detect PnL drop
    pv_before = state.account.portfolio_value

    try:
        logger.warning("QUOTA RECOVERY: starting (quota=%d, target=%d)",
                       _volume_quota_remaining, _QR_TARGET)

        # Refresh nonce to clear any corruption from prior 429s
        client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)

        for attempt in range(1, _QR_MAX_ATTEMPTS + 1):
            # PnL safety check: abort if portfolio value dropped significantly
            pv_now = state.account.portfolio_value
            if pv_before is not None and pv_now is not None and pv_before > 0:
                pnl_drop = pv_before - pv_now
                if pnl_drop >= _QR_MAX_LOSS:
                    logger.warning(
                        "QUOTA RECOVERY: PnL drop $%.2f >= max_loss $%.2f, aborting",
                        pnl_drop, _QR_MAX_LOSS)
                    return _volume_quota_remaining >= _QR_TRIGGER

            # Wait for free 15s slot (use 16s for safety margin)
            since_last = time.monotonic() - _last_send_time
            free_wait = _RL_FREE_SLOT_INTERVAL + 1.0  # 16s for safety
            if since_last < free_wait:
                wait = free_wait - since_last
                logger.info("QUOTA RECOVERY: waiting %.1fs for free slot", wait)
                await asyncio.sleep(wait)

            # Determine order direction: reduce position if any, else buy
            pos = state.account.position_size
            is_ask = pos > EPSILON  # sell if long, buy if short/flat

            # Use minimum order size
            min_base = state.config.min_base_amount
            tick = state.config.amount_tick_float
            size = max(min_base, tick) if min_base > 0 else tick
            if size <= 0:
                logger.warning("QUOTA RECOVERY: cannot determine order size, aborting")
                return False

            # Get current prices
            best_bid, best_ask = get_best_prices()
            mid = state.market.mid_price
            if not best_bid or not best_ask or not mid or mid <= 0:
                logger.warning("QUOTA RECOVERY: no orderbook data, aborting")
                return False

            # IOC market order: use best price (crosses spread for immediate fill)
            if is_ask:
                price = best_bid  # sell at best bid
            else:
                price = best_ask  # buy at best ask

            raw_price = _to_raw_price(price)
            raw_size = _to_raw_amount(size)
            order_id = next_client_order_index()

            logger.info("QUOTA RECOVERY: attempt %d/%d — %s %.6f @ %.2f",
                        attempt, _QR_MAX_ATTEMPTS,
                        "SELL" if is_ask else "BUY", size, price)

            old_quota = _volume_quota_remaining
            try:
                async with _sdk_write_lock:
                    _record_ops_sent(1)
                    tx, response, err = await client.create_market_order(
                        market_index=state.config.market_id,
                        client_order_index=order_id,
                        base_amount=raw_size,
                        avg_execution_price=raw_price,
                        is_ask=is_ask,
                    )

                if err:
                    logger.warning("QUOTA RECOVERY: order error: %s", err)
                    return False

                # Extract quota from response
                if response is not None:
                    _update_volume_quota(getattr(response, 'volume_quota_remaining', None))

                logger.info("QUOTA RECOVERY: attempt %d — quota %d -> %d",
                            attempt, old_quota, _volume_quota_remaining)

                # Safety: verify quota actually increased
                if _volume_quota_remaining <= old_quota:
                    logger.warning("QUOTA RECOVERY: quota did not increase (%d -> %d), stopping",
                                   old_quota, _volume_quota_remaining)
                    return False

                # Safety: track estimated cost (spread crossing + fees)
                spread_cost = abs(best_ask - best_bid) * size * 0.5
                fee_estimate = size * mid * 0.001  # ~10bps estimate
                cumulative_loss += spread_cost + fee_estimate
                if cumulative_loss >= _QR_MAX_LOSS:
                    logger.warning("QUOTA RECOVERY: loss limit reached ($%.2f >= $%.2f), stopping",
                                   cumulative_loss, _QR_MAX_LOSS)
                    return _volume_quota_remaining >= _QR_TRIGGER  # partial success

                # Check if target reached
                if _volume_quota_remaining >= _QR_TARGET:
                    logger.info("QUOTA RECOVERY: target reached (quota=%d), resuming",
                                _volume_quota_remaining)
                    return True

            except Exception as e:
                logger.warning("QUOTA RECOVERY: exception: %s", e, exc_info=True)
                return False

        logger.warning("QUOTA RECOVERY: max attempts reached (quota=%d)", _volume_quota_remaining)
        return _volume_quota_remaining >= _QR_TRIGGER
    finally:
        _quota_recovery_in_progress = False


# === TX WebSocket ===

class TxWebSocket:
    """Persistent WebSocket connection for sending transactions via WS.

    Uses ``jsonapi/sendtxbatch`` messages. Auto-reconnects on disconnect.
    WS sendTx/sendBatchTx are excluded from the 200 msg/min client limit.
    """

    def __init__(self, url: str):
        self._url = url
        self._ws = None
        self._lock = asyncio.Lock()
        self._connected = False
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._recv_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Establish (or re-establish) the WS connection."""
        await self._stop_recv_loop()
        try:
            self._ws = await websockets.connect(
                self._url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_INTERVAL,
            )
            # Consume the initial "connected" message so recv_loop doesn't queue it
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                init_msg = json.loads(raw)
                logger.info("TxWebSocket connected to %s (init: %s)", self._url, init_msg.get("type", "?"))
            except asyncio.TimeoutError:
                logger.info("TxWebSocket connected to %s (no init message)", self._url)
            self._connected = True
            # Drain stale messages from previous connection
            while not self._recv_queue.empty():
                try:
                    self._recv_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            # Start background recv loop
            self._recv_task = asyncio.create_task(self._recv_loop())
        except Exception as e:
            self._connected = False
            logger.warning("TxWebSocket connect failed: %s", e)

    async def close(self) -> None:
        """Close the WS connection."""
        self._connected = False
        await self._stop_recv_loop()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    @property
    def is_connected(self) -> bool:
        if not self._connected or self._ws is None:
            return False
        try:
            return self._ws.state.name == "OPEN"
        except Exception:
            return False

    async def _recv_loop(self) -> None:
        """Background task: read from WS, respond to app-level pings, queue responses."""
        _info_types = {"connected", "subscribed"}
        try:
            while True:
                raw = await self._ws.recv()
                data = json.loads(raw)
                msg_type = data.get("type", "")
                if msg_type == "ping":
                    try:
                        await self._ws.send('{"type":"pong"}')
                    except Exception:
                        break
                elif msg_type in _info_types:
                    pass  # drop informational messages
                else:
                    await self._recv_queue.put(data)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("TxWebSocket _recv_loop exited: %s", e)
        # Connection lost — signal any waiting send_batch()
        self._connected = False
        await self._recv_queue.put(None)

    async def _stop_recv_loop(self) -> None:
        """Cancel the background recv task if running."""
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass
            self._recv_task = None

    async def send_batch(self, tx_types: list, tx_infos: list) -> Optional[dict]:
        """Send a transaction batch via WS and return the parsed response.

        Returns None if WS is down (caller should fall back to REST).
        """
        async with self._lock:
            if not self.is_connected:
                try:
                    await self.connect()
                except Exception:
                    return None
                if not self.is_connected:
                    return None

            # SDK sends tx_types/tx_infos as JSON-encoded strings
            _tx_types_str = json.dumps(tx_types)
            _tx_infos_str = json.dumps(tx_infos)
            if isinstance(_tx_types_str, bytes):
                _tx_types_str = _tx_types_str.decode()
            if isinstance(_tx_infos_str, bytes):
                _tx_infos_str = _tx_infos_str.decode()
            msg_str = json.dumps({
                "type": "jsonapi/sendtxbatch",
                "data": {
                    "tx_types": _tx_types_str,
                    "tx_infos": _tx_infos_str,
                },
            })
            if isinstance(msg_str, bytes):
                msg_str = msg_str.decode()

            async def _send_and_recv() -> Optional[dict]:
                """Send batch message and read response from recv queue."""
                await self._ws.send(msg_str)
                resp = await asyncio.wait_for(self._recv_queue.get(), timeout=10.0)
                if resp is None:
                    # Sentinel — connection lost
                    return None
                return resp

            try:
                resp = await _send_and_recv()
                if resp is not None:
                    return resp
                # Connection was lost (sentinel) — reconnect and retry once
                logger.warning("TxWebSocket connection lost; reconnecting and retrying once")
                await self.connect()
                if not self.is_connected:
                    return None
                return await _send_and_recv()
            except Exception as e:
                logger.warning("TxWebSocket send failed (%s); reconnecting and retrying once", e)
                self._connected = False
                try:
                    await self.connect()
                    if not self.is_connected:
                        return None
                    return await _send_and_recv()
                except Exception as e2:
                    logger.warning("TxWebSocket retry also failed (%s); falling back to REST", e2)
                    self._connected = False
                    return None


# Module-level TxWebSocket instance (initialized in main())
_tx_ws: Optional[TxWebSocket] = None

# === HOT PATH ===

def collect_order_operations(level_prices, base_amount, _log_debug=False):
    """Decide what order ops are needed for all levels. Pure logic, no network.

    Returns a list of BatchOp describing creates/modifies needed this iteration.
    """
    ops = []
    orders = state.orders
    effective_threshold = _adaptive_threshold_bps()
    for level, (buy_price, sell_price) in enumerate(level_prices):
        for is_buy, new_price in [(True, buy_price), (False, sell_price)]:
            side = "buy" if is_buy else "sell"
            new_size = base_amount

            if new_price is None or new_size is None:
                continue
            if new_price <= 0 or new_size <= 0:
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
                if not _has_exchange_id(existing_id):
                    order_manager.mark_status(
                        side, SideStatus.LIVE, level=level,
                        target_price=new_price, target_size=new_size,
                    )
                    if _log_debug:
                        logger.debug(
                            "Keeping %s[%d]: awaiting exchange order_index for client id %d",
                            side, level, existing_id,
                        )
                    continue
                change_bps = price_change_bps(existing_price, new_price)
                size_changed = _size_change_requires_update(existing_size, new_size)
                needs_modify = existing_price is None or change_bps > effective_threshold or size_changed
                if not needs_modify:
                    order_manager.mark_status(
                        side, SideStatus.LIVE, level=level,
                        target_price=new_price, target_size=new_size,
                    )
                    if _log_debug:
                        logger.debug(
                            "Keeping %s[%d]: price %.2f bps <= %.2f and size unchanged",
                            side, level, change_bps, effective_threshold,
                        )
                    continue
                exchange_id = _resolve_exchange_id(existing_id)
                if exchange_id is None:
                    if _log_debug:
                        logger.debug(
                            "Keeping %s[%d]: exchange order_index missing for client id %d",
                            side, level, existing_id,
                        )
                    continue
                ops.append(BatchOp(
                    side=side, level=level, action="modify",
                    price=new_price, size=new_size,
                    order_id=existing_id, exchange_id=exchange_id,
                ))
            else:
                # No existing order — create new one
                new_order_id = next_client_order_index()
                ops.append(BatchOp(
                    side=side, level=level, action="create",
                    price=new_price, size=new_size,
                    order_id=new_order_id, exchange_id=0,
                ))
    return ops


async def _send_single_op_rest(client, tx_type: int, tx_info, op) -> bool:
    """Send a single already-signed op via REST sendTx (qualifies for free 15s slot).

    Returns True on success, False on error.
    """
    try:
        resp = await client.send_tx(tx_type=tx_type, tx_info=tx_info)
        quota_remaining = getattr(resp, 'volume_quota_remaining', None)
        _update_volume_quota(quota_remaining)
        logger.info(
            "REST single-tx OK: %s %s[%d] (quota remaining: %s)",
            op.action, op.side, op.level, quota_remaining,
        )
        _record_order_success()
        if op.action != "cancel":
            order_manager.bind_live(op.side, op.order_id, op.price, op.size, level=op.level)
        return True
    except Exception as e:
        body = getattr(e, 'body', None) or getattr(e, 'reason', '')
        logger.warning("REST single-tx error for %s %s[%d]: %s | body=%s",
                       op.action, op.side, op.level, e, body)
        if _is_quota_error(e):
            _update_volume_quota(0)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
        elif _is_transient_error(e):
            _trigger_global_backoff()
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
        return False


async def sign_and_send_batch(client, ops: list):
    """Sign all BatchOps locally, then send as a single send_tx_batch call."""
    if not ops:
        return

    # Free-slot mode: when quota is 0, send only 1 op via REST sendTx
    free_slot_mode = _volume_quota_remaining is not None and _volume_quota_remaining <= 0
    if free_slot_mode:
        ops = ops[:1]  # only first (highest priority) op
        logger.info("Free-slot mode (quota=0): sending 1 op via REST sendTx")

    # Safety: drop create ops whose slot got filled since collect_order_operations
    # ran (e.g. by WS account_orders update or reconciler rebind).  This avoids
    # orphaning the order that now occupies the slot.
    orders = state.orders
    safe_ops = []
    for op in ops:
        if op.action == "create":
            slot_id = (orders.bid_order_ids[op.level] if op.side == "buy"
                       else orders.ask_order_ids[op.level])
            if slot_id is not None:
                logger.warning(
                    "Dropping stale create %s[%d]: slot already has order %d",
                    op.side, op.level, slot_id,
                )
                continue
        safe_ops.append(op)
    ops = safe_ops
    if not ops:
        return

    if logger.isEnabledFor(logging.DEBUG):
        ops_desc = ", ".join(f"{o.action} {o.side}[{o.level}] @{o.price}" for o in ops)
        logger.debug("Batch preparing %d ops: %s", len(ops), ops_desc)

    market_index = state.config.market_id
    tx_types = []
    tx_infos = []
    signed_ops = []
    signed_nonces = []  # (api_key_index,) per signed op — for rollback

    for op in ops:
        api_key_index, nonce = client.nonce_manager.next_nonce()

        if op.action == "create":
            is_ask = (op.side == "sell")
            raw_price = _to_raw_price(op.price)
            raw_size = _to_raw_amount(op.size)
            tx_type, tx_info, _tx_hash, err = client.sign_create_order(
                market_index=market_index,
                client_order_index=op.order_id,
                base_amount=raw_size,
                price=raw_price,
                is_ask=is_ask,
                order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                nonce=nonce,
                api_key_index=api_key_index,
            )
        elif op.action == "modify":
            raw_price = _to_raw_price(op.price)
            raw_size = _to_raw_amount(op.size)
            tx_type, tx_info, _tx_hash, err = client.sign_modify_order(
                market_index=market_index,
                order_index=op.exchange_id,
                base_amount=raw_size,
                price=raw_price,
                nonce=nonce,
                api_key_index=api_key_index,
            )
        elif op.action == "cancel":
            tx_type, tx_info, _tx_hash, err = client.sign_cancel_order(
                market_index=market_index,
                order_index=op.exchange_id,
                nonce=nonce,
                api_key_index=api_key_index,
            )
        else:
            logger.error("Unknown batch op action: %s", op.action)
            client.nonce_manager.acknowledge_failure(api_key_index)
            continue

        if err:
            logger.warning("Batch sign error for %s[%d] %s: %s", op.side, op.level, op.action, err)
            client.nonce_manager.acknowledge_failure(api_key_index)
            continue

        tx_types.append(int(tx_type))
        tx_infos.append(tx_info)
        signed_ops.append(op)
        signed_nonces.append(api_key_index)

    if not tx_types:
        logger.warning("All batch ops failed signing; nothing to send")
        return

    # Mark ops as in-progress
    for op in signed_ops:
        if op.action == "create":
            order_manager.mark_status(
                op.side, SideStatus.PLACING, level=op.level,
                pending_order_id=op.order_id,
                target_price=op.price, target_size=op.size,
            )
        elif op.action == "modify":
            order_manager.mark_status(
                op.side, SideStatus.MODIFYING, level=op.level,
                pending_order_id=op.order_id,
                target_price=op.price, target_size=op.size,
            )

    # Free-slot mode: send single op via REST sendTx (not batch)
    if free_slot_mode and len(tx_types) == 1:
        try:
            async with _sdk_write_lock:
                if time.monotonic() < _global_backoff_until:
                    client.nonce_manager.acknowledge_failure(signed_nonces[0])
                    return
                _record_ops_sent(1)
                await _send_single_op_rest(client, tx_types[0], tx_infos[0], signed_ops[0])
        except Exception as e:
            logger.error("Free-slot single send exception: %s", e, exc_info=True)
            client.nonce_manager.acknowledge_failure(signed_nonces[0])
        return

    try:
        async with _sdk_write_lock:
            if time.monotonic() < _global_backoff_until:
                logger.debug("Batch aborted: global backoff active (acquired lock too late)")
                for aki in signed_nonces:
                    client.nonce_manager.acknowledge_failure(aki)
                return
            _record_ops_sent(len(signed_ops))

            # Try WS first, fall back to REST
            ws_resp = None
            if _tx_ws is not None and _tx_ws.is_connected:
                ws_resp = await _tx_ws.send_batch(tx_types, tx_infos)

            if ws_resp is not None:
                # Parse WS response
                logger.debug("WS batch raw response: %s", ws_resp)
                # Check for error envelope: {"error": {"code": ..., "message": ...}}
                ws_error = ws_resp.get("error")
                if isinstance(ws_error, dict):
                    raw_code = ws_error.get("code", -1)
                    message = str(ws_error.get("message", ""))
                else:
                    raw_code = ws_resp.get("code", ws_resp.get("status_code", 0))
                    message = str(ws_resp.get("message", ""))
                code = int(raw_code) if raw_code is not None else 0
                # WS uses HTTP-style codes: 200 = success, 0 = success
                if code == 200:
                    code = 0
                quota_remaining = ws_resp.get("volume_quota_remaining", "?")
                send_method = "WS"
                _update_volume_quota(quota_remaining)
            else:
                # REST fallback
                resp = await client.send_tx_batch(tx_types, tx_infos)
                code = resp.code
                message = resp.message or ""
                quota_remaining = resp.volume_quota_remaining
                send_method = "REST"
                _update_volume_quota(quota_remaining)
                # Normalize REST code like WS: 200 = success
                if code == 200:
                    code = 0

        if code != 0:
            err_msg = message or f"code={code}"
            err_lower = err_msg.lower()
            logger.warning(
                "Batch response error (%s): code=%s message=%s (quota_remaining=%s)",
                send_method, code, err_msg, quota_remaining,
            )
            if "quota" in err_lower and "remained" not in err_lower and "didn't use" not in err_lower:
                # Volume quota exhausted — use free 15s slot pacing, not exponential backoff
                _update_volume_quota(0)
                logger.warning("Batch hit volume quota limit; switching to free-slot pacing (15s)")
                for aki in signed_nonces:
                    client.nonce_manager.acknowledge_failure(aki)
                client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
                return
            if "429" in err_lower or "too many" in err_lower:
                logger.warning("Batch hit 429 rate limit; triggering global backoff")
                _trigger_global_backoff()
                for aki in signed_nonces:
                    client.nonce_manager.acknowledge_failure(aki)
                return
            if "nonce" in err_lower:
                logger.warning("Batch nonce error: %s; refreshing nonces", err_msg)
                seen_keys = set()
                for aki in signed_nonces:
                    if aki not in seen_keys:
                        client.nonce_manager.hard_refresh_nonce(aki)
                        seen_keys.add(aki)
                return
            logger.error("Batch send failed (%s): %s", send_method, err_msg)
            _record_order_rejection(f"batch:{err_msg}")
            return

        # Success
        logger.info(
            "Batch sent via %s: %d ops OK (quota remaining: %s)",
            send_method, len(signed_ops), quota_remaining,
        )
        _record_order_success()
        for op in signed_ops:
            if op.action != "cancel":
                order_manager.bind_live(op.side, op.order_id, op.price, op.size, level=op.level)

    except Exception as e:
        body = getattr(e, 'body', None) or getattr(e, 'reason', '')
        logger.error("Batch send_tx_batch exception: %s | body=%s", e, body, exc_info=True)
        if _is_quota_error(e) or _is_transient_error(e):
            if _is_quota_error(e):
                _update_volume_quota(0)
            else:
                _trigger_global_backoff()
            for aki in signed_nonces:
                client.nonce_manager.acknowledge_failure(aki)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
            return
        _record_order_rejection("batch:exception")


def calculate_order_prices(mid_price, position_size=0.0):
    """Calculate bid/ask prices for all order levels.

    Returns a list of ``NUM_LEVELS`` ``(buy_price, sell_price)`` tuples.
    Level 0 is the tight (base) spread from vol_obi; level 1+ widen the
    spread by ``SPREAD_FACTOR_LEVEL1``.
    """
    none_levels = [(None, None)] * NUM_LEVELS
    calc = state.vol_obi_state.calculator
    if calc is not None and calc.warmed_up:
        try:
            buy_0, sell_0 = calc.quote(mid_price, position_size)
            if buy_0 is None or sell_0 is None:
                return none_levels
            levels = [(buy_0, sell_0)]
            # Derive wider levels by scaling the spread from mid
            bid_depth = mid_price - buy_0
            ask_depth = sell_0 - mid_price
            tick = state.config.price_tick_float
            for lvl in range(1, NUM_LEVELS):
                factor = SPREAD_FACTOR_LEVEL1 ** lvl
                raw_bid = mid_price - bid_depth * factor
                raw_ask = mid_price + ask_depth * factor
                if tick > 0:
                    raw_bid = math.floor(raw_bid / tick) * tick
                    raw_ask = math.ceil(raw_ask / tick) * tick
                levels.append((raw_bid, raw_ask))
            return levels
        except (ValueError, ZeroDivisionError, OverflowError) as e:
            logger.error(f"Error in vol_obi quote: {e}", exc_info=True)
            return none_levels
    # Not warmed up yet — no fallback
    return none_levels

_MAX_CONSECUTIVE_LOOP_ERRORS = 10


async def market_making_loop(client):
    logger.info("Starting 2-sided market making loop...")
    _log_info = logger.isEnabledFor(logging.INFO)
    _log_debug = logger.isEnabledFor(logging.DEBUG)
    consecutive_errors = 0
    _loop_start_time = time.monotonic()
    _warmup_logged = False
    _last_warmup_log_min = -1
    _warmup_complete_logged = False
    _last_quote_log_time = 0.0

    while True:
        try:
            # Time-based warmup: collect data without trading
            elapsed = time.monotonic() - _loop_start_time
            if elapsed < WARMUP_SECONDS:
                if not _warmup_logged:
                    logger.info(
                        "Warmup period: collecting data for %.0f seconds before trading...",
                        WARMUP_SECONDS,
                    )
                    _warmup_logged = True
                else:
                    current_min = int(elapsed) // 60
                    if current_min > _last_warmup_log_min and _log_info:
                        logger.info(
                            "Warmup: %d/%d seconds elapsed",
                            int(elapsed), int(WARMUP_SECONDS),
                        )
                        _last_warmup_log_min = current_min
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            if not _warmup_complete_logged:
                _warmup_complete_logged = True
                global _trading_start_time
                _trading_start_time = time.monotonic()
                _calc = state.vol_obi_state.calculator
                vol_ready = (_calc is not None and _calc.warmed_up)
                ba = state.binance_alpha
                binance_ready = ba is None or ba.warmed_up
                # Refresh capital timestamp — data is valid (no trading during warmup)
                if state.account.available_capital is not None:
                    state.account.last_capital_update = time.monotonic()
                # Reconnect TxWebSocket (may have been dropped during warmup)
                if _tx_ws is not None and not _tx_ws.is_connected:
                    logger.info("Reconnecting TxWebSocket after warmup...")
                    await _tx_ws.connect()
                logger.info(
                    "Warmup complete (%.0fs). vol_obi ready=%s, binance_alpha ready=%s",
                    WARMUP_SECONDS, vol_ready, binance_ready,
                )
            ws_healthy = check_websocket_health()
            risk_controller.maybe_recover(websocket_healthy=ws_healthy)

            if risk_controller.is_paused():
                if not risk_controller.pause_cancel_done:
                    logger.warning("Trading is paused (%s); cancelling local live orders.", state.risk.pause_reason)
                    cancel_ops = []
                    for lvl in range(NUM_LEVELS):
                        bid_id = state.orders.bid_order_ids[lvl]
                        ask_id = state.orders.ask_order_ids[lvl]
                        if bid_id is not None:
                            exchange_id = _resolve_exchange_id(bid_id)
                            if exchange_id is not None:
                                cancel_ops.append(BatchOp(
                                    side="buy", level=lvl, action="cancel",
                                    price=0, size=0,
                                    order_id=bid_id, exchange_id=exchange_id,
                                ))
                        if ask_id is not None:
                            exchange_id = _resolve_exchange_id(ask_id)
                            if exchange_id is not None:
                                cancel_ops.append(BatchOp(
                                    side="sell", level=lvl, action="cancel",
                                    price=0, size=0,
                                    order_id=ask_id, exchange_id=exchange_id,
                                ))
                    if cancel_ops:
                        if await _wait_for_write_slot(op_count=len(cancel_ops), cancel_only=True):
                            await sign_and_send_batch(client, cancel_ops)
                    elif _has_live_local_orders():
                        logger.info("Pause cleanup waiting for exchange order ids before sending cancels.")

                    reconcile_ok = False
                    try:
                        reconcile_ok = await reconcile_orders_with_exchange(client, source="pause_cancel_verify")
                    except Exception as exc:
                        logger.error("Post-pause reconciliation failed: %s", exc)
                    if reconcile_ok and not _has_live_local_orders():
                        risk_controller.pause_cancel_done = True
                    else:
                        logger.warning("Pause cleanup incomplete; will retry while trading remains paused.")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            if not ws_healthy:
                logger.warning("Websocket connection unhealthy, attempting restart...")
                if not await restart_websocket():
                    await asyncio.sleep(10)
                    continue

            # Wait for fresh order book data but wake up to check state
            # Clear before wait to avoid losing updates between wait() returning and clear()
            order_book_received.clear()
            try:
                await asyncio.wait_for(order_book_received.wait(), timeout=ORDER_TIMEOUT)
            except asyncio.TimeoutError:
                pass

            # Snapshot volatile state into locals for speed and consistency
            snap_mid = state.market.mid_price
            snap_position = state.account.position_size
            snap_capital = state.account.available_capital

            if snap_mid is None:
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            base_amount = calculate_dynamic_base_amount(snap_mid, capital=snap_capital)
            if base_amount is None or base_amount <= 0:
                if _log_info:
                    logger.warning("Base amount is zero or invalid; skipping order refresh.")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            level_prices = calculate_order_prices(
                snap_mid, position_size=snap_position)

            # Log level-0 quote (rate-limited to once every 10s)
            buy_0, sell_0 = level_prices[0]
            if buy_0 is not None and sell_0 is not None:
                _now_mono = time.monotonic()
                if _log_info and _now_mono - _last_quote_log_time >= 10.0:
                    _last_quote_log_time = _now_mono
                    bid_spread_pct = (snap_mid - buy_0) / snap_mid * 100 if snap_mid > 0 else 0
                    ask_spread_pct = (sell_0 - snap_mid) / snap_mid * 100 if snap_mid > 0 else 0
                    logger.info(
                        "QUOTING | Position: %+.4f | Mid: $%.4f | Bid: $%.4f (-%.4f%%) | Ask: $%.4f (+%.4f%%) | Quota: %s | Thr: %.0fbp",
                        snap_position, snap_mid, buy_0, bid_spread_pct, sell_0, ask_spread_pct,
                        _volume_quota_remaining if _volume_quota_remaining is not None else "?",
                        _adaptive_threshold_bps(),
                    )
            else:
                if _log_debug:
                    logger.debug("Could not calculate quotes; skipping refresh.")

            # Quota recovery: if enabled, critically low, NOT in cooldown, and past post-warmup grace
            if (_QR_ENABLED
                    and _volume_quota_remaining is not None
                    and _volume_quota_remaining < _QR_TRIGGER
                    and not _quota_recovery_in_progress
                    and time.monotonic() - _quota_recovery_last_attempt >= _QR_COOLDOWN
                    and time.monotonic() - _trading_start_time >= _QR_POST_WARMUP_GRACE):
                recovered = await _attempt_quota_recovery(client)
                if recovered:
                    logger.info("Quota recovered, resuming normal operations")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue  # re-evaluate on next iteration

            # Batch all order operations into a single send_tx_batch call
            ops = collect_order_operations(level_prices, base_amount, _log_debug)
            if ops:
                if time.monotonic() < _global_backoff_until:
                    pass  # in backoff — existing orders stay live
                elif await _wait_for_write_slot(
                    op_count=len(ops),
                    cancel_only=all(o.action == "cancel" for o in ops),
                ):
                    await sign_and_send_batch(client, ops)

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            logger.error("Unhandled error in market_making_loop: %s", e, exc_info=True)
            if consecutive_errors >= _MAX_CONSECUTIVE_LOOP_ERRORS:
                logger.error(
                    "Pausing trading: %d consecutive unhandled errors",
                    consecutive_errors,
                )
                risk_controller.trigger_pause(
                    f"{consecutive_errors} consecutive unhandled loop errors"
                )
                consecutive_errors = 0
            await asyncio.sleep(5)


async def track_balance():
    log_path = os.path.join(LOG_DIR, "balance_log.txt")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    loop = asyncio.get_running_loop()
    while True:
        try:
            if state.account.position_size == 0 and state.account.portfolio_value is not None:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                line = f"[{timestamp}] Portfolio Value: ${state.account.portfolio_value:,.2f}\n"
                await loop.run_in_executor(None, _append_line, log_path, line)
                logger.info(f"💰 Portfolio value of ${state.account.portfolio_value:,.2f} logged to {log_path}")
            elif state.account.position_size != 0:
                logger.info(f"⏸️ Skipping balance logging (open position: {state.account.position_size})")
            else:
                logger.info("⏸️ Skipping balance logging (portfolio value not yet received)")
        except Exception as e:
            logger.error(f"❌ Error in track_balance: {e}", exc_info=True)
        await asyncio.sleep(300)


def _append_line(path, line):
    with open(path, "a") as f:
        f.write(line)


async def periodic_orderbook_sanity_check(interval=None, tolerance_pct=None):
    """Background task: compare WS orderbook against ticker WS data.

    Uses the ticker WS channel (best bid/offer) instead of REST.
    Falls back to REST only if ticker data is unavailable.
    On divergence, sets ``ws_reconnect_event`` to trigger a WS reconnect
    which will deliver a fresh snapshot and re-sync the local book.
    """
    if interval is None:
        interval = 30  # 30s since ticker is real-time (was 10s with REST)
    if tolerance_pct is None:
        tolerance_pct = SANITY_CHECK_TOLERANCE_PCT

    while True:
        await asyncio.sleep(interval)
        try:
            ob = state.market.local_order_book
            if not ob['initialized']:
                continue

            if not ob['bids'] or not ob['asks']:
                continue

            try:
                ws_best_bid = ob['bids'].peekitem(-1)[0]
                ws_best_ask = ob['asks'].peekitem(0)[0]
            except (IndexError, ValueError):
                continue

            if ws_best_bid >= ws_best_ask:
                logger.warning(
                    "Orderbook sanity FAILED: crossed book bid=%.4f >= ask=%.4f",
                    ws_best_bid, ws_best_ask,
                )
                ws_reconnect_event.set()
                continue

            # Use ticker WS data as reference (no REST call needed)
            ticker_age = time.monotonic() - state.market.ticker_updated_at
            ticker_bid = state.market.ticker_best_bid
            ticker_ask = state.market.ticker_best_ask

            if ticker_bid is not None and ticker_ask is not None and ticker_age < 30.0:
                # Compare orderbook WS vs ticker WS
                bid_diff_pct = abs(ws_best_bid - ticker_bid) / ticker_bid * 100 if ticker_bid > 0 else 0
                ask_diff_pct = abs(ws_best_ask - ticker_ask) / ticker_ask * 100 if ticker_ask > 0 else 0

                if bid_diff_pct > tolerance_pct or ask_diff_pct > tolerance_pct:
                    logger.warning(
                        "Orderbook sanity FAILED (ticker) | WS bid=%.4f ask=%.4f | Ticker bid=%.4f ask=%.4f | diff bid=%.4f%% ask=%.4f%%",
                        ws_best_bid, ws_best_ask, ticker_bid, ticker_ask,
                        bid_diff_pct, ask_diff_pct,
                    )
                    ws_reconnect_event.set()
                    logger.info("Triggered WS reconnect via sanity checker (ticker)")
                else:
                    logger.info(
                        "Orderbook sanity OK (ticker) | bid_diff=%.4f%% ask_diff=%.4f%%",
                        bid_diff_pct, ask_diff_pct,
                    )
            else:
                # Ticker data unavailable/stale — fall back to REST
                logger.debug("Ticker data stale (%.1fs) or missing; falling back to REST sanity check", ticker_age)
                result = await check_orderbook_sanity(
                    market_id=state.config.market_id,
                    ws_bids=ob['bids'],
                    ws_asks=ob['asks'],
                    tolerance_pct=tolerance_pct,
                )
                if result.ok:
                    logger.info(
                        "Orderbook sanity OK (REST fallback) | bid_diff=%.4f%% ask_diff=%.4f%% latency=%.0fms",
                        result.bid_diff_pct, result.ask_diff_pct, result.latency_ms,
                    )
                else:
                    logger.warning(
                        "Orderbook sanity FAILED (REST fallback): %s | WS bid=%.4f ask=%.4f | REST bid=%.4f ask=%.4f",
                        result.reason,
                        result.ws_best_bid, result.ws_best_ask,
                        result.rest_best_bid, result.rest_best_ask,
                    )
                    ws_reconnect_event.set()
                    logger.info("Triggered WS reconnect via sanity checker (REST fallback)")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Error in orderbook sanity check: %s", e, exc_info=True)


# === COLD PATH ===

async def main():
    global ws_task, stale_order_task

    logger.info("🚀 === Market Maker v2 Starting (2-Sided Quoting) ===")
    _validate_config()

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)
    order_api = lighter.OrderApi(api_client)

    market_id, price_tick, amount_tick = await get_market_details_async(MARKET_SYMBOL)
    if market_id is None:
        logger.error(f"❌ Could not retrieve market details for {MARKET_SYMBOL}. Exiting.")
        return
    state.config.market_id = market_id
    state.config.price_tick_size = Decimal(str(price_tick))
    state.config.amount_tick_size = Decimal(str(amount_tick)) if amount_tick else Decimal(0)
    state.config.price_tick_float = float(state.config.price_tick_size)
    state.config.amount_tick_float = float(state.config.amount_tick_size)

    # Fetch exchange-level minimum order sizes
    try:
        order_books_resp = await order_api.order_books()
        for ob in order_books_resp.order_books:
            if ob.market_id == market_id:
                state.config.min_base_amount = float(getattr(ob, 'min_base_amount', 0) or 0)
                state.config.min_quote_amount = float(getattr(ob, 'min_quote_amount', 0) or 0)
                break
    except Exception as exc:
        logger.warning("Could not fetch min order sizes: %s", exc)

    logger.info(
        "📊 Market %s: id=%s, tick(price)=%s, tick(amount)=%s, min_base=%s, min_quote=$%s",
        MARKET_SYMBOL, state.config.market_id, state.config.price_tick_size,
        state.config.amount_tick_size, state.config.min_base_amount, state.config.min_quote_amount,
    )

    # Initialize vol_obi spread calculator
    state.vol_obi_state.calculator = VolObiCalculator(
        tick_size=state.config.price_tick_float,
        window_steps=VOL_OBI_WINDOW_STEPS,
        step_ns=VOL_OBI_STEP_NS,
        vol_to_half_spread=VOL_OBI_VOL_TO_HALF_SPREAD,
        min_half_spread_bps=VOL_OBI_MIN_HALF_SPREAD_BPS,
        c1_ticks=VOL_OBI_C1_TICKS,
        skew=VOL_OBI_SKEW,
        looking_depth=VOL_OBI_LOOKING_DEPTH,
        min_warmup_samples=VOL_OBI_MIN_WARMUP_SAMPLES,
        max_position_dollar=VOL_OBI_MAX_POSITION_DOLLAR,
    )
    logger.info(
        "📈 Spread mode: vol_obi (Volatility + OBI) | "
        "vol_to_half=%.2f | min_bps=%.1f | skew=%.2f | warmup=%d samples",
        VOL_OBI_VOL_TO_HALF_SPREAD, VOL_OBI_MIN_HALF_SPREAD_BPS,
        VOL_OBI_SKEW, VOL_OBI_MIN_WARMUP_SAMPLES,
    )

    # Start Binance alpha source (if applicable)
    binance_task = None
    if ALPHA_SOURCE == "binance":
        binance_sym = lighter_to_binance_symbol(MARKET_SYMBOL)
        if binance_sym is not None:
            shared_alpha = SharedAlpha(min_samples=BINANCE_OBI_MIN_SAMPLES)
            state.binance_alpha = shared_alpha
            binance_client = BinanceObiClient(
                binance_symbol=binance_sym,
                shared_alpha=shared_alpha,
                window_size=BINANCE_OBI_WINDOW,
                looking_depth=BINANCE_OBI_LOOKING_DEPTH,
                stale_threshold=BINANCE_STALE_SECONDS,
            )
            binance_task = asyncio.create_task(binance_client.run())
            logger.info(
                "Binance alpha: %s | window=%d stale=%.0fs min_samples=%d",
                binance_sym, BINANCE_OBI_WINDOW, BINANCE_STALE_SECONDS, BINANCE_OBI_MIN_SAMPLES,
            )
        else:
            logger.info("No Binance mapping for %s; using Lighter OBI", MARKET_SYMBOL)

    try:
        client = build_signer_client(
            url=BASE_URL,
            account_index=ACCOUNT_INDEX,
            private_key=API_KEY_PRIVATE_KEY,
            api_key_index=API_KEY_INDEX,
        )
    except Exception as exc:
        logger.error(f"❌ Failed to initialize SignerClient: {trim_exception(exc)}")
        await api_client.close()
        return
    err = client.check_client()
    if err is not None:
        logger.error(f"❌ CheckClient error: {trim_exception(err)}")
        await api_client.close()
        await client.close()
        return
    logger.info("✅ Client connected successfully")

    # Clean slate: cancel all at startup — but skip if no open orders
    _startup_orders = await _fetch_account_active_orders(client)
    if _startup_orders is None:
        # Fetch failed — cancel unconditionally to be safe
        logger.info("Could not fetch active orders; cancelling all as precaution")
        await cancel_all_orders(client)
    elif len(_startup_orders) > 0:
        logger.info("Found %d open orders at startup — cancelling all", len(_startup_orders))
        await cancel_all_orders(client)
    else:
        logger.info("No open orders at startup — skipping cancel_all (saves quota)")
    # Record the send time so free-slot timer starts from here
    global _last_send_time
    _last_send_time = time.monotonic()
    logger.info("Startup volume quota: %s remaining",
                _volume_quota_remaining if _volume_quota_remaining is not None else "unknown")
    if _volume_quota_remaining is not None and _volume_quota_remaining < _RL_QUOTA_MEDIUM:
        logger.warning("LOW STARTUP QUOTA: only %d remaining — free-slot mode (1 op per 15s via REST)",
                       _volume_quota_remaining)
        # Refresh nonce (quota 429s can corrupt nonce state)
        client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
    await asyncio.sleep(3)

    state.market.last_order_book_update = time.monotonic()

    # Initialize TxWebSocket for sending transactions via WS
    global _tx_ws
    _tx_ws = TxWebSocket(WEBSOCKET_URL)
    await _tx_ws.connect()

    # Start WebSocket Tasks
    ws_task = asyncio.create_task(subscribe_to_market_data(state.config.market_id))
    ticker_task = asyncio.create_task(subscribe_to_ticker(state.config.market_id))
    logger.info("✅ ticker WS subscription started for market %d", state.config.market_id)

    user_stats_task = asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX))
    account_all_task = asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX))

    account_orders_task = None
    if client is not None and API_KEY_PRIVATE_KEY:
        account_orders_task = asyncio.create_task(
            subscribe_to_account_orders(client, state.config.market_id, ACCOUNT_INDEX)
        )
        logger.info("✅ account_orders authenticated WS task started")
    else:
        logger.info("ℹ️  account_orders WS skipped (no credentials)")

    stale_order_task = None
    if client is not None and API_KEY_PRIVATE_KEY:
        stale_order_task = asyncio.create_task(
            stale_order_reconciler_loop(client, state.config.market_id, ACCOUNT_INDEX)
        )
        logger.info(
            "✅ stale-order reconciler task started (interval=%.1fs, debounce=%d)",
            STALE_ORDER_POLLER_INTERVAL_SEC,
            STALE_ORDER_DEBOUNCE_COUNT,
        )

    try:
        logger.info("⏳ Waiting for initial order book, account data, and position data...")
        await asyncio.wait_for(order_book_received.wait(), timeout=30.0)
        logger.info(f"✅ Websocket connected for market {state.config.market_id}")
        
        logger.info("⏳ Waiting for valid account capital...")
        await asyncio.wait_for(account_state_received.wait(), timeout=30.0)
        logger.info(f"✅ Received valid account capital: ${state.account.available_capital}; and portfolio value: ${state.account.portfolio_value}.")

        logger.info("⏳ Waiting for initial position data...")
        await asyncio.wait_for(account_all_received.wait(), timeout=30.0)
        logger.info(f"✅ Received initial position data. Current size: {state.account.position_size}")

        # Emergency close any leftover position from a previous unclean shutdown
        if abs(state.account.position_size) > EPSILON:
            logger.warning(
                "Detected open position (%.6f) on startup — attempting emergency close.",
                state.account.position_size,
            )
            closed = await emergency_close_position(client, reason="startup")
            if not closed:
                pos_value = abs(state.account.position_size) * (state.market.mid_price or 0)
                if pos_value > 50.0:
                    logger.error("Failed to close startup position ($%.2f). Aborting to prevent compounding risk.", pos_value)
                    return
                logger.warning(
                    "Failed to close small startup position ($%.2f). Continuing — quoting skew will manage it.",
                    pos_value,
                )

        logger.info(f"⚙️ Attempting to set leverage to {LEVERAGE}x with {MARGIN_MODE} margin...")
        _, _, err = await adjust_leverage(client, state.config.market_id, LEVERAGE, MARGIN_MODE, logger=logger)
        if err:
            logger.error(f"❌ Failed to adjust leverage: {err}. Continuing with default leverage.")
        else:
            logger.info(f"✅ Successfully set leverage to {LEVERAGE}x")

        balance_task = asyncio.create_task(track_balance())
        sanity_task = asyncio.create_task(periodic_orderbook_sanity_check())
        await market_making_loop(client)

    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for initial data from websockets.")
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 === Shutdown signal received - Stopping... ===")
    finally:
        logger.info("🧹 === Market Maker Cleanup Starting ===")
        tasks_to_cancel = []
        if 'user_stats_task' in locals():
            tasks_to_cancel.append(user_stats_task)
        if 'account_all_task' in locals():
            tasks_to_cancel.append(account_all_task)
        if account_orders_task is not None:
            tasks_to_cancel.append(account_orders_task)
        if stale_order_task is not None:
            tasks_to_cancel.append(stale_order_task)
        if 'balance_task' in locals():
            tasks_to_cancel.append(balance_task)
        if 'sanity_task' in locals():
            tasks_to_cancel.append(sanity_task)
        if 'ticker_task' in locals():
            tasks_to_cancel.append(ticker_task)
        if 'ws_task' in locals():
            tasks_to_cancel.append(ws_task)
        if binance_task is not None:
            tasks_to_cancel.append(binance_task)

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        try:
            logger.info("🛡️ Final safety measure: attempting to cancel all orders.")
            await asyncio.wait_for(cancel_all_orders(client), timeout=10)
        except asyncio.TimeoutError:
            logger.error("Timeout during final order cancellation.")
        except Exception as e:
            logger.error(f"Error during final order cancellation: {e}")

        # Emergency close any open position before shutting down.
        # We need WS data for best prices, so briefly re-subscribe if orderbook is stale.
        if abs(state.account.position_size) > EPSILON:
            logger.warning(
                "Open position detected at shutdown (%.6f) — attempting emergency close.",
                state.account.position_size,
            )
            # If the orderbook is gone (WS tasks cancelled), fetch REST prices directly
            best_bid, best_ask = get_best_prices()
            if best_bid is None or best_ask is None:
                try:
                    loop = asyncio.get_running_loop()
                    from orderbook_sanity import _fetch_rest_top_of_book
                    rest_bid, rest_ask = await loop.run_in_executor(
                        None, _fetch_rest_top_of_book, state.config.market_id, 5.0,
                    )
                    if rest_bid > 0 and rest_ask > 0:
                        state.market.mid_price = (rest_bid + rest_ask) / 2.0
                        ob = state.market.local_order_book
                        ob['bids'][rest_bid] = 1.0
                        ob['asks'][rest_ask] = 1.0
                except Exception as exc:
                    logger.error("Failed to fetch REST prices for shutdown close: %s", exc)
            try:
                await asyncio.wait_for(
                    emergency_close_position(client, reason="shutdown"),
                    timeout=15,
                )
            except asyncio.TimeoutError:
                logger.error("Timeout during shutdown emergency position close!")
            except Exception as e:
                logger.error("Error during shutdown emergency position close: %s", e)

        # Verify no orders remain live after shutdown cancel
        try:
            remaining = await asyncio.wait_for(
                _fetch_account_active_orders(client), timeout=5
            )
            if remaining is None:
                logger.warning("Could not verify order cancellation (REST fetch failed).")
            elif len(remaining) > 0:
                live_ids = [o.get("order_index", "?") for o in remaining]
                logger.error(
                    "ORDERS STILL LIVE AFTER SHUTDOWN CANCEL: %s — manual intervention required!",
                    live_ids,
                )
            else:
                logger.info("Verified: no orders remain live on exchange.")
        except (asyncio.TimeoutError, Exception) as exc:
            logger.warning("Post-shutdown verification failed: %s", exc)

        if _tx_ws is not None:
            await _tx_ws.close()
        await client.close()
        await api_client.close()
        logger.info("🛑 Market maker stopped.")

# ============ Entrypoint with signal handling ============ 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Lighter market maker")
    parser.add_argument("--symbol", default=os.getenv("MARKET_SYMBOL", "BTC"), help="Market symbol to trade")
    args = parser.parse_args()
    MARKET_SYMBOL = args.symbol.upper()
    os.environ["MARKET_SYMBOL"] = MARKET_SYMBOL

    async def main_with_signal_handling():
        loop = asyncio.get_running_loop()
        main_task = asyncio.create_task(main())

        def shutdown_handler(sig):
            logger.info(f"🛑 Received exit signal {sig.name}. Starting graceful shutdown...")
            if not main_task.done():
                main_task.cancel()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_handler, sig)
            except NotImplementedError:
                pass

        try:
            await main_task
        except asyncio.CancelledError:
            logger.info("🛑 Main task cancelled. Cleanup is handled in main().")

    try:
        asyncio.run(main_with_signal_handling())
        logger.info("✅ Application has finished gracefully.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Application exiting.")
