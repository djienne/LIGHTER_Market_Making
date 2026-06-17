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
import csv
import subprocess


def _build_cython_extension_if_missing() -> bool:
    """Build the Cython fast path when the extension has not been compiled yet."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    setup_path = os.path.join(project_dir, "setup_cython.py")
    if not os.path.exists(setup_path):
        return False
    try:
        result = subprocess.run(
            [_sys.executable, setup_path, "build_ext", "--inplace"],
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Automatic Cython build failed: %s", exc)
        return False
    if result.returncode != 0:
        logging.getLogger(__name__).warning(
            "Automatic Cython build failed with exit code %s:\n%s",
            result.returncode,
            result.stdout,
        )
        return False
    return True


def _load_cython_extension() -> bool:
    global _BookSide, _price_change_bps_c, _dynamic_max_position_c
    try:
        from _vol_obi_fast import CBookSide as _fast_book_side
        from _vol_obi_fast import price_change_bps_fast as _fast_price_change_bps
        from _vol_obi_fast import dynamic_max_position_fast as _fast_dynamic_max_position
    except ImportError:
        return False
    _BookSide = _fast_book_side
    _price_change_bps_c = _fast_price_change_bps
    _dynamic_max_position_c = _fast_dynamic_max_position
    return True


_BookSide = None
_price_change_bps_c = None
_dynamic_max_position_c = None
_CYTHON_AVAILABLE = _load_cython_extension()
if not _CYTHON_AVAILABLE and _build_cython_extension_if_missing():
    _CYTHON_AVAILABLE = _load_cython_extension()

if not _CYTHON_AVAILABLE:
    _BookSide = None
    _price_change_bps_c = None
    _dynamic_max_position_c = None
# Fail-fast if Cython not available (opt out with ALLOW_PYTHON_FALLBACK=1 for dev/test)
ALLOW_PYTHON_FALLBACK = os.getenv("ALLOW_PYTHON_FALLBACK", "").lower() in ("1", "true", "yes")
if not _CYTHON_AVAILABLE and not ALLOW_PYTHON_FALLBACK:
    raise ImportError(
        "Cython extension _vol_obi_fast not available after automatic build attempt. "
        "Install a C compiler and Python development headers, then build with: "
        "python setup_cython.py build_ext --inplace\n"
        "Set ALLOW_PYTHON_FALLBACK=1 for dev/test only."
    )
if not _CYTHON_AVAILABLE:
    from sortedcontainers import SortedDict as _BookSide
    logging.getLogger(__name__).warning(
        "PYTHON FALLBACK active (ALLOW_PYTHON_FALLBACK=1) — not recommended for production. "
        "Build Cython: python setup_cython.py build_ext --inplace"
    )
import websockets
from utils import EPSILON, get_market_details_async, load_config_params
from adjust_leverage import adjust_leverage
from orderbook import apply_orderbook_update
from ws_manager import ws_subscribe, ws_subscribe_fast
from orderbook_sanity import check_orderbook_sanity
from vol_obi import VolObiCalculator
from cartea_jaimungal import CarteaJaimungalCalculator, CarteaJaimungalParams
from lighter_estimators import CJSnapshot, LighterCJEstimator
from live_metrics import LiveMetricsTracker, LiveStateStore, QualityAdjustment
from binance_obi import (
    BinanceBookTickerClient, BinanceDiffDepthClient,
    SharedAlpha, SharedBBO, lighter_to_binance_symbol,
)
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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float_list(name: str, default: list[float]) -> list[float]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)
    values: list[float] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = float(item)
        except ValueError:
            continue
        if value > 0:
            values.append(value)
    return values or list(default)


def _normalize_quote_engine(value) -> str:
    engine = str(value or "vol_obi").strip().lower().replace("-", "_")
    if engine in {"cj", "cartea", "cartea_jaimungal"}:
        return "cartea_jaimungal"
    if engine in {"vol", "obi", "vol_obi"}:
        return "vol_obi"
    raise SystemExit(f"FATAL: unknown quote_engine={value!r}; expected vol_obi or cartea_jaimungal")

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WEBSOCKET_URL = "wss://mainnet.zklighter.elliot.ai/stream"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "0"))
API_KEY_MAKER_ONLY: Optional[bool] = None

MARKET_SYMBOL = os.getenv("MARKET_SYMBOL", "BTC")
MARKET_ID = None
PRICE_TICK_SIZE = None
AMOUNT_TICK_SIZE = None

LEVERAGE = int(os.getenv("LEVERAGE", _trading.get("leverage", 2)))
MARGIN_MODE = os.getenv("MARGIN_MODE", _trading.get("margin_mode", "cross"))
POSITION_VALUE_THRESHOLD_USD = float(os.getenv(
    "POSITION_VALUE_THRESHOLD_USD",
    _trading.get("position_value_threshold_usd", 15.0)))
MIN_ORDER_VALUE_USD = float(_trading.get("min_order_value_usd", 14.5))
# Maker fee rate (0.00004 = 0.004%, Lighter premium tier; standard accounts are 0)
MAKER_FEE_RATE = float(os.getenv(
    "MAKER_FEE_RATE",
    _trading.get("maker_fee_rate", 0.00004)))

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
    _trading.get("default_quote_update_threshold_bps", 8.0)))
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
    _safety.get("max_live_orders_per_market", 4)))
PANIC_CLOSE_ON_STARTUP = _env_bool(
    "PANIC_CLOSE_ON_STARTUP",
    bool(_safety.get("panic_close_on_startup", False)),
)
PANIC_CLOSE_ON_SHUTDOWN = _env_bool(
    "PANIC_CLOSE_ON_SHUTDOWN",
    bool(_safety.get("panic_close_on_shutdown", False)),
)

_live_quality_cfg = _trading.get("live_quality", {})
LIVE_MARKOUT_HORIZONS = _env_float_list(
    "LIVE_MARKOUT_HORIZONS",
    [float(v) for v in _live_quality_cfg.get("markout_horizons_sec", [5, 30, 60])],
)
LIVE_QUALITY_WINDOW_SECONDS = float(os.getenv(
    "LIVE_QUALITY_WINDOW_SECONDS",
    _live_quality_cfg.get("window_seconds", 3600.0)))
LIVE_QUALITY_ADAPTIVE_ENABLED = _env_bool(
    "LIVE_QUALITY_ADAPTIVE_ENABLED",
    bool(_live_quality_cfg.get("adaptive_enabled", True)),
)
LIVE_QUALITY_ADAPTIVE_HORIZON = float(os.getenv(
    "LIVE_QUALITY_ADAPTIVE_HORIZON",
    _live_quality_cfg.get("adaptive_horizon_sec", 30.0)))
LIVE_QUALITY_ADVERSE_THRESHOLD_BPS = float(os.getenv(
    "LIVE_QUALITY_ADVERSE_THRESHOLD_BPS",
    _live_quality_cfg.get("adverse_threshold_bps", 2.0)))
LIVE_QUALITY_SPREAD_WIDEN_PER_BPS = float(os.getenv(
    "LIVE_QUALITY_SPREAD_WIDEN_PER_BPS",
    _live_quality_cfg.get("spread_widen_per_adverse_bps", 0.05)))
LIVE_QUALITY_MAX_SPREAD_MULTIPLIER = float(os.getenv(
    "LIVE_QUALITY_MAX_SPREAD_MULTIPLIER",
    _live_quality_cfg.get("max_spread_multiplier", 1.5)))
LIVE_QUALITY_SIZE_REDUCE_PER_BPS = float(os.getenv(
    "LIVE_QUALITY_SIZE_REDUCE_PER_BPS",
    _live_quality_cfg.get("size_reduce_per_adverse_bps", 0.06)))
LIVE_QUALITY_MIN_SIZE_MULTIPLIER = float(os.getenv(
    "LIVE_QUALITY_MIN_SIZE_MULTIPLIER",
    _live_quality_cfg.get("min_size_multiplier", 0.55)))
LIVE_QUALITY_METRICS_FLUSH_SECONDS = float(os.getenv(
    "LIVE_QUALITY_METRICS_FLUSH_SECONDS",
    _live_quality_cfg.get("metrics_flush_seconds", 10.0)))

_inventory_bias_cfg = _trading.get("inventory_exit_bias", {})
INVENTORY_EXIT_BIAS_ENABLED = _env_bool(
    "INVENTORY_EXIT_BIAS_ENABLED",
    bool(_inventory_bias_cfg.get("enabled", True)),
)
INVENTORY_EXIT_BIAS_MIN_RATIO = float(os.getenv(
    "INVENTORY_EXIT_BIAS_MIN_RATIO",
    _inventory_bias_cfg.get("min_ratio", 0.05)))
INVENTORY_EXIT_TIGHTEN_PER_RATIO = float(os.getenv(
    "INVENTORY_EXIT_TIGHTEN_PER_RATIO",
    _inventory_bias_cfg.get("exit_tighten_per_ratio", 0.45)))
INVENTORY_ADD_WIDEN_PER_RATIO = float(os.getenv(
    "INVENTORY_ADD_WIDEN_PER_RATIO",
    _inventory_bias_cfg.get("add_widen_per_ratio", 0.75)))
INVENTORY_MAX_EXIT_TIGHTEN = float(os.getenv(
    "INVENTORY_MAX_EXIT_TIGHTEN",
    _inventory_bias_cfg.get("max_exit_tighten", 0.35)))
INVENTORY_MAX_ADD_WIDEN = float(os.getenv(
    "INVENTORY_MAX_ADD_WIDEN",
    _inventory_bias_cfg.get("max_add_widen", 0.65)))
INVENTORY_ADVERSE_BOOST_PER_BPS = float(os.getenv(
    "INVENTORY_ADVERSE_BOOST_PER_BPS",
    _inventory_bias_cfg.get("adverse_boost_per_bps", 0.03)))

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
    _ws.get("account_recv_timeout", 1800.0)))

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
    if QUOTE_ENGINE == "cartea_jaimungal":
        if CJ_REFRESH_SECONDS <= 0:
            errors.append(f"CJ_REFRESH_SECONDS={CJ_REFRESH_SECONDS} must be > 0")
        if CJ_SPREAD_MULTIPLIER <= 0:
            errors.append(f"CJ_SPREAD_MULTIPLIER={CJ_SPREAD_MULTIPLIER} must be > 0")
        if CJ_MIN_HALF_SPREAD_BPS < 0 or CJ_MAX_HALF_SPREAD_BPS <= 0:
            errors.append("CJ half-spread bounds must be positive")
        if CJ_MIN_HALF_SPREAD_BPS > CJ_MAX_HALF_SPREAD_BPS:
            errors.append("CJ_MIN_HALF_SPREAD_BPS must be <= CJ_MAX_HALF_SPREAD_BPS")
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
    reduce_only: bool = False


class TxSendStatus(str, Enum):
    OK = "ok"
    REJECTED = "rejected"
    NOT_SENT = "not_sent"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class TxSendResult:
    status: TxSendStatus
    response: Optional[dict] = None
    reason: str = ""


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


def _is_maker_only_restriction(value) -> bool:
    msg = str(value).lower()
    return (
        "maker-only api key" in msg
        or "maker only api key" in msg
        or "0ms delay transactions" in msg
    )


def _mark_api_key_maker_only(reason: str) -> None:
    global API_KEY_MAKER_ONLY
    if API_KEY_MAKER_ONLY is True:
        return
    API_KEY_MAKER_ONLY = True
    logger.warning(
        "API key %d marked maker-only (%s); restricted transactions will be skipped",
        API_KEY_INDEX,
        reason,
    )


async def _detect_maker_only_api_key(
    client,
    account_id: Optional[int] = None,
    api_key_index: Optional[int] = None,
    timeout: float = 10.0,
) -> Optional[bool]:
    """Return whether the configured key is maker-only, or None if detection failed."""
    global API_KEY_MAKER_ONLY
    if client is None or not hasattr(client, "create_auth_token_with_expiry"):
        return None
    account_id = account_id if account_id is not None else ACCOUNT_INDEX
    api_key_index = api_key_index if api_key_index is not None else API_KEY_INDEX

    try:
        auth_token, err = client.create_auth_token_with_expiry()
    except Exception as exc:
        logger.warning("Maker-only API key detection failed while creating auth token: %s", exc)
        return None
    if err or not auth_token:
        logger.warning("Maker-only API key detection auth token unavailable: %s", err)
        return None

    loop = asyncio.get_running_loop()

    def _do_request() -> dict:
        resp = requests.get(
            f"{BASE_URL}/api/v1/getMakerOnlyApiKeys",
            params={"account_index": account_id},
            headers={"authorization": auth_token},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("getMakerOnlyApiKeys returned non-object JSON")
        return data

    try:
        data = await asyncio.wait_for(loop.run_in_executor(None, _do_request), timeout=timeout)
    except Exception as exc:
        logger.warning("Maker-only API key detection failed: %s", exc)
        return None

    raw_indexes = (
        data.get("api_key_indexes")
        or data.get("maker_only_api_key_indexes")
        or data.get("maker_only_indexes")
        or []
    )
    if not isinstance(raw_indexes, list):
        logger.warning("Maker-only API key detection returned unexpected indexes: %r", raw_indexes)
        return None
    try:
        maker_only_indexes = {int(index) for index in raw_indexes}
    except (TypeError, ValueError):
        logger.warning("Maker-only API key detection returned non-integer indexes: %r", raw_indexes)
        return None

    API_KEY_MAKER_ONLY = int(api_key_index) in maker_only_indexes
    if API_KEY_MAKER_ONLY:
        logger.info(
            "API key %d is maker-only; allowing post-only/modify/cancel/cancel-all only",
            api_key_index,
        )
    else:
        logger.info("API key %d is not maker-only", api_key_index)
    return API_KEY_MAKER_ONLY


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
    "VOL_OBI_VOL_TO_HALF_SPREAD", _vol_obi_cfg.get("vol_to_half_spread", 60.0)))
VOL_OBI_MIN_HALF_SPREAD_BPS = float(os.getenv(
    "VOL_OBI_MIN_HALF_SPREAD_BPS", _vol_obi_cfg.get("min_half_spread_bps", 8.0)))
VOL_OBI_C1_TICKS = float(os.getenv(
    "VOL_OBI_C1_TICKS", _vol_obi_cfg.get("c1_ticks", 40.0)))
VOL_OBI_SKEW = float(os.getenv(
    "VOL_OBI_SKEW", _vol_obi_cfg.get("skew", 0.1)))
VOL_OBI_LOOKING_DEPTH = float(os.getenv(
    "VOL_OBI_LOOKING_DEPTH", _vol_obi_cfg.get("looking_depth", 0.025)))
VOL_OBI_MIN_WARMUP_SAMPLES = int(os.getenv(
    "VOL_OBI_MIN_WARMUP_SAMPLES", _vol_obi_cfg.get("min_warmup_samples", 100)))
WARMUP_SECONDS = float(os.getenv(
    "WARMUP_SECONDS", _vol_obi_cfg.get("warmup_seconds", 600)))

# Quote engine config.  ``vol_obi`` is the legacy engine; ``cartea_jaimungal``
# uses Lighter public trades to estimate lambda/kappa/epsilon and solve
# inventory-aware bid/ask distances.
QUOTE_ENGINE = _normalize_quote_engine(os.getenv(
    "QUOTE_ENGINE",
    _trading.get("quote_engine", _trading.get("quoteEngine", "vol_obi")),
))

_cj_cfg = _trading.get("cartea_jaimungal", _trading.get("cj", {}))
_cj_estimator_cfg = _trading.get("cj_estimator", _config.get("cj_estimator", {}))

CJ_USE_ESTIMATOR = _env_bool(
    "CJ_USE_ESTIMATOR",
    bool(_cj_cfg.get("use_estimator", _cj_cfg.get("cj_use_estimator", False))
         or _cj_estimator_cfg.get("enabled", False)),
)
CJ_REQUIRE_ESTIMATOR_READY = _env_bool(
    "CJ_REQUIRE_ESTIMATOR_READY",
    bool(_cj_cfg.get("require_estimator_ready", _cj_cfg.get("cj_require_estimator_ready", False))),
)
CJ_ESTIMATOR_BLEND = float(os.getenv(
    "CJ_ESTIMATOR_BLEND", _cj_cfg.get("estimator_blend", _cj_cfg.get("cj_estimator_blend", 1.0))))
CJ_REFRESH_SECONDS = float(os.getenv(
    "CJ_REFRESH_SECONDS", _cj_cfg.get("refresh_seconds", _cj_cfg.get("cj_refresh_seconds", 300.0))))
CJ_LAMBDA_SCALE = float(os.getenv(
    "CJ_LAMBDA_SCALE", _cj_cfg.get("lambda_scale", _cj_cfg.get("cj_lambda_scale", 1.0))))
CJ_KAPPA_SCALE = float(os.getenv(
    "CJ_KAPPA_SCALE", _cj_cfg.get("kappa_scale", _cj_cfg.get("cj_kappa_scale", 1.0))))
CJ_EPSILON_SCALE = float(os.getenv(
    "CJ_EPSILON_SCALE", _cj_cfg.get("epsilon_scale", _cj_cfg.get("cj_epsilon_scale", 1.0))))
CJ_SIGMA2_SCALE = float(os.getenv(
    "CJ_SIGMA2_SCALE", _cj_cfg.get("sigma2_scale", _cj_cfg.get("cj_sigma2_scale", 1.0))))

CJ_LAMBDA = float(os.getenv("CJ_LAMBDA", _cj_cfg.get("lambda", _cj_cfg.get("cj_lambda", 0.30))))
CJ_KAPPA = float(os.getenv("CJ_KAPPA", _cj_cfg.get("kappa", _cj_cfg.get("cj_kappa", 0.035))))
CJ_EPSILON = float(os.getenv("CJ_EPSILON", _cj_cfg.get("epsilon", _cj_cfg.get("cj_epsilon", 4.0))))
CJ_SIGMA2_PER_SEC = float(os.getenv(
    "CJ_SIGMA2_PER_SEC", _cj_cfg.get("sigma2_per_sec", _cj_cfg.get("cj_sigma2_per_sec", 0.0))))
CJ_ALPHA = float(os.getenv("CJ_ALPHA", _cj_cfg.get("alpha", _cj_cfg.get("cj_alpha", 0.001))))
CJ_PHI = float(os.getenv("CJ_PHI", _cj_cfg.get("phi", _cj_cfg.get("cj_phi", 0.00008))))
CJ_HORIZON_SECONDS = float(os.getenv(
    "CJ_HORIZON_SECONDS", _cj_cfg.get("horizon_seconds", _cj_cfg.get("cj_horizon_seconds", 60.0))))
CJ_Q_MAX = int(os.getenv("CJ_Q_MAX", _cj_cfg.get("q_max", _cj_cfg.get("cj_q_max", 3))))
CJ_SOLVER_MODE = str(os.getenv(
    "CJ_SOLVER_MODE", _cj_cfg.get("solver_mode", _cj_cfg.get("cj_solver_mode", "asymmetric")))).strip().lower()
CJ_ASYM_N_STEPS = int(os.getenv(
    "CJ_ASYM_N_STEPS", _cj_cfg.get("asym_n_steps", _cj_cfg.get("cj_asym_n_steps", 30))))
CJ_ASYM_MAX_ITER = int(os.getenv(
    "CJ_ASYM_MAX_ITER", _cj_cfg.get("asym_max_iter", _cj_cfg.get("cj_asym_max_iter", 10))))
CJ_SPREAD_MULTIPLIER = float(os.getenv(
    "CJ_SPREAD_MULTIPLIER", _cj_cfg.get("spread_multiplier", _cj_cfg.get("cj_spread_multiplier", 1.5))))
CJ_MIN_HALF_SPREAD_BPS = float(os.getenv(
    "CJ_MIN_HALF_SPREAD_BPS", _cj_cfg.get("min_half_spread_bps", _cj_cfg.get("cj_min_half_spread_bps", 4.0))))
CJ_MAX_HALF_SPREAD_BPS = float(os.getenv(
    "CJ_MAX_HALF_SPREAD_BPS", _cj_cfg.get("max_half_spread_bps", _cj_cfg.get("cj_max_half_spread_bps", 80.0))))
CJ_INVENTORY_UNIT_BASE = float(os.getenv(
    "CJ_INVENTORY_UNIT_BASE", _cj_cfg.get("inventory_unit_base", _cj_cfg.get("cj_inventory_unit_base", 0.0002))))
CJ_MAX_TOXICITY = float(os.getenv(
    "CJ_MAX_TOXICITY", _cj_cfg.get("max_toxicity", _cj_cfg.get("cj_max_toxicity", 1.5))))
CJ_VOLATILITY_SPREAD_MULTIPLIER = float(os.getenv(
    "CJ_VOLATILITY_SPREAD_MULTIPLIER",
    _cj_cfg.get("volatility_spread_multiplier", _cj_cfg.get("cj_volatility_spread_multiplier", 0.10)),
))

CJ_ESTIMATOR_WINDOW_SECONDS = float(os.getenv(
    "CJ_ESTIMATOR_WINDOW_SECONDS", _cj_estimator_cfg.get("window_seconds", 900.0)))
CJ_ESTIMATOR_MARKOUT_SECONDS = float(os.getenv(
    "CJ_ESTIMATOR_MARKOUT_SECONDS", _cj_estimator_cfg.get("markout_seconds", 5.0)))
CJ_ESTIMATOR_MIN_TRADES_PER_SIDE = int(os.getenv(
    "CJ_ESTIMATOR_MIN_TRADES_PER_SIDE", _cj_estimator_cfg.get("min_trades_per_side", 8)))
CJ_ESTIMATOR_MIN_MARKOUTS_PER_SIDE = int(os.getenv(
    "CJ_ESTIMATOR_MIN_MARKOUTS_PER_SIDE", _cj_estimator_cfg.get("min_markouts_per_side", 4)))
CJ_ESTIMATOR_KAPPA_MIN = float(os.getenv(
    "CJ_ESTIMATOR_KAPPA_MIN", _cj_estimator_cfg.get("kappa_min", 0.005)))
CJ_ESTIMATOR_KAPPA_MAX = float(os.getenv(
    "CJ_ESTIMATOR_KAPPA_MAX", _cj_estimator_cfg.get("kappa_max", 0.25)))
CJ_ESTIMATOR_MIN_KAPPA_POINTS = int(os.getenv(
    "CJ_ESTIMATOR_MIN_KAPPA_POINTS", _cj_estimator_cfg.get("min_kappa_points", 4)))
CJ_ESTIMATOR_MIN_KAPPA_R2 = float(os.getenv(
    "CJ_ESTIMATOR_MIN_KAPPA_R2", _cj_estimator_cfg.get("min_kappa_r2", 0.15)))
CJ_ESTIMATOR_EPSILON_FLOOR = float(os.getenv(
    "CJ_ESTIMATOR_EPSILON_FLOOR", _cj_estimator_cfg.get("epsilon_floor", 0.0)))
CJ_ESTIMATOR_EPSILON_CAP = float(os.getenv(
    "CJ_ESTIMATOR_EPSILON_CAP", _cj_estimator_cfg.get("epsilon_cap", 80.0)))
CJ_ESTIMATOR_DEFAULT_LAMBDA = float(os.getenv(
    "CJ_ESTIMATOR_DEFAULT_LAMBDA", _cj_estimator_cfg.get("default_lambda", CJ_LAMBDA)))
CJ_ESTIMATOR_DEFAULT_KAPPA = float(os.getenv(
    "CJ_ESTIMATOR_DEFAULT_KAPPA", _cj_estimator_cfg.get("default_kappa", CJ_KAPPA)))
CJ_ESTIMATOR_DEFAULT_EPSILON = float(os.getenv(
    "CJ_ESTIMATOR_DEFAULT_EPSILON", _cj_estimator_cfg.get("default_epsilon", CJ_EPSILON)))

# Binance alpha config
_alpha_cfg = _trading.get("alpha", {})
ALPHA_SOURCE = os.getenv("ALPHA_SOURCE", _alpha_cfg.get("source", "binance"))
BINANCE_STALE_SECONDS = float(os.getenv("BINANCE_STALE_SECONDS", _alpha_cfg.get("stale_seconds", 5.0)))
BINANCE_OBI_WINDOW = int(os.getenv("BINANCE_OBI_WINDOW", _alpha_cfg.get("window_size", 6000)))
BINANCE_OBI_MIN_SAMPLES = int(os.getenv("BINANCE_OBI_MIN_SAMPLES", _alpha_cfg.get("min_samples", 150)))
BINANCE_OBI_LOOKING_DEPTH = float(os.getenv("BINANCE_OBI_LOOKING_DEPTH", _alpha_cfg.get("looking_depth", 0.025)))
BINANCE_BBO_MIN_SAMPLES = int(os.getenv("BINANCE_BBO_MIN_SAMPLES", _alpha_cfg.get("bbo_min_samples", 10)))
BINANCE_BBO_STALE_SECONDS = float(os.getenv("BINANCE_BBO_STALE_SECONDS", _alpha_cfg.get("bbo_stale_seconds", 5.0)))
BINANCE_DEPTH_SNAPSHOT_LIMIT = int(os.getenv("BINANCE_DEPTH_SNAPSHOT_LIMIT", _alpha_cfg.get("depth_snapshot_limit", 1000)))

# Global events and task refs (not part of state — asyncio primitives)
order_book_received = asyncio.Event()    # legacy: used by startup wait + tests
_book_seq: int = 0                       # monotonic counter, bumped by Lighter book callback
_quote_seq: int = 0                      # bumped by any market-data input that can change quotes
_quote_seq_event = asyncio.Event()       # signaled on every quote-relevant market-data bump


def _bump_quote_signal() -> None:
    global _quote_seq
    _quote_seq += 1
    _quote_seq_event.set()


def _refresh_external_alpha_override() -> None:
    calc = state.vol_obi_state.calculator
    if calc is None:
        return
    if QUOTE_ENGINE == "cartea_jaimungal":
        calc.set_alpha_override(None)
        return
    ba = state.binance_alpha
    if ba is not None and ba.warmed_up and not ba.is_stale(BINANCE_STALE_SECONDS):
        calc.set_alpha_override(ba.alpha)
    else:
        calc.set_alpha_override(None)


def _on_external_alpha_update() -> None:
    _refresh_external_alpha_override()
    _bump_quote_signal()
account_state_received = asyncio.Event()
account_all_received = asyncio.Event()
ws_reconnect_event = asyncio.Event()
ws_client = None
ws_task = None
stale_order_task = None
order_state_task = None
_send_task: Optional[asyncio.Task] = None  # background paced-send task
_latest_ops: Optional[list] = None         # mailbox: latest computed ops (live-mode only)

# Serialize all SDK write operations (create/modify/cancel) to avoid nonce collisions.
# The Lighter SDK's nonce counter is not safe for concurrent async calls.
_sdk_write_lock = asyncio.Lock()

_WS_AUTH_TOKEN_TTL = int(os.getenv(
    "WS_AUTH_TOKEN_TTL",
    _ws.get("auth_token_ttl", 9 * 60)))   # seconds — refresh before 10-min server-side expiry
_account_orders_ws_ready = False
_account_orders_ws_connected = asyncio.Event()
_reconcile_pending_event = asyncio.Event()
RECONCILER_SLOW_INTERVAL_SEC = 60.0

# WS-based cancel confirmation: order_id -> asyncio.Event
_order_cancel_events: dict[int, asyncio.Event] = {}
_pause_cleanup_running = False
QUOTE_TELEMETRY_INTERVAL_SEC = 30.0
QUOTE_TELEMETRY_STALE_AFTER_SEC = 60.0
QUOTA_STUCK_WARNING_COOLDOWN_SEC = 300.0
ORDER_LIFECYCLE_WATCHDOG_INTERVAL_SEC = 2.0
ORDER_PLACING_TIMEOUT_SEC = 30.0

# Dry-run / paper-trading engine (None when live)
_dry_run_engine: Optional['DryRunEngine'] = None
_trade_logger = None  # TradeLogger instance (set in main, used in both modes)
DRY_RUN = False
DRY_RUN_CAPITAL: Optional[float] = None
_cj_estimator: Optional[LighterCJEstimator] = None
_last_cj_refresh: float = 0.0
_last_cj_estimator_ready: bool = False
_last_cj_gate_log: float = 0.0

# =========================
# Logging setup
# =========================
logger = setup_logging(__name__, log_dir=LOG_DIR, log_filename="market_maker_debug.txt")

# Propagate handlers to sub-module loggers so their messages appear in our output
for _sub_logger_name in (
    'binance_obi',
    'vol_obi',
    'ws_manager',
    'orderbook_sanity',
    'cartea_jaimungal',
    'lighter_estimators',
):
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
# Pre-computed spread widening factors per level (avoids per-tick exponentiation)
_SPREAD_FACTORS = [SPREAD_FACTOR_LEVEL1 ** lvl for lvl in range(max(NUM_LEVELS, 1))]


class OrderEventType(str, Enum):
    BIND_LIVE = "BIND_LIVE"
    CLEAR_LIVE = "CLEAR_LIVE"
    CLEAR_ALL = "CLEAR_ALL"
    RECONCILE = "RECONCILE"


@dataclass(slots=True)
class OrderEvent:
    event_type: OrderEventType
    side: str = ""
    level: int = 0
    order_id: int = 0
    price: float = 0.0
    size: float = 0.0
    remote_orders: list = field(default_factory=list)
    source: str = ""


@dataclass(slots=True)
class LiveFillContext:
    side: str
    level: int
    client_order_index: Optional[int]
    exchange_order_index: Optional[int]
    order_price: Optional[float]
    order_size: Optional[float]
    mid_at_fill: Optional[float]
    recorded_at: float
    source: str = "account_orders_ws"


_order_event_queue: deque = deque()
_latest_reconcile_event: Optional[OrderEvent] = None
_pending_trades: deque = deque()  # raw trade batches deferred from WS callback
_pending_live_fill_contexts: deque = deque(maxlen=200)
_processed_account_trade_ids: set[str] = set()
_pending_trades_scheduled = False
_pending_dry_run_fill_check = False
_live_fill_accounting_started = False
_live_fill_position_size = 0.0
_live_fill_entry_vwap = 0.0
_live_fill_realized_pnl = 0.0
_live_fill_count = 0
_live_volume_usd = 0.0
_live_state_store: Optional[LiveStateStore] = None
_live_metrics: Optional[LiveMetricsTracker] = None
_live_fill_seq = 0
_last_quality_adjustment_log = 0.0
_account_trade_accept_after_ms = 0
_last_live_accounting_sync_log = 0.0
_last_inventory_exit_bias_log = 0.0


def _enqueue_order_event(event: OrderEvent) -> None:
    """Route hot slot updates to the queue and reconcile snapshots to latest-only storage."""
    global _latest_reconcile_event
    if event.event_type == OrderEventType.RECONCILE:
        _latest_reconcile_event = event
        _reconcile_pending_event.set()
        return
    _order_event_queue.append(event)


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
        'bids': _BookSide(), 'asks': _BookSide(), 'initialized': False,
        'last_offset': None,
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
    # Precomputed on capital/mid change — read-only in hot loop
    precomputed_base_amount: Optional[float] = None
    precomputed_max_pos_usd: float = 0.0


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
class QuoteTelemetryState:
    updated_at: float = 0.0
    mid: Optional[float] = None
    position_size: float = 0.0
    buy_0: Optional[float] = None
    sell_0: Optional[float] = None
    max_pos_usd: float = 0.0
    quota_remaining: Optional[int] = None
    threshold_bps: float = 0.0


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
    binance_bbo: Optional[SharedBBO] = None


state = MMState()
_quote_telemetry = QuoteTelemetryState()


def _reset_quote_telemetry() -> None:
    global _quote_telemetry
    _quote_telemetry = QuoteTelemetryState()


def _base_cj_params() -> CarteaJaimungalParams:
    solver_mode = CJ_SOLVER_MODE if CJ_SOLVER_MODE in {"symmetric", "asymmetric"} else "asymmetric"
    return CarteaJaimungalParams(
        lambda_plus=CJ_LAMBDA,
        lambda_minus=CJ_LAMBDA,
        kappa_plus=CJ_KAPPA,
        kappa_minus=CJ_KAPPA,
        epsilon_plus=CJ_EPSILON,
        epsilon_minus=CJ_EPSILON,
        sigma2_per_sec=CJ_SIGMA2_PER_SEC,
        alpha=CJ_ALPHA,
        phi=CJ_PHI,
        horizon_seconds=CJ_HORIZON_SECONDS,
        q_max=CJ_Q_MAX,
        spread_multiplier=CJ_SPREAD_MULTIPLIER,
        min_half_spread_bps=CJ_MIN_HALF_SPREAD_BPS,
        max_half_spread_bps=CJ_MAX_HALF_SPREAD_BPS,
        maker_fee_rate=MAKER_FEE_RATE,
        inventory_unit_base=CJ_INVENTORY_UNIT_BASE,
        max_toxicity=CJ_MAX_TOXICITY,
        volatility_spread_multiplier=CJ_VOLATILITY_SPREAD_MULTIPLIER,
        solver_mode=solver_mode,
        asym_n_steps=CJ_ASYM_N_STEPS,
        asym_max_iter=CJ_ASYM_MAX_ITER,
    )


def _blend(base: float, dynamic: float, blend: float) -> float:
    b = min(max(float(blend), 0.0), 1.0)
    return float(base) * (1.0 - b) + float(dynamic) * b


def _dynamic_cj_params(snapshot: CJSnapshot | None) -> CarteaJaimungalParams:
    base = _base_cj_params()
    if snapshot is None or not snapshot.ready:
        return base

    blend = CJ_ESTIMATOR_BLEND
    return CarteaJaimungalParams(
        lambda_plus=_blend(base.lambda_plus, snapshot.lambda_plus * CJ_LAMBDA_SCALE, blend),
        lambda_minus=_blend(base.lambda_minus, snapshot.lambda_minus * CJ_LAMBDA_SCALE, blend),
        kappa_plus=_blend(base.kappa_plus, snapshot.kappa_plus * CJ_KAPPA_SCALE, blend),
        kappa_minus=_blend(base.kappa_minus, snapshot.kappa_minus * CJ_KAPPA_SCALE, blend),
        epsilon_plus=_blend(base.epsilon_plus, snapshot.epsilon_plus * CJ_EPSILON_SCALE, blend),
        epsilon_minus=_blend(base.epsilon_minus, snapshot.epsilon_minus * CJ_EPSILON_SCALE, blend),
        sigma2_per_sec=_blend(base.sigma2_per_sec, snapshot.sigma2_per_sec * CJ_SIGMA2_SCALE, blend),
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


def _refresh_cj_params_if_needed(force: bool = False) -> None:
    global _last_cj_refresh, _last_cj_estimator_ready
    if QUOTE_ENGINE != "cartea_jaimungal":
        return

    calc = state.vol_obi_state.calculator
    update_params = getattr(calc, "update_params", None)
    if calc is None or not callable(update_params):
        return

    now = time.monotonic()
    if not force and now - _last_cj_refresh < max(CJ_REFRESH_SECONDS, 1.0):
        return

    snapshot = _cj_estimator.snapshot() if (CJ_USE_ESTIMATOR and _cj_estimator is not None) else None
    params = _dynamic_cj_params(snapshot)
    try:
        changed = update_params(params)
        _last_cj_refresh = now
        _last_cj_estimator_ready = bool(snapshot.ready) if snapshot is not None else False
        if changed or force:
            if snapshot is not None:
                logger.info(
                    "CJ params refreshed: ready=%s lambda=(%.4f/%.4f) kappa=(%.4f/%.4f) "
                    "epsilon=(%.2f/%.2f) sigma2=%.6f kappa_fit=%.3f",
                    snapshot.ready,
                    params.lambda_plus,
                    params.lambda_minus,
                    params.kappa_plus,
                    params.kappa_minus,
                    params.epsilon_plus,
                    params.epsilon_minus,
                    params.sigma2_per_sec,
                    snapshot.kappa_fit_quality,
                )
            else:
                logger.info(
                    "CJ params refreshed from static defaults: lambda=%.4f kappa=%.4f epsilon=%.2f",
                    params.lambda_plus,
                    params.kappa_plus,
                    params.epsilon_plus,
                )
    except (ValueError, OverflowError) as exc:
        logger.warning("CJ dynamic params rejected; keeping previous surface: %s", exc)


def _cj_estimator_gate_allows_quote() -> bool:
    """Return True when CJ live is allowed to quote."""
    global _last_cj_gate_log
    if QUOTE_ENGINE != "cartea_jaimungal" or not CJ_REQUIRE_ESTIMATOR_READY:
        return True
    if not CJ_USE_ESTIMATOR or _cj_estimator is None:
        return True

    snapshot = _cj_estimator.snapshot()
    if snapshot.ready:
        if not _last_cj_estimator_ready:
            _refresh_cj_params_if_needed(force=True)
        return True

    now = time.monotonic()
    if now - _last_cj_gate_log >= 30.0:
        _last_cj_gate_log = now
        logger.info(
            "CJ estimator gate: waiting for ready snapshot "
            "(trades +/−=%d/%d, markouts +/−=%d/%d, kappa_points +/−=%d/%d)",
            snapshot.trade_count_plus,
            snapshot.trade_count_minus,
            snapshot.markout_count_plus,
            snapshot.markout_count_minus,
            snapshot.kappa_points_plus,
            snapshot.kappa_points_minus,
        )
    return False


def _publish_quote_telemetry(
    *,
    mid: float,
    position_size: float,
    buy_0: Optional[float],
    sell_0: Optional[float],
    max_pos_usd: float,
    quota_remaining: Optional[int],
    threshold_bps: float,
) -> None:
    _quote_telemetry.updated_at = time.monotonic()
    _quote_telemetry.mid = mid
    _quote_telemetry.position_size = position_size
    _quote_telemetry.buy_0 = buy_0
    _quote_telemetry.sell_0 = sell_0
    _quote_telemetry.max_pos_usd = max_pos_usd
    _quote_telemetry.quota_remaining = quota_remaining
    _quote_telemetry.threshold_bps = threshold_bps


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

    # -- Private: direct state mutation (called only by drain_events / dry_run) --

    def _bind_live(self, side: str, order_id: int, price: float, size: float, *, level: int = 0) -> None:
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

    def _clear_live(self, side: str, level: Optional[int] = None) -> None:
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

    def _clear_all(self) -> None:
        self._clear_live("buy")
        self._clear_live("sell")

    # -- Event queue: single-owner drain (the only public mutation API) --

    def drain_hot_events(self) -> None:
        """Process only hot order-slot mutations needed by quote/send decisions."""
        while _order_event_queue:
            evt = _order_event_queue.popleft()
            if evt.event_type == OrderEventType.BIND_LIVE:
                self._bind_live(evt.side, evt.order_id, evt.price, evt.size, level=evt.level)
            elif evt.event_type == OrderEventType.CLEAR_LIVE:
                self._clear_live(evt.side, level=evt.level)
            elif evt.event_type == OrderEventType.CLEAR_ALL:
                self._clear_all()

    def drain_reconcile_events(self) -> bool:
        """Process the latest pending reconcile snapshot, if any."""
        global _latest_reconcile_event
        evt = _latest_reconcile_event
        if evt is None:
            _reconcile_pending_event.clear()
            return False
        _latest_reconcile_event = None
        self._process_reconcile(evt.remote_orders, evt.source)
        if _latest_reconcile_event is None:
            _reconcile_pending_event.clear()
        return True

    def drain_events(self) -> None:
        """Process all pending order events for compatibility with tests/cold paths."""
        self.drain_hot_events()
        while self.drain_reconcile_events():
            self.drain_hot_events()

    def _process_reconcile(self, remote_orders: list, source: str) -> None:
        """Apply a full reconcile snapshot atomically (clear stale + refresh)."""
        _update_id_mapping_from_orders(remote_orders)
        live_client_ids = _orders_to_live_client_id_set(remote_orders)
        orders = state.orders
        for lvl in range(NUM_LEVELS):
            bid_id = orders.bid_order_ids[lvl]
            if bid_id is not None and bid_id not in live_client_ids:
                self._clear_live("buy", lvl)
            ask_id = orders.ask_order_ids[lvl]
            if ask_id is not None and ask_id not in live_client_ids:
                self._clear_live("sell", lvl)
        # Refresh price/size for tracked orders (direct, not enqueued)
        remote_by_client: dict[int, dict] = {}
        for order in remote_orders:
            cid = _extract_client_order_index(order)
            if cid is not None:
                remote_by_client[cid] = order
        for level in range(NUM_LEVELS):
            bid_id = orders.bid_order_ids[level]
            if bid_id is not None and bid_id in remote_by_client:
                self._sync_from_remote("buy", level, remote_by_client[bid_id])
            ask_id = orders.ask_order_ids[level]
            if ask_id is not None and ask_id in remote_by_client:
                self._sync_from_remote("sell", level, remote_by_client[ask_id])

    def _sync_from_remote(self, side: str, level: int, order: dict) -> None:
        """Direct bind from exchange data (used inside drain_events only)."""
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
            self.mark_status(side, SideStatus.LIVE, level=level)
            return
        self._bind_live(side, cid, price, size, level=level)


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
    if _price_change_bps_c is not None:
        return _price_change_bps_c(old_price, new_price)
    return abs(new_price - old_price) / old_price * 10000.0


def get_position_value_usd(position_size: float, mid_price: Optional[float]) -> float:
    if mid_price is None:
        return 0.0
    return abs(position_size) * mid_price


def _dynamic_max_position_dollar(mid_price: float, capital: float = None, base_amount: float = None) -> float:
    """Compute max position dollar from live capital, leverage, and order size."""
    if capital is None:
        capital = state.account.available_capital
    if capital is None or capital <= 0 or mid_price is None or mid_price <= 0:
        return 0.0  # Can't compute — suppress all quoting (safe default)
    if _dynamic_max_position_c is not None and base_amount is not None:
        return _dynamic_max_position_c(mid_price, capital, LEVERAGE, base_amount, NUM_LEVELS)
    raw = capital * LEVERAGE
    if base_amount is not None and base_amount > 0:
        order_usd = base_amount * mid_price
        raw -= 2.0 * NUM_LEVELS * order_usd  # room for all order levels (both sides)
    return max(0.0, raw * 0.9)  # 10% safety margin


def _recompute_derived_params(mid_price: Optional[float] = None) -> None:
    """Recompute base_amount and max_position from current capital/mid.

    Called on capital update (user_stats WS) and on significant mid-price
    change (orderbook WS).  Results are stored as read-only snapshots for
    the hot loop.
    """
    if mid_price is None:
        mid_price = state.market.mid_price
    capital = state.account.available_capital
    base_amount = calculate_dynamic_base_amount(mid_price, capital=capital)
    max_pos = _dynamic_max_position_dollar(mid_price, capital, base_amount)
    state.account.precomputed_base_amount = base_amount
    state.account.precomputed_max_pos_usd = max_pos
    # Push to vol_obi calculator if max_pos changed
    calc = state.vol_obi_state.calculator
    if calc is not None and max_pos > 0:
        set_max_position = getattr(calc, "set_max_position_dollar", None)
        if callable(set_max_position):
            set_max_position(max_pos)
        set_inventory_unit = getattr(calc, "set_inventory_unit_base", None)
        if callable(set_inventory_unit) and base_amount is not None and base_amount > 0:
            set_inventory_unit(base_amount)


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


def _non_actionable_close_reason(position_size: float, reference_price: Optional[float]) -> Optional[str]:
    """Return why a position cannot be closed without violating exchange minima."""
    close_size = abs(position_size)
    if close_size < EPSILON:
        return None

    reasons = []
    min_base = state.config.min_base_amount
    min_quote = max(float(state.config.min_quote_amount or 0.0), float(MIN_ORDER_VALUE_USD or 0.0))
    amount_tick = state.config.amount_tick_float
    quote_value = close_size * reference_price if reference_price and reference_price > 0 else None

    if amount_tick > 0 and close_size + EPSILON < amount_tick:
        reasons.append(f"size {close_size:.8f} < amount_tick {amount_tick:.8f}")
    if min_base > 0 and close_size + EPSILON < min_base:
        reasons.append(f"size {close_size:.8f} < min_base {min_base:.8f}")
    if min_quote > 0 and quote_value is not None and quote_value + EPSILON < min_quote:
        reasons.append(f"notional ${quote_value:.2f} < min_order_value ${min_quote:.2f}")

    return "; ".join(reasons) if reasons else None


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
    if API_KEY_MAKER_ONLY is True:
        logger.error(
            "emergency_close (%s): skipped because API key %d is maker-only; "
            "only post-only orders, modify, cancel, and cancel-all are allowed",
            reason,
            API_KEY_INDEX,
        )
        return False

    best_bid, best_ask = get_best_prices()
    if best_bid is None or best_ask is None or mid is None:
        logger.error("emergency_close (%s): no orderbook data to close position %.6f", reason, pos)
        return False

    non_actionable_reason = _non_actionable_close_reason(pos, mid)
    if non_actionable_reason is not None:
        logger.info(
            "emergency_close (%s): skipping non-actionable dust position %.8f ($%.2f): %s",
            reason, pos, abs(pos) * mid, non_actionable_reason,
        )
        return True

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
# "Default" tx-type limit: 40 ops per rolling 60s window (binding constraint).
# NOTE: This matches the documented default per-transaction-type limit.
# Premium accounts also have a separate weighted limit (4,000+ sendTx/min).
# If sendTxBatch counts as 1 request regardless of op count, the effective
# limit may be higher.  Verify with exchange support before increasing.
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
_last_backoff_trigger_time: float = 0.0

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
    # Prevent unbounded growth — always preserve currently tracked orders
    if len(_client_to_exchange_id) > 200:
        live_ids = set()
        orders = state.orders
        for lvl in range(NUM_LEVELS):
            for oid in (orders.bid_order_ids[lvl], orders.ask_order_ids[lvl]):
                if oid is not None:
                    live_ids.add(oid)
        to_keep = {k: v for k, v in _client_to_exchange_id.items() if k in live_ids}
        remaining = {k: v for k, v in sorted(_client_to_exchange_id.items())[-100:]
                     if k not in to_keep}
        to_keep.update(remaining)
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


def _extract_order_fill_size(order: dict, side: str, level: int) -> Optional[float]:
    """Best-effort filled size for a dead order update.

    Filled orders often report ``remaining_base_amount=0``.  For logging we
    prefer initial/original size fields, then fall back to the bot's local slot
    size before that slot is cleared.
    """
    for key in ("initial_base_amount", "original_base_amount", "base_amount", "size", "amount"):
        raw = order.get(key)
        try:
            size = float(raw)
            if size > 0:
                return size
        except (TypeError, ValueError):
            pass

    local_sizes = state.orders.bid_sizes if side == "buy" else state.orders.ask_sizes
    if 0 <= level < len(local_sizes):
        try:
            size = float(local_sizes[level])
            if size > 0:
                return size
        except (TypeError, ValueError):
            pass
    return None


def _record_live_fill_context(side: str, level: int, order: dict) -> None:
    """Remember side/slot metadata so account_all trades can be logged properly."""
    cid = _extract_client_order_index(order)
    eid = _extract_order_index(order)
    local_prices = state.orders.bid_prices if side == "buy" else state.orders.ask_prices
    order_price = _extract_order_price(order)
    if order_price is None and 0 <= level < len(local_prices):
        try:
            order_price = float(local_prices[level])
        except (TypeError, ValueError):
            order_price = None
    order_size = _extract_order_fill_size(order, side, level)
    ctx = LiveFillContext(
        side=side,
        level=level,
        client_order_index=cid,
        exchange_order_index=eid,
        order_price=order_price,
        order_size=order_size,
        mid_at_fill=state.market.mid_price,
        recorded_at=time.monotonic(),
    )
    _pending_live_fill_contexts.append(ctx)


def _current_market_position_payload() -> Optional[dict]:
    positions = state.account.positions
    if not isinstance(positions, dict) or state.config.market_id is None:
        return None
    return positions.get(str(state.config.market_id)) or positions.get(state.config.market_id)


def _extract_position_entry_vwap() -> Optional[float]:
    position = _current_market_position_payload()
    if not isinstance(position, dict):
        return None
    for key in ("avg_entry_price", "entry_price", "average_entry_price"):
        raw = position.get(key)
        try:
            value = float(raw)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    return None


def _live_state_payload() -> dict:
    return {
        "account_index": ACCOUNT_INDEX,
        "market_id": state.config.market_id,
        "position_size_est": _live_fill_position_size,
        "entry_vwap": _live_fill_entry_vwap,
        "realized_pnl_cumulative": _live_fill_realized_pnl,
        "fill_count": _live_fill_count,
        "volume_usd": _live_volume_usd,
        "exchange_position_size": state.account.position_size,
        "exchange_entry_vwap": _extract_position_entry_vwap(),
        "portfolio_value": state.account.portfolio_value,
        "available_capital": state.account.available_capital,
    }


def _persist_live_state() -> None:
    if _live_state_store is None:
        return
    try:
        _live_state_store.save(_live_state_payload())
    except OSError as exc:
        logger.warning("Could not persist live state: %s", exc)


def _restore_live_state_defaults() -> tuple[float, int, float]:
    if _live_state_store is None:
        return 0.0, 0, 0.0
    payload = _live_state_store.load()
    if not payload and _trade_logger is not None:
        path = getattr(_trade_logger, "path", None)
        if path:
            try:
                with open(path, newline="") as f:
                    rows = list(csv.DictReader(f))
                if rows:
                    last = rows[-1]
                    payload = {
                        "realized_pnl_cumulative": last.get("realized_pnl_cumulative") or last.get("realized_pnl"),
                        "fill_count": len(rows),
                        "volume_usd": sum(float(row.get("notional_usd") or 0.0) for row in rows),
                    }
                    logger.info(
                        "Bootstrapped live state from trade log: fills=%d volume=$%.2f realized_cum=%s",
                        payload["fill_count"],
                        payload["volume_usd"],
                        payload["realized_pnl_cumulative"],
                    )
            except (OSError, ValueError, KeyError) as exc:
                logger.warning("Could not bootstrap live state from trade log: %s", exc)
    realized = payload.get("realized_pnl_cumulative", payload.get("realized_pnl", 0.0))
    fill_count = payload.get("fill_count", 0)
    volume = payload.get("volume_usd", 0.0)
    try:
        realized_value = float(realized or 0.0)
    except (TypeError, ValueError):
        realized_value = 0.0
    try:
        fill_count_value = int(fill_count or 0)
    except (TypeError, ValueError):
        fill_count_value = 0
    try:
        volume_value = float(volume or 0.0)
    except (TypeError, ValueError):
        volume_value = 0.0
    return realized_value, max(fill_count_value, 0), max(volume_value, 0.0)


def _fill_identity_for_metrics(trade: dict, client_order_index=None, exchange_order_index=None) -> str:
    global _live_fill_seq
    trade_identity = _account_trade_identity(trade)
    if trade_identity is not None:
        return trade_identity
    if exchange_order_index is not None:
        return f"{state.config.market_id}:exchange_order:{exchange_order_index}:{trade.get('price')}:{trade.get('size')}"
    if client_order_index is not None:
        return f"{state.config.market_id}:client_order:{client_order_index}:{trade.get('price')}:{trade.get('size')}"
    _live_fill_seq += 1
    return f"{state.config.market_id}:local_seq:{_live_fill_seq}"


def _trade_timestamp_ms(trade: dict) -> Optional[int]:
    for key in ("timestamp", "time", "created_at", "executed_at"):
        raw = trade.get(key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue
        if value > 1_000_000_000_000_000:
            value /= 1_000.0  # micros -> millis
        elif value < 10_000_000_000:
            value *= 1_000.0  # seconds -> millis
        return int(value)
    return None


def _is_startup_trade_echo(trade: dict) -> bool:
    trade_ts_ms = _trade_timestamp_ms(trade)
    return (
        _account_trade_accept_after_ms > 0
        and trade_ts_ms is not None
        and trade_ts_ms < _account_trade_accept_after_ms
    )


def _sync_live_accounting_to_exchange(reason: str) -> bool:
    """Keep observability accounting aligned with the authoritative exchange position."""
    global _live_fill_position_size, _live_fill_entry_vwap, _last_live_accounting_sync_log

    if not _live_fill_accounting_started or _dry_run_engine is not None:
        return False

    exchange_pos = state.account.position_size
    tolerance = max(EPSILON, state.config.amount_tick_float * 0.5)
    if abs(exchange_pos - _live_fill_position_size) <= tolerance:
        return False

    old_pos = _live_fill_position_size
    old_entry = _live_fill_entry_vwap
    exchange_entry = _extract_position_entry_vwap()
    if abs(exchange_pos) < EPSILON:
        _live_fill_position_size = 0.0
        _live_fill_entry_vwap = 0.0
    else:
        _live_fill_position_size = exchange_pos
        if exchange_entry is not None and exchange_entry > 0:
            _live_fill_entry_vwap = exchange_entry
        elif old_pos * exchange_pos > 0 and old_entry > 0:
            _live_fill_entry_vwap = old_entry
        else:
            _live_fill_entry_vwap = state.market.mid_price or 0.0

    now = time.monotonic()
    if now - _last_live_accounting_sync_log >= 30.0:
        logger.warning(
            "Live fill accounting resynced to exchange position (%s): local %.8f -> exchange %.8f",
            reason,
            old_pos,
            exchange_pos,
        )
        _last_live_accounting_sync_log = now
    _persist_live_state()
    return True


def _initialize_live_fill_accounting_from_account() -> None:
    """Seed local fill accounting from the exchange position snapshot."""
    global _live_fill_accounting_started
    global _live_fill_position_size, _live_fill_entry_vwap, _live_fill_realized_pnl
    global _live_fill_count, _live_volume_usd

    position_size = state.account.position_size
    entry_vwap = _extract_position_entry_vwap()
    if entry_vwap is None:
        entry_vwap = state.market.mid_price or 0.0
    restored_realized, restored_count, restored_volume = _restore_live_state_defaults()

    _live_fill_accounting_started = True
    _live_fill_position_size = position_size
    _live_fill_entry_vwap = entry_vwap if abs(position_size) >= EPSILON else 0.0
    _live_fill_realized_pnl = restored_realized
    _live_fill_count = restored_count
    _live_volume_usd = restored_volume

    if abs(position_size) >= EPSILON:
        logger.info(
            "Live fill accounting initialized from exchange snapshot: pos=%.8f entry_vwap=%.8g realized_cum=$%.6f",
            position_size,
            _live_fill_entry_vwap,
            _live_fill_realized_pnl,
        )
    else:
        logger.info(
            "Live fill accounting initialized flat: restored realized_cum=$%.6f fills=%d volume=$%.2f",
            _live_fill_realized_pnl,
            _live_fill_count,
            _live_volume_usd,
        )
    _persist_live_state()


def _match_live_fill_context(price: float, size: float) -> Optional[LiveFillContext]:
    """Match a trade payload to the latest account_orders fill context."""
    if not _pending_live_fill_contexts:
        return None

    now = time.monotonic()
    fresh_contexts = [ctx for ctx in _pending_live_fill_contexts if now - ctx.recorded_at <= 300]
    if len(fresh_contexts) != len(_pending_live_fill_contexts):
        _pending_live_fill_contexts.clear()
        _pending_live_fill_contexts.extend(fresh_contexts)
    if not fresh_contexts:
        return None

    price_tick = state.config.price_tick_float if state.config.price_tick_float > 0 else 0.0
    amount_tick = state.config.amount_tick_float if state.config.amount_tick_float > 0 else 0.0
    price_tol = max(price_tick * 3, abs(price) * 0.00003, 1e-9)
    size_tol = max(amount_tick * 3, abs(size) * 0.05, 1e-12)
    best_idx = None
    best_score = float("inf")
    for idx, ctx in enumerate(_pending_live_fill_contexts):
        score = now - ctx.recorded_at
        if ctx.order_price is not None:
            price_diff = abs(ctx.order_price - price)
            if price_diff > price_tol:
                continue
            score += price_diff / max(price_tol, 1e-9)
        if ctx.order_size is not None:
            size_diff = abs(ctx.order_size - size)
            if size_diff > size_tol:
                continue
            score += size_diff / max(size_tol, 1e-12)
        if score < best_score:
            best_score = score
            best_idx = idx

    if best_idx is None:
        return None

    contexts = list(_pending_live_fill_contexts)
    ctx = contexts.pop(best_idx)
    _pending_live_fill_contexts.clear()
    _pending_live_fill_contexts.extend(contexts)
    return ctx


def _boolish(value) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _account_trade_identity(trade: dict) -> Optional[str]:
    for key in ("trade_id", "id", "tx_hash"):
        value = trade.get(key)
        if value is not None:
            return f"{trade.get('market_id', state.config.market_id)}:{key}:{value}"
    ts = _trade_timestamp_ms(trade)
    price = trade.get("price")
    size = trade.get("size")
    side_hint = trade.get("side") or trade.get("type") or trade.get("is_maker_ask")
    if ts is not None and price is not None and size is not None and side_hint is not None:
        return f"{trade.get('market_id', state.config.market_id)}:synthetic:{ts}:{side_hint}:{price}:{size}"
    return None


def _side_from_account_trade(trade: dict) -> str:
    raw_side = str(trade.get("side") or "").strip().lower()
    if raw_side in {"buy", "sell"}:
        return raw_side

    raw_type = str(trade.get("type") or "").strip().lower()
    if raw_type in {"buy", "sell"}:
        return raw_type

    maker_ask = _boolish(trade.get("is_maker_ask"))
    if maker_ask is not None:
        # This is our account trade. If our resting order was the maker ask,
        # our fill is a sell; maker bid means our fill is a buy.
        return "sell" if maker_ask else "buy"

    return "unknown"


def _apply_live_fill_accounting(side: str, price: float, size: float) -> tuple[float, float, float, float, float]:
    """Update local realized-PnL estimate for live fills.

    Returns ``position_after_est, realized_delta, realized_cumulative,
    entry_vwap_after, fee_usd``.  This is only observability; exchange portfolio
    value remains authoritative.
    """
    global _live_fill_accounting_started
    global _live_fill_position_size, _live_fill_entry_vwap, _live_fill_realized_pnl
    global _live_fill_count, _live_volume_usd

    signed_fill = size if side == "buy" else -size
    fee_usd = abs(price * size * MAKER_FEE_RATE)

    if not _live_fill_accounting_started:
        _live_fill_accounting_started = True
        _live_fill_position_size = 0.0
        _live_fill_entry_vwap = 0.0
        _live_fill_realized_pnl = 0.0

    pos = _live_fill_position_size
    vwap = _live_fill_entry_vwap
    realized_delta = -fee_usd

    if abs(pos) < EPSILON:
        new_pos = signed_fill
        new_vwap = price if abs(new_pos) >= EPSILON else 0.0
    elif pos * signed_fill > 0:
        new_abs = abs(pos) + abs(signed_fill)
        new_pos = pos + signed_fill
        new_vwap = ((abs(pos) * vwap) + (abs(signed_fill) * price)) / new_abs
    else:
        closing_size = min(abs(pos), abs(signed_fill))
        if pos > 0 and side == "sell":
            realized_delta += (price - vwap) * closing_size
        elif pos < 0 and side == "buy":
            realized_delta += (vwap - price) * closing_size

        new_pos = pos + signed_fill
        if abs(new_pos) < EPSILON:
            new_pos = 0.0
            new_vwap = 0.0
        elif pos * new_pos > 0:
            new_vwap = vwap
        else:
            new_vwap = price

    _live_fill_position_size = new_pos
    _live_fill_entry_vwap = new_vwap
    _live_fill_realized_pnl += realized_delta
    _live_fill_count += 1
    _live_volume_usd += abs(price * size)
    _persist_live_state()
    return new_pos, realized_delta, _live_fill_realized_pnl, new_vwap, fee_usd


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


def _is_reducing_side(side: str, position_size: float) -> bool:
    return (position_size > EPSILON and side == "sell") or (
        position_size < -EPSILON and side == "buy"
    )


def _sync_tracked_order_from_remote(side: str, level: int, order: dict) -> None:
    """Enqueue a BIND_LIVE event to refresh a tracked order from exchange data."""
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
    _enqueue_order_event(OrderEvent(
        event_type=OrderEventType.BIND_LIVE,
        side=side, level=level,
        order_id=cid, price=price, size=size,
    ))


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

    Stores a coalesced RECONCILE snapshot for deferred cold-path processing.
    Returns ``(ok, unknown_exchange_ids)`` computed against *projected* state
    (what state will look like after reconcile is applied).
    """
    # Populate the client_order_index -> order_index mapping (no order-state mutation)
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

    # Compute projected tracked IDs (IDs that will survive after reconcile)
    projected_tracked: set[int] = set()
    for lvl in range(NUM_LEVELS):
        bid_id = orders.bid_order_ids[lvl]
        if bid_id is not None:
            if bid_id not in live_client_ids:
                mismatch_reasons.append(f"missing_local_bid[{lvl}]:{bid_id}")
            else:
                projected_tracked.add(bid_id)
        ask_id = orders.ask_order_ids[lvl]
        if ask_id is not None:
            if ask_id not in live_client_ids:
                mismatch_reasons.append(f"missing_local_ask[{lvl}]:{ask_id}")
            else:
                projected_tracked.add(ask_id)

    # Coalesce reconcile snapshots — actual clear/refresh happens in the
    # cold reconcile task (or drain_events() in tests/cold paths).
    _enqueue_order_event(OrderEvent(
        event_type=OrderEventType.RECONCILE,
        remote_orders=remote_orders,
        source=source,
    ))

    unknown_client_ids = live_client_ids - projected_tracked
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
            await asyncio.sleep(0)  # yield to let hot-path callbacks run
            if not ok and risk_controller.mismatch_streak >= max(1, STALE_ORDER_DEBOUNCE_COUNT):
                risk_controller.trigger_pause(
                    f"order reconciliation mismatch for {risk_controller.mismatch_streak} polls"
                )
        except asyncio.CancelledError:
            raise
        except (requests.RequestException, ValueError, KeyError, OSError) as exc:
            logger.error("Unexpected stale-order poller error: %s", exc, exc_info=True)


def on_order_book_update(market_id, payload, is_snapshot_hint=None):
    ob = state.market.local_order_book
    global _pending_dry_run_fill_check
    try:
        if market_id == state.config.market_id:
            bids_in = payload.get('bids', [])
            asks_in = payload.get('asks', [])
            offset = payload.get('offset')

            # Offset sanity: Lighter offsets are a server-side sequence that
            # advances by arbitrary steps between coalesced deltas (verified
            # against live feed 2026-06-10), so forward jumps are normal.
            # Only a NON-advancing offset on a delta is anomalous (stale or
            # out-of-order replay) — skip it rather than corrupt the book.
            last_offset = ob.get('last_offset')
            if (offset is not None and last_offset is not None
                    and not is_snapshot_hint and ob['initialized']
                    and offset <= last_offset):
                logger.warning(
                    "Orderbook stale/out-of-order delta for market %d: "
                    "offset %d <= last %d; skipping message",
                    market_id, offset, last_offset,
                )
                return
            if offset is not None:
                ob['last_offset'] = offset

            is_snapshot = apply_orderbook_update(
                ob['bids'],
                ob['asks'],
                ob['initialized'],
                bids_in,
                asks_in,
                is_snapshot_hint=is_snapshot_hint,
            )
            if is_snapshot:
                ob['initialized'] = True
                logger.info("Initializing/snapshot local orderbook for market %d", market_id)
                # Note: vol_obi calculator is reset on WS *disconnect* (_on_disconnect),
                # NOT here. In-connection snapshots (server refreshes) should not discard
                # accumulated volatility/OBI data.

            # Calculate mid price using O(1) peek
            state.market.ws_connection_healthy = True
            state.market.last_order_book_update = time.monotonic()

            if ob['bids'] and ob['asks']:
                best_bid = ob['bids'].peekitem(-1)[0]
                best_ask = ob['asks'].peekitem(0)[0]
                mid = (best_bid + best_ask) / 2
                prev_mid = state.market.mid_price
                state.market.mid_price = mid

                # Recompute derived params when mid changes materially (~$10 move)
                if prev_mid is None or round(mid, -1) != round(prev_mid, -1):
                    _recompute_derived_params(mid)

                if _cj_estimator is not None:
                    _cj_estimator.on_book_update(mid)

                # Feed vol_obi calculator on every book update (hot path)
                calc = state.vol_obi_state.calculator
                if calc is not None:
                    _refresh_external_alpha_override()
                    calc.on_book_update(mid, ob['bids'], ob['asks'])

                if _dry_run_engine is not None and not _pending_dry_run_fill_check:
                    _pending_dry_run_fill_check = True
                    asyncio.get_event_loop().call_soon(_deferred_check_fills)

                order_book_received.set()
                global _book_seq
                _book_seq += 1
                _bump_quote_signal()
            else:
                # Book is one-sided — clear stale mid_price to prevent
                # the trading loop from quoting at an outdated level.
                state.market.mid_price = None

    except (KeyError, IndexError, ValueError, TypeError, ZeroDivisionError) as e:
        logger.error(f"Error in order book callback: {e}", exc_info=True)
        state.market.ws_connection_healthy = False
        state.market.mid_price = None
        ob = state.market.local_order_book
        ob['initialized'] = False
        ob['last_offset'] = None
        ob['bids'].clear()
        ob['asks'].clear()

async def subscribe_to_market_data(market_id):
    """Connects to the websocket, subscribes to orderbook updates."""

    def _on_disconnect():
        state.market.ws_connection_healthy = False
        state.market.mid_price = None
        ob = state.market.local_order_book
        ob['initialized'] = False
        ob['last_offset'] = None
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
                payload = data['order_book']
                # Offset may sit on the envelope instead of the payload
                if 'offset' not in payload and 'offset' in data:
                    payload['offset'] = data['offset']
                on_order_book_update(
                    market_id, payload,
                    is_snapshot_hint=(msg_type == "subscribed/order_book"),
                )

    await ws_subscribe_fast(
        channels=[f"order_book/{market_id}"],
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

    await ws_subscribe_fast(
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


def on_public_trade_update(market_id, data):
    """Feed public Lighter trades into the CJ estimator."""
    if _cj_estimator is None or market_id != state.config.market_id:
        return
    try:
        trades = data.get("trades") or []
        if not isinstance(trades, list):
            return
        mid = state.market.mid_price
        for trade in trades:
            if isinstance(trade, dict):
                _cj_estimator.on_trade(trade, mid)
    except (ValueError, TypeError) as exc:
        logger.warning("Error processing public trade update: %s", exc)


async def subscribe_to_public_trades(market_id):
    """Subscribe to public Lighter trades for CJ lambda/kappa/epsilon estimation."""

    def _on_message(data):
        msg_type = data.get("type", "")
        if "trade" in msg_type:
            on_public_trade_update(market_id, data)

    await ws_subscribe_fast(
        channels=[f"trade/{market_id}"],
        label="public trades",
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
                if _dry_run_engine is None or not _dry_run_engine.initialized:
                    state.account.available_capital = new_available_capital
                    state.account.portfolio_value = new_portfolio_value
                state.account.last_capital_update = time.monotonic()
                _recompute_derived_params()
                logger.info(
                    f"Received user stats for account {account_id}: "
                    f"Available Capital=${state.account.available_capital}, Portfolio Value=${state.account.portfolio_value}"
                )
                _persist_live_state()
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
    global _pending_trades_scheduled
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
                        if _dry_run_engine is None:
                            state.account.position_size = new_size
                else:
                    logger.warning("Ignoring malformed account_all positions payload: %r", raw_positions)

            # Defer trade sort/dedup/log to next event-loop tick so the WS
            # callback returns promptly and doesn't block market-data ingestion.
            new_trades_by_market = data.get("trades", {}) if isinstance(data, dict) else {}
            if new_trades_by_market:
                _pending_trades.append(new_trades_by_market)
                if not _pending_trades_scheduled:
                    _pending_trades_scheduled = True
                    asyncio.get_event_loop().call_soon(_process_pending_trades)
            elif positions_updated:
                _sync_live_accounting_to_exchange("account_all_position_update")

            if positions_updated and not account_all_received.is_set():
                account_all_received.set()

    except (ValueError, TypeError) as e:
        logger.error(f"Error processing account_all update: {e}", exc_info=True)


def _process_pending_trades() -> None:
    """Drain deferred trade batches — sort, dedup, log.  Runs via call_soon."""
    global _pending_trades_scheduled
    try:
        while _pending_trades:
            new_trades_by_market = _pending_trades.popleft()
            all_new_trades = [trade for trades in new_trades_by_market.values() for trade in trades]
            all_new_trades.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            for trade in reversed(all_new_trades):
                trade_market_id = trade.get("market_id", state.config.market_id)
                if str(trade_market_id) != str(state.config.market_id):
                    continue
                trade_identity = _account_trade_identity(trade)
                if (
                    (trade_identity is not None and trade_identity in _processed_account_trade_ids)
                    or trade in state.account.recent_trades
                ):
                    continue
                if _is_startup_trade_echo(trade):
                    if trade_identity is not None:
                        _processed_account_trade_ids.add(trade_identity)
                    logger.info(
                        "Skipping startup account_all trade echo: market=%s side=%s size=%s price=%s ts=%s",
                        trade.get("market_id"),
                        trade.get("side") or trade.get("type") or trade.get("is_maker_ask"),
                        trade.get("size"),
                        trade.get("price"),
                        trade.get("timestamp") or trade.get("time"),
                    )
                    continue
                if trade_identity is not None:
                    _processed_account_trade_ids.add(trade_identity)
                    if len(_processed_account_trade_ids) > 50_000:
                        _processed_account_trade_ids.clear()
                        _processed_account_trade_ids.add(trade_identity)
                state.account.recent_trades.append(trade)
                price = float(trade.get("price", 0) or 0)
                size = float(trade.get("size", 0) or 0)
                fill_context = _match_live_fill_context(price, size)
                raw_side = _side_from_account_trade(trade)
                side = fill_context.side if fill_context is not None else raw_side
                if side not in {"buy", "sell"}:
                    side = "unknown"

                position_after = state.account.position_size
                realized_delta = 0.0
                realized_cumulative = None
                entry_vwap_after = None
                fee_usd = None
                mid_at_fill = fill_context.mid_at_fill if fill_context is not None else state.market.mid_price
                spread_capture_bps = None
                client_order_index = fill_context.client_order_index if fill_context is not None else None
                exchange_order_index = fill_context.exchange_order_index if fill_context is not None else None
                fill_source = fill_context.source if fill_context is not None else "account_all_ws"

                if side in {"buy", "sell"} and price > 0 and size > 0:
                    position_est, realized_delta, realized_cumulative, entry_vwap_after, fee_usd = (
                        _apply_live_fill_accounting(side, price, size)
                    )
                    if abs(position_after) < EPSILON and abs(position_est) >= EPSILON:
                        position_after = position_est
                    if mid_at_fill is not None and mid_at_fill > 0:
                        if side == "buy":
                            spread_capture_bps = (mid_at_fill - price) / mid_at_fill * 10_000
                        else:
                            spread_capture_bps = (price - mid_at_fill) / mid_at_fill * 10_000

                logger.info(
                    "WebSocket trade update: Market %s, Side %s, Size %s, Price %s, "
                    "realized_delta=$%.6f, realized_cum=%s",
                    trade.get("market_id"),
                    side,
                    trade.get("size"),
                    trade.get("price"),
                    realized_delta,
                    "n/a" if realized_cumulative is None else f"${realized_cumulative:.6f}",
                )
                if _trade_logger is not None and _dry_run_engine is None:
                    fill_id = _fill_identity_for_metrics(trade, client_order_index, exchange_order_index)
                    _trade_logger.log_fill(
                        side=side,
                        price=price,
                        size=size,
                        level=fill_context.level if fill_context is not None else 0,
                        position_after=position_after,
                        realized_pnl=realized_delta,
                        available_capital=state.account.available_capital or 0.0,
                        portfolio_value=state.account.portfolio_value or 0.0,
                        simulated=False,
                        notional_usd=price * size,
                        fee_usd=fee_usd,
                        entry_vwap_after=entry_vwap_after,
                        realized_pnl_cumulative=realized_cumulative,
                        mid_at_fill=mid_at_fill,
                        spread_capture_bps=spread_capture_bps,
                        inventory_after_usd=position_after * (mid_at_fill or price),
                        client_order_index=client_order_index,
                        exchange_order_index=exchange_order_index,
                        fill_source=fill_source,
                    )
                    if _live_metrics is not None:
                        _live_metrics.record_fill(
                            fill_id=fill_id,
                            side=side,
                            price=price,
                            size=size,
                            mid_at_fill=mid_at_fill,
                            spread_capture_bps=spread_capture_bps,
                            position_after=position_after,
                            realized_delta_usd=realized_delta,
                            realized_pnl_cumulative=realized_cumulative,
                            fill_source=fill_source,
                            client_order_index=client_order_index,
                            exchange_order_index=exchange_order_index,
                        )
    finally:
        _sync_live_accounting_to_exchange("post_account_all_trade_batch")
        _pending_trades_scheduled = False
        if _pending_trades:
            _pending_trades_scheduled = True
            asyncio.get_event_loop().call_soon(_process_pending_trades)


def _deferred_check_fills() -> None:
    """Run dry-run fill simulation deferred from the WS callback via call_soon."""
    global _pending_dry_run_fill_check
    try:
        if _dry_run_engine is not None:
            ob = state.market.local_order_book
            if ob['bids'] and ob['asks']:
                _dry_run_engine.check_fills(ob['bids'], ob['asks'])
    finally:
        _pending_dry_run_fill_check = False


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

            # Reuse the reconciler: clears stale local orders, refreshes
            # tracked ones, AND detects/surfaces orphaned exchange orders.
            ok, unknown_ids = _reconcile_local_orders_with_remote_orders(
                live_orders, source="account_orders_ws_snapshot"
            )
            if unknown_ids:
                logger.warning(
                    "WS snapshot: %d orphaned orders detected: %s — will be cancelled by reconciler",
                    len(unknown_ids), sorted(unknown_ids),
                )
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
                is_fill = str(status).lower() in {"filled", "partial_filled"}
                # Order is dead (filled / cancelled / expired) — enqueue clear
                for lvl in range(NUM_LEVELS):
                    if orders.bid_order_ids[lvl] == cid:
                        logger.info("Bid[%d] %d status=%s — enqueue clear", lvl, cid, status)
                        if is_fill:
                            _record_live_fill_context("buy", lvl, o)
                        _enqueue_order_event(OrderEvent(
                            event_type=OrderEventType.CLEAR_LIVE,
                            side="buy", level=lvl,
                        ))
                    if orders.ask_order_ids[lvl] == cid:
                        logger.info("Ask[%d] %d status=%s — enqueue clear", lvl, cid, status)
                        if is_fill:
                            _record_live_fill_context("sell", lvl, o)
                        _enqueue_order_event(OrderEvent(
                            event_type=OrderEventType.CLEAR_LIVE,
                            side="sell", level=lvl,
                        ))
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
            logger.info("account_orders: refreshing auth token (%d-min TTL)",
                        _WS_AUTH_TOKEN_TTL // 60)
            # Reconcile to catch any updates lost during token refresh
            try:
                await reconcile_orders_with_exchange(client, source="token_refresh")
            except Exception as exc:
                logger.warning("Post-token-refresh reconciliation failed: %s", exc)
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
    _quote_seq_event.clear()
    state.market.mid_price = None
    ob['initialized'] = False
    ob['last_offset'] = None
    ob['bids'].clear()
    ob['asks'].clear()

    # Start new task
    ws_task = _supervise_task(
        asyncio.create_task(subscribe_to_market_data(state.config.market_id)),
        "market_data_ws",
    )

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
        logger.warning("Capital data unavailable; suppressing quoting (returning None)")
        return None

    # Warn if capital data is old, but don't suppress — the last known value
    # is still valid (user_stats WS only fires on balance changes, so no
    # update for an hour just means no balance change occurred).
    if state.account.last_capital_update > 0 and not DRY_RUN:
        age = time.monotonic() - state.account.last_capital_update
        if age > _CAPITAL_STALE_SECONDS:
            logger.info(
                "Capital data is %.0fs old (threshold: %.0fs); using last known value",
                age, _CAPITAL_STALE_SECONDS,
            )

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
        logger.warning("calculate_dynamic_base_amount failed (%s); suppressing quoting", exc)
        return None

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
    if not _op_timestamps or len(_op_timestamps) + n <= _RL_OPS_PER_WINDOW:
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
    global _global_backoff_until, _global_backoff_consecutive, _consecutive_successes, _last_backoff_trigger_time
    _global_backoff_consecutive += 1
    _consecutive_successes = 0
    _last_backoff_trigger_time = time.monotonic()
    duration = min(_RL_BACKOFF_BASE * (2 ** (_global_backoff_consecutive - 1)), _RL_BACKOFF_MAX)
    _global_backoff_until = time.monotonic() + duration
    logger.warning("429 rate limit — global backoff for %.0fs (attempt #%d)",
                   duration, _global_backoff_consecutive)


def _reset_global_backoff():
    """After a successful write, require N consecutive successes before
    resetting escalation counter. Auto-resets after 5 minutes without a 429."""
    global _global_backoff_consecutive, _consecutive_successes
    _consecutive_successes += 1
    if _global_backoff_consecutive > 0:
        # Time-based decay: auto-reset if no 429 for 5 minutes
        if _last_backoff_trigger_time > 0 and time.monotonic() - _last_backoff_trigger_time > 300.0:
            logger.info("Backoff auto-reset: no 429 for 5+ minutes (was level %d)",
                        _global_backoff_consecutive)
            _global_backoff_consecutive = 0
            _consecutive_successes = 0
            return
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


async def _quota_recovery_task(client) -> None:
    """Background task wrapper: runs quota recovery without blocking the hot loop."""
    try:
        recovered = await _attempt_quota_recovery(client)
        if recovered:
            logger.info("Quota recovered, resuming normal operations")
    except Exception as exc:
        logger.error("Quota recovery task failed: %s", exc, exc_info=True)


async def _pause_cleanup_task(client) -> None:
    """Background task: cancel all orders and verify during pause.  Non-blocking to hot loop."""
    global _pause_cleanup_running
    try:
        _pause_attempts = getattr(risk_controller, '_pause_cleanup_attempts', 0) + 1
        risk_controller._pause_cleanup_attempts = _pause_attempts
        logger.warning("Trading is paused (%s); cancelling live orders (attempt %d).",
                       state.risk.pause_reason, _pause_attempts)

        # Force-clear stuck PLACING orders after 5 retries
        if _pause_attempts > 5 and _has_live_local_orders():
            logger.warning("Pause cleanup stuck after %d attempts — force-clearing PLACING orders",
                           _pause_attempts)
            for lvl in range(NUM_LEVELS):
                for side_name in ("buy", "sell"):
                    lc = order_manager.lifecycle(side_name, lvl)
                    if lc.status == SideStatus.PLACING:
                        _enqueue_order_event(OrderEvent(
                            event_type=OrderEventType.CLEAR_LIVE,
                            side=side_name, level=lvl,
                        ))

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
            if _dry_run_engine is not None:
                await _dry_run_engine.process_batch(cancel_ops)
            elif await _wait_for_write_slot(op_count=len(cancel_ops), cancel_only=True):
                await sign_and_send_batch(client, cancel_ops)
        elif _has_live_local_orders():
            logger.info("Pause cleanup waiting for exchange order ids before sending cancels.")

        if DRY_RUN:
            if not _has_live_local_orders():
                risk_controller.pause_cancel_done = True
                risk_controller._pause_cleanup_attempts = 0
            else:
                logger.warning("Pause cleanup incomplete; will retry while trading remains paused.")
        else:
            reconcile_ok = False
            try:
                reconcile_ok = await reconcile_orders_with_exchange(client, source="pause_cancel_verify")
            except Exception as exc:
                logger.error("Post-pause reconciliation failed: %s", exc)
            if reconcile_ok and not _has_live_local_orders():
                risk_controller.pause_cancel_done = True
                risk_controller._pause_cleanup_attempts = 0
            else:
                logger.warning("Pause cleanup incomplete; will retry while trading remains paused.")
    except Exception as exc:
        logger.error("Pause cleanup task failed: %s", exc, exc_info=True)
    finally:
        _pause_cleanup_running = False


async def order_lifecycle_watchdog_loop(interval=None, placing_timeout=None) -> None:
    """Background task: clear stale PLACING slots outside the hot order diff path."""
    if interval is None:
        interval = ORDER_LIFECYCLE_WATCHDOG_INTERVAL_SEC
    if placing_timeout is None:
        placing_timeout = ORDER_PLACING_TIMEOUT_SEC

    while True:
        await asyncio.sleep(interval)
        try:
            now = time.monotonic()
            for level in range(NUM_LEVELS):
                for side in ("buy", "sell"):
                    lc = order_manager.lifecycle(side, level)
                    if lc.status != SideStatus.PLACING or lc.updated_at <= 0:
                        continue

                    existing_id = (
                        state.orders.bid_order_ids[level]
                        if side == "buy"
                        else state.orders.ask_order_ids[level]
                    )
                    if existing_id is None or _has_exchange_id(existing_id):
                        continue

                    age = now - lc.updated_at
                    if age <= placing_timeout:
                        continue

                    logger.warning(
                        "Order %s[%d] stuck in PLACING for %.0fs — clearing stale slot",
                        side, level, age,
                    )
                    _enqueue_order_event(OrderEvent(
                        event_type=OrderEventType.CLEAR_LIVE,
                        side=side, level=level,
                    ))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Order lifecycle watchdog failed: %s", exc, exc_info=True)


async def _attempt_quota_recovery(client) -> bool:
    """Send small IOC market orders to generate volume and recover quota.

    Returns True if quota reached _QR_TARGET, False otherwise.
    Safeguards: max attempts, max cumulative loss, PnL monitoring, verify quota increases.
    """
    global _quota_recovery_in_progress, _quota_recovery_last_attempt
    if API_KEY_MAKER_ONLY is True:
        logger.info("QUOTA RECOVERY: disabled because API key %d is maker-only", API_KEY_INDEX)
        return False

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
                    if _is_maker_only_restriction(err):
                        _mark_api_key_maker_only("quota recovery market order rejected")
                    client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
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
                if _is_maker_only_restriction(e):
                    _mark_api_key_maker_only("quota recovery market order exception")
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
                close_timeout=5,
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

    def _drain_recv_queue(self) -> None:
        """Drop stale responses before a new transaction send."""
        while not self._recv_queue.empty():
            try:
                self._recv_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    @staticmethod
    def _code_message_from_response(resp: dict) -> tuple[int, str]:
        ws_error = resp.get("error")
        if isinstance(ws_error, dict):
            raw_code = ws_error.get("code", -1)
            message = str(ws_error.get("message", ""))
        elif ws_error is not None:
            raw_code = -1
            message = str(ws_error)
        else:
            raw_code = resp.get("code", resp.get("status_code", 0))
            message = str(resp.get("message", ""))
        try:
            code = int(raw_code) if raw_code is not None else 0
        except (TypeError, ValueError):
            code = -1
        if code == 200:
            code = 0
        return code, message

    @staticmethod
    def _status_from_response(resp: dict) -> TxSendStatus:
        code, _message = TxWebSocket._code_message_from_response(resp)
        if code == 0:
            return TxSendStatus.OK
        return TxSendStatus.REJECTED

    async def send_batch(self, tx_types: list, tx_infos: list) -> TxSendResult:
        """Send a transaction batch via WS and return transport outcome.

        ``NOT_SENT`` means no frame was written and REST fallback is safe.
        ``UNKNOWN`` means a frame may have reached Lighter; callers must not
        retry the same batch directly.
        """
        async with self._lock:
            if not self.is_connected:
                try:
                    await self.connect()
                except Exception:
                    return TxSendResult(TxSendStatus.NOT_SENT, reason="connect_failed")
                if not self.is_connected:
                    return TxSendResult(TxSendStatus.NOT_SENT, reason="not_connected")

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

            self._drain_recv_queue()

            try:
                await self._ws.send(msg_str)
            except Exception as exc:
                self._connected = False
                logger.warning("TxWebSocket send failed after attempt; outcome unknown: %s", exc)
                return TxSendResult(TxSendStatus.UNKNOWN, reason=f"send_failed:{exc}")

            try:
                resp = await asyncio.wait_for(self._recv_queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                self._connected = False
                logger.warning("TxWebSocket send timed out after frame write; outcome unknown")
                await self.close()
                return TxSendResult(TxSendStatus.UNKNOWN, reason="response_timeout")
            except Exception as exc:
                self._connected = False
                logger.warning("TxWebSocket response wait failed after frame write; outcome unknown: %s", exc)
                await self.close()
                return TxSendResult(TxSendStatus.UNKNOWN, reason=f"response_failed:{exc}")

            if resp is None:
                self._connected = False
                logger.warning("TxWebSocket disconnected after frame write; outcome unknown")
                return TxSendResult(TxSendStatus.UNKNOWN, reason="disconnected_after_send")
            if not isinstance(resp, dict):
                logger.warning("TxWebSocket returned malformed response after frame write: %r", resp)
                return TxSendResult(TxSendStatus.UNKNOWN, reason="malformed_response")
            return TxSendResult(self._status_from_response(resp), response=resp)


# Module-level TxWebSocket instance (initialized in main())
_tx_ws: Optional[TxWebSocket] = None
_unknown_outcome_reconcile_tasks: set[asyncio.Task] = set()


def _consume_background_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.warning("Background reconcile task failed: %s", exc, exc_info=True)


def _schedule_unknown_outcome_reconcile(client, source: str) -> None:
    try:
        task = asyncio.create_task(reconcile_orders_with_exchange(client, source=source))
    except RuntimeError:
        logger.warning("Unable to schedule reconciliation for %s: no running event loop", source)
        return
    _unknown_outcome_reconcile_tasks.add(task)
    task.add_done_callback(_unknown_outcome_reconcile_tasks.discard)
    task.add_done_callback(_consume_background_task_result)


def _handle_unknown_tx_outcome(client, signed_nonces: list, reason: str) -> None:
    logger.error(
        "Transaction batch outcome is UNKNOWN (%s). Pausing trading and forcing reconciliation.",
        reason,
    )
    risk_controller.mark_reconcile(ok=False, reason=f"tx_unknown:{reason}")
    risk_controller.trigger_pause(f"tx_unknown_outcome:{reason}")
    seen_keys = set()
    for aki in signed_nonces:
        if aki in seen_keys:
            continue
        client.nonce_manager.hard_refresh_nonce(aki)
        seen_keys.add(aki)
    _schedule_unknown_outcome_reconcile(client, source="tx_unknown_outcome")

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
            reduce_only = _is_reducing_side(side, state.account.position_size)

            if new_price is None:
                # Position limit suppressed this side — cancel any live order
                if is_buy:
                    existing_id = orders.bid_order_ids[level]
                else:
                    existing_id = orders.ask_order_ids[level]
                if existing_id is not None:
                    exchange_id = _resolve_exchange_id(existing_id)
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
                exchange_id = _resolve_exchange_id(existing_id)
                if exchange_id is None:
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
                    if _log_debug:
                        logger.debug(
                            "Keeping %s[%d]: price %.2f bps <= %.2f and size unchanged",
                            side, level, change_bps, effective_threshold,
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
                    reduce_only=reduce_only,
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
            _enqueue_order_event(OrderEvent(
                event_type=OrderEventType.BIND_LIVE,
                side=op.side, level=op.level,
                order_id=op.order_id, price=op.price, size=op.size,
            ))
        return True
    except Exception as e:
        body = getattr(e, 'body', None) or getattr(e, 'reason', '')
        logger.warning("REST single-tx error for %s %s[%d]: %s | body=%s",
                       op.action, op.side, op.level, e, body)
        if _is_quota_error(e):
            _update_volume_quota(0)
            client.nonce_manager.acknowledge_failure(API_KEY_INDEX)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
        elif _is_transient_error(e):
            _trigger_global_backoff()
            client.nonce_manager.acknowledge_failure(API_KEY_INDEX)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
        else:
            # Ordinary rejection (e.g. "order not found") — track for circuit breaker
            client.nonce_manager.acknowledge_failure(API_KEY_INDEX)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
            _record_order_rejection(f"rest_single:{e}")
        return False


def _sign_ops_sync(client, market_index: int, ops: list):
    """Sign BatchOps via the native signer (CPU-bound; run in an executor).

    Returns ``(tx_types, tx_infos, signed_ops, signed_nonces)``.  Per-op
    sign errors are logged, the op skipped and its nonce rolled back.
    """
    tx_types = []
    tx_infos = []
    signed_ops = []
    signed_nonces = []  # api_key_index per signed op — for rollback

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
                reduce_only=op.reduce_only,
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

    return tx_types, tx_infos, signed_ops, signed_nonces


async def sign_and_send_batch(client, ops: list):
    """Sign all BatchOps locally, then send as a single send_tx_batch call."""
    if not ops:
        return

    # Free-slot mode: when quota is 0, send only 1 op via REST sendTx
    free_slot_mode = _volume_quota_remaining is not None and _volume_quota_remaining <= 0
    if free_slot_mode:
        # Prioritize: cancels first, then position-reducing side
        pos = state.account.position_size
        def _free_slot_priority(op):
            if op.action == "cancel":
                return 0
            if pos > 0 and op.side == "sell":
                return 1  # long → prefer sell to reduce
            if pos < 0 and op.side == "buy":
                return 1  # short → prefer buy to reduce
            return 2
        ops = sorted(ops, key=_free_slot_priority)
        ops = ops[:1]
        logger.info("Free-slot mode (quota=0): sending 1 op via REST sendTx (%s %s)",
                    ops[0].action, ops[0].side)

    # Drain hot slot mutations only. Full reconcile snapshots are handled by
    # order_state_reconcile_loop so the send lane does not process cold state.
    order_manager.drain_hot_events()

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
                logger.info(
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
    signed_nonces = []  # api_key_index per signed op — for rollback on errors
    _sign_ms = 0.0

    try:
        async with _sdk_write_lock:
            if time.monotonic() < _global_backoff_until:
                logger.debug("Batch aborted: global backoff active")
                return

            # Sign off-loop: the native signer calls are CPU-bound and would
            # freeze the event loop (WS callbacks + hot loop) if run inline.
            # The write lock is held across sign+send so no other SDK writer
            # can interleave its nonce acquisition with ours.
            loop = asyncio.get_running_loop()
            _t0 = time.perf_counter()
            tx_types, tx_infos, signed_ops, signed_nonces = await loop.run_in_executor(
                None, _sign_ops_sync, client, market_index, ops,
            )
            _sign_ms = (time.perf_counter() - _t0) * 1000.0

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
                    _record_ops_sent(1)
                    await _send_single_op_rest(client, tx_types[0], tx_infos[0], signed_ops[0])
                except Exception as e:
                    logger.error("Free-slot single send exception: %s", e, exc_info=True)
                    client.nonce_manager.acknowledge_failure(signed_nonces[0])
                return

            _record_ops_sent(len(signed_ops))

            # Try WS first. REST fallback is safe only if no WS frame was sent.
            ws_result = None
            if _tx_ws is not None and _tx_ws.is_connected:
                ws_result = await _tx_ws.send_batch(tx_types, tx_infos)

            if ws_result is not None and ws_result.status == TxSendStatus.UNKNOWN:
                _handle_unknown_tx_outcome(client, signed_nonces, ws_result.reason)
                return

            if ws_result is not None and ws_result.status in (TxSendStatus.OK, TxSendStatus.REJECTED):
                # Parse WS response
                ws_resp = ws_result.response or {}
                logger.debug("WS batch raw response: %s", ws_resp)
                code, message = TxWebSocket._code_message_from_response(ws_resp)
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
            for aki in signed_nonces:
                client.nonce_manager.acknowledge_failure(aki)
            client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
            _record_order_rejection(f"batch:{err_msg}")
            return

        # Success
        logger.info(
            "Batch sent via %s: %d ops OK (sign %.1fms, quota remaining: %s)",
            send_method, len(signed_ops), _sign_ms, quota_remaining,
        )
        _record_order_success()
        for op in signed_ops:
            if op.action != "cancel":
                _enqueue_order_event(OrderEvent(
                    event_type=OrderEventType.BIND_LIVE,
                    side=op.side, level=op.level,
                    order_id=op.order_id, price=op.price, size=op.size,
                ))

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
        for aki in signed_nonces:
            client.nonce_manager.acknowledge_failure(aki)
        client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)
        _record_order_rejection("batch:exception")


def calculate_order_prices(mid_price, position_size=0.0, capital=None, base_amount=None,
                           max_pos_usd=None):
    """Calculate bid/ask prices for all order levels.

    Returns a list of ``NUM_LEVELS`` ``(buy_price, sell_price)`` tuples.
    Level 0 is the tight (base) spread from vol_obi; level 1+ widen the
    spread by ``SPREAD_FACTOR_LEVEL1``.

    When position value exceeds the dynamic max, the side that would
    *increase* exposure is suppressed (set to ``None``).
    """
    none_levels = [(None, None)] * NUM_LEVELS
    if not _cj_estimator_gate_allows_quote():
        if abs(position_size) >= EPSILON:
            return _fallback_reduce_only_quote_levels(mid_price, position_size)
        return none_levels

    calc = state.vol_obi_state.calculator
    if calc is not None and calc.warmed_up:
        try:
            if max_pos_usd is None:
                max_pos_usd = _dynamic_max_position_dollar(mid_price, capital, base_amount)
            set_max_position = getattr(calc, "set_max_position_dollar", None)
            if callable(set_max_position) and max_pos_usd is not None and max_pos_usd > 0:
                set_max_position(max_pos_usd)
            set_inventory_unit = getattr(calc, "set_inventory_unit_base", None)
            if callable(set_inventory_unit) and base_amount is not None and base_amount > 0:
                set_inventory_unit(base_amount)

            buy_0, sell_0 = calc.quote(mid_price, position_size)
            if buy_0 is None and sell_0 is None:
                if abs(position_size) >= EPSILON:
                    return _fallback_reduce_only_quote_levels(mid_price, position_size)
                return none_levels

            # Hard position limit: suppress side that would increase exposure
            if max_pos_usd <= 0:
                # Can't compute position limit (missing capital?) — suppress all quoting
                if abs(position_size) >= EPSILON:
                    return _fallback_reduce_only_quote_levels(mid_price, position_size)
                return none_levels
            pos_value_usd = abs(position_size) * mid_price
            if pos_value_usd >= max_pos_usd:
                if position_size > 0:
                    buy_0 = None  # Long at limit — suppress buys
                elif position_size < 0:
                    sell_0 = None  # Short at limit — suppress sells
                if buy_0 is None and sell_0 is None:
                    return _fallback_reduce_only_quote_levels(mid_price, position_size)

            levels = [(buy_0, sell_0)]
            # Derive wider levels by scaling the spread from mid
            bid_depth = (mid_price - buy_0) if buy_0 is not None else None
            ask_depth = (sell_0 - mid_price) if sell_0 is not None else None
            tick = state.config.price_tick_float
            for lvl in range(1, NUM_LEVELS):
                factor = _SPREAD_FACTORS[lvl]
                raw_bid = (mid_price - bid_depth * factor) if bid_depth is not None else None
                raw_ask = (mid_price + ask_depth * factor) if ask_depth is not None else None
                if raw_bid is not None and tick > 0:
                    raw_bid = math.floor(raw_bid / tick) * tick
                if raw_ask is not None and tick > 0:
                    raw_ask = math.ceil(raw_ask / tick) * tick
                levels.append((raw_bid, raw_ask))
            return levels
        except (ValueError, ZeroDivisionError, OverflowError) as e:
            logger.error("Error in %s quote: %s", QUOTE_ENGINE, e, exc_info=True)
            if abs(position_size) >= EPSILON:
                return _fallback_reduce_only_quote_levels(mid_price, position_size)
            return none_levels
    # Not warmed up yet — do not open fresh inventory, but keep an exit quote
    # for any inventory already received from the exchange.
    if abs(position_size) >= EPSILON:
        return _fallback_reduce_only_quote_levels(mid_price, position_size)
    return none_levels


def _normalize_live_order_size(size: float, mid_price: float) -> float:
    if size <= 0 or mid_price <= 0:
        return 0.0
    tick = state.config.amount_tick_float
    if tick > 0:
        size = math.floor(size / tick) * tick
    min_base = state.config.min_base_amount
    min_quote = max(float(state.config.min_quote_amount or 0.0), float(MIN_ORDER_VALUE_USD or 0.0))
    if min_base > 0 and size + EPSILON < min_base:
        size = min_base
    if min_quote > 0 and size * mid_price + EPSILON < min_quote:
        size = min_quote / mid_price
        if tick > 0:
            size = math.ceil(size / tick) * tick
    return max(size, 0.0)


def _apply_quality_spread_multiplier(level_prices, mid_price: float, multiplier: float):
    if multiplier <= 1.0001 or mid_price <= 0:
        return level_prices
    tick = state.config.price_tick_float
    adjusted = []
    for bid, ask in level_prices:
        new_bid = bid
        new_ask = ask
        if bid is not None:
            bid_depth = max(mid_price - bid, 0.0)
            new_bid = mid_price - bid_depth * multiplier
            if tick > 0:
                new_bid = math.floor(new_bid / tick) * tick
        if ask is not None:
            ask_depth = max(ask - mid_price, 0.0)
            new_ask = mid_price + ask_depth * multiplier
            if tick > 0:
                new_ask = math.ceil(new_ask / tick) * tick
        adjusted.append((new_bid, new_ask))
    return adjusted


def _apply_inventory_exit_bias(
    level_prices,
    mid_price: float,
    position_size: float,
    max_pos_usd: Optional[float],
    quality_adjustment: QualityAdjustment,
):
    """Bias quotes toward flattening live inventory without changing total risk budget."""
    global _last_inventory_exit_bias_log

    if not INVENTORY_EXIT_BIAS_ENABLED or mid_price <= 0 or not max_pos_usd or max_pos_usd <= 0:
        return level_prices
    if abs(position_size) < EPSILON:
        return level_prices

    inventory_value = abs(position_size) * mid_price
    ratio = inventory_value / max_pos_usd
    if ratio < INVENTORY_EXIT_BIAS_MIN_RATIO:
        return level_prices

    adverse_excess = max(0.0, quality_adjustment.adverse_bps - LIVE_QUALITY_ADVERSE_THRESHOLD_BPS)
    boost = 1.0 + min(0.5, adverse_excess * max(INVENTORY_ADVERSE_BOOST_PER_BPS, 0.0))
    exit_tighten = min(
        max(INVENTORY_MAX_EXIT_TIGHTEN, 0.0),
        max(INVENTORY_EXIT_TIGHTEN_PER_RATIO, 0.0) * ratio * boost,
    )
    add_widen = min(
        max(INVENTORY_MAX_ADD_WIDEN, 0.0),
        max(INVENTORY_ADD_WIDEN_PER_RATIO, 0.0) * ratio * boost,
    )
    if exit_tighten <= 0 and add_widen <= 0:
        return level_prices

    tick = state.config.price_tick_float
    min_depth = tick if tick > 0 else max(mid_price * 1e-6, 1e-9)
    adjusted = []
    for bid, ask in level_prices:
        new_bid = bid
        new_ask = ask
        if bid is not None:
            bid_depth = max(mid_price - bid, min_depth)
            if position_size < 0:
                # Short inventory: bid is the reducing side, so quote it closer.
                bid_depth *= max(0.05, 1.0 - exit_tighten)
            else:
                # Long inventory: bid increases risk, so quote it farther.
                bid_depth *= 1.0 + add_widen
            new_bid = mid_price - bid_depth
            if tick > 0:
                new_bid = math.floor(new_bid / tick) * tick
            if new_bid >= mid_price:
                new_bid = mid_price - min_depth
                if tick > 0:
                    new_bid = math.floor(new_bid / tick) * tick

        if ask is not None:
            ask_depth = max(ask - mid_price, min_depth)
            if position_size > 0:
                # Long inventory: ask is the reducing side, so quote it closer.
                ask_depth *= max(0.05, 1.0 - exit_tighten)
            else:
                # Short inventory: ask increases risk, so quote it farther.
                ask_depth *= 1.0 + add_widen
            new_ask = mid_price + ask_depth
            if tick > 0:
                new_ask = math.ceil(new_ask / tick) * tick
            if new_ask <= mid_price:
                new_ask = mid_price + min_depth
                if tick > 0:
                    new_ask = math.ceil(new_ask / tick) * tick
        adjusted.append((new_bid, new_ask))

    now = time.monotonic()
    if now - _last_inventory_exit_bias_log >= 60.0:
        logger.info(
            "Inventory exit bias active: pos=%.8f inv=$%.2f ratio=%.3f exit_tighten=%.3f add_widen=%.3f",
            position_size,
            inventory_value,
            ratio,
            exit_tighten,
            add_widen,
        )
        _last_inventory_exit_bias_log = now
    return adjusted


def _fallback_reduce_only_quote_levels(mid_price: float, position_size: float):
    """Return a single passive reducing quote when the model withholds quotes.

    This keeps live inventory from becoming unprotected if the quote engine
    refuses to quote at/above inventory limits.  It intentionally returns only
    level 0 so the total reduce-only size cannot exceed one base clip.
    """
    none_levels = [(None, None)] * NUM_LEVELS
    if abs(position_size) < EPSILON or mid_price <= 0:
        return none_levels

    tick = state.config.price_tick_float
    min_depth = tick if tick > 0 else max(mid_price * 1e-6, 1e-9)
    fallback_bps = max(CJ_MIN_HALF_SPREAD_BPS, VOL_OBI_MIN_HALF_SPREAD_BPS, 1.0)
    depth = max(mid_price * fallback_bps / 10_000.0, min_depth)

    levels = list(none_levels)
    if position_size > 0:
        ask = mid_price + depth
        if tick > 0:
            ask = math.ceil(ask / tick) * tick
        if ask <= mid_price:
            ask = mid_price + min_depth
        levels[0] = (None, ask)
    else:
        bid = mid_price - depth
        if tick > 0:
            bid = math.floor(bid / tick) * tick
        if bid >= mid_price:
            bid = mid_price - min_depth
        levels[0] = (bid, None)
    return levels


def _update_live_quality(mid_price: Optional[float], position_size: float, max_pos_usd: Optional[float]) -> QualityAdjustment:
    if _live_metrics is None or _dry_run_engine is not None:
        return QualityAdjustment(reason="unavailable")
    return _live_metrics.update(
        mid_price=mid_price,
        position_size=position_size,
        max_pos_usd=max_pos_usd,
        realized_pnl_cumulative=_live_fill_realized_pnl,
        portfolio_value=state.account.portfolio_value,
        available_capital=state.account.available_capital,
    )


def _maybe_log_quality_adjustment(adjustment: QualityAdjustment) -> None:
    global _last_quality_adjustment_log
    if adjustment.reason not in {"adverse_markout"}:
        return
    now = time.monotonic()
    if now - _last_quality_adjustment_log < 60.0:
        return
    _last_quality_adjustment_log = now
    logger.warning(
        "Live quality guard active: adverse=%.3fbps samples=%d spread_mult=%.3f size_mult=%.3f",
        adjustment.adverse_bps,
        adjustment.sample_count,
        adjustment.spread_multiplier,
        adjustment.size_multiplier,
    )

_MAX_CONSECUTIVE_LOOP_ERRORS = 10


async def _paced_send(client) -> None:
    """Background sender: drain the ops mailbox, pacing each batch.

    The freshest ops are pulled from the mailbox *after* the pacing gate, so
    a long rate-limit wait never sends prices computed before the wait.  The
    drain loop keeps going until the mailbox is empty, so ops posted while a
    send was in flight go out immediately instead of waiting for the next
    book tick.  Exceptions logged, not raised.
    """
    global _latest_ops
    try:
        while _latest_ops is not None:
            # Peek for pacing parameters only — the authoritative pull
            # happens after the gate (newest ops win, sizes approximate).
            peek = _latest_ops
            cancel_only = all(o.action == "cancel" for o in peek)
            if not await _wait_for_write_slot(op_count=len(peek), cancel_only=cancel_only):
                logger.debug(
                    "Paced send: write slot denied, %d ops dropped (recomputed next tick)",
                    len(peek),
                )
                _latest_ops = None
                return
            ops, _latest_ops = _latest_ops, None
            if not ops:
                return
            await sign_and_send_batch(client, ops)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.error("Paced send failed: %s", exc, exc_info=True)


async def order_state_reconcile_loop() -> None:
    """Cold-path task: apply coalesced reconcile snapshots outside the quote lane."""
    while True:
        try:
            await _reconcile_pending_event.wait()
            await asyncio.sleep(0)
            while order_manager.drain_reconcile_events():
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Order-state reconcile task failed: %s", exc, exc_info=True)
            await asyncio.sleep(1.0)


async def market_making_loop(client):
    global _pause_cleanup_running, _send_task, _latest_ops, _quote_seq
    logger.info("Starting 2-sided market making loop...")
    _log_info = logger.isEnabledFor(logging.INFO)
    _log_debug = logger.isEnabledFor(logging.DEBUG)
    consecutive_errors = 0
    _loop_start_time = time.monotonic()
    _warmup_logged = False
    _last_warmup_log_min = -1
    _warmup_complete_logged = False
    _last_seen_seq = _quote_seq  # track last-processed quote-relevant market-data sequence

    while True:
        try:
            # Time-based warmup: collect data without trading
            elapsed = time.monotonic() - _loop_start_time
            if elapsed < WARMUP_SECONDS:
                if abs(state.account.position_size) >= EPSILON:
                    logger.warning(
                        "Open inventory %.8f detected during warmup; bypassing warmup to quote reduce-only exit.",
                        state.account.position_size,
                    )
                    _loop_start_time = time.monotonic() - WARMUP_SECONDS
                    elapsed = WARMUP_SECONDS
                else:
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
                binance_ready = (ba is None or ba.warmed_up)
                # Refresh capital timestamp — data is valid (no trading during warmup)
                if state.account.available_capital is not None:
                    state.account.last_capital_update = time.monotonic()
                # Reconnect TxWebSocket (may have been dropped during warmup)
                if _tx_ws is not None and not _tx_ws.is_connected:
                    logger.info("Reconnecting TxWebSocket after warmup...")
                    await _tx_ws.connect()
                _refresh_cj_params_if_needed(force=True)
                cj_ready = _last_cj_estimator_ready if QUOTE_ENGINE == "cartea_jaimungal" else None
                external_alpha_ready = "n/a" if QUOTE_ENGINE == "cartea_jaimungal" else str(binance_ready)
                logger.info(
                    "Warmup complete (%.0fs). engine=%s quote_ready=%s external_alpha_ready=%s cj_estimator_ready=%s",
                    WARMUP_SECONDS, QUOTE_ENGINE, vol_ready, external_alpha_ready, cj_ready,
                )
            ws_healthy = check_websocket_health()
            risk_controller.maybe_recover(websocket_healthy=ws_healthy)

            if risk_controller.is_paused():
                _reset_quote_telemetry()
                if not risk_controller.pause_cancel_done and not _pause_cleanup_running:
                    _pause_cleanup_running = True
                    asyncio.create_task(_pause_cleanup_task(client))
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            if not ws_healthy:
                # Trigger reconnect via the event — ws_manager handles it
                # in its own task without blocking the hot loop.
                _reset_quote_telemetry()
                if not ws_reconnect_event.is_set():
                    logger.warning("Websocket unhealthy — triggering reconnect via event")
                    ws_reconnect_event.set()
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            # Wait for fresh market data via monotonic sequence counter.
            # Lighter book changes and external alpha changes both wake this path.
            if _quote_seq == _last_seen_seq:
                _quote_seq_event.clear()
                try:
                    await asyncio.wait_for(_quote_seq_event.wait(), timeout=ORDER_TIMEOUT)
                except asyncio.TimeoutError:
                    pass
            _last_seen_seq = _quote_seq

            # Process only hot slot mutations here. Reconcile snapshots are
            # coalesced and handled by the cold reconcile task.
            order_manager.drain_hot_events()

            # Snapshot volatile state into locals for speed and consistency
            snap_mid = state.market.mid_price
            snap_position = state.account.position_size
            snap_capital = state.account.available_capital

            if snap_mid is None:
                _reset_quote_telemetry()
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            # Use precomputed values (updated on capital/mid change events)
            base_amount = state.account.precomputed_base_amount
            _max_pos = state.account.precomputed_max_pos_usd
            if base_amount is None or base_amount <= 0:
                # First tick or data unavailable — force recompute
                _recompute_derived_params(snap_mid)
                base_amount = state.account.precomputed_base_amount
                _max_pos = state.account.precomputed_max_pos_usd
            if base_amount is None or base_amount <= 0:
                _reset_quote_telemetry()
                if _log_info:
                    logger.warning("Base amount is zero or invalid; skipping order refresh.")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            quality_adjustment = _update_live_quality(snap_mid, snap_position, _max_pos)
            if quality_adjustment.size_multiplier < 0.999:
                base_amount = _normalize_live_order_size(
                    base_amount * quality_adjustment.size_multiplier,
                    snap_mid,
                )
                if base_amount <= 0:
                    _reset_quote_telemetry()
                    await asyncio.sleep(MIN_LOOP_INTERVAL)
                    continue

            _refresh_cj_params_if_needed()
            _refresh_external_alpha_override()

            level_prices = calculate_order_prices(
                snap_mid, position_size=snap_position,
                capital=snap_capital, base_amount=base_amount,
                max_pos_usd=_max_pos)
            if quality_adjustment.spread_multiplier > 1.0001:
                level_prices = _apply_quality_spread_multiplier(
                    level_prices,
                    snap_mid,
                    quality_adjustment.spread_multiplier,
                )
                _maybe_log_quality_adjustment(quality_adjustment)
            level_prices = _apply_inventory_exit_bias(
                level_prices,
                snap_mid,
                snap_position,
                _max_pos,
                quality_adjustment,
            )

            buy_0, sell_0 = level_prices[0]
            _publish_quote_telemetry(
                mid=snap_mid,
                position_size=snap_position,
                buy_0=buy_0,
                sell_0=sell_0,
                max_pos_usd=_max_pos,
                quota_remaining=_volume_quota_remaining,
                threshold_bps=_adaptive_threshold_bps(),
            )

            # --- Quota / backoff logic (skip entirely in dry-run) ---
            if not DRY_RUN:
                # Quota recovery: launch as background task (non-blocking)
                if (_QR_ENABLED
                        and API_KEY_MAKER_ONLY is not True
                        and _volume_quota_remaining is not None
                        and _volume_quota_remaining < _QR_TRIGGER
                        and not _quota_recovery_in_progress
                        and time.monotonic() - _quota_recovery_last_attempt >= _QR_COOLDOWN
                        and time.monotonic() - _trading_start_time >= _QR_POST_WARMUP_GRACE):
                    asyncio.create_task(_quota_recovery_task(client))

                # Yield to quota recovery — don't compete for the free slot
                if _quota_recovery_in_progress:
                    await asyncio.sleep(MIN_LOOP_INTERVAL)
                    continue

            # Harvest completed background send task
            if _send_task is not None and _send_task.done():
                _send_task = None

            if _dry_run_engine is not None:
                # Dry-run: synchronous path (no network latency, no mailbox)
                ops = collect_order_operations(level_prices, base_amount, _log_debug)
                if ops:
                    await _dry_run_engine.process_batch(ops)
            else:
                # Live: always compute ops — no backpressure gate.  Store in
                # mailbox; the sender task drains the freshest batch after
                # its pacing gate.
                ops = collect_order_operations(level_prices, base_amount, _log_debug)
                if ops:
                    _latest_ops = ops
                # Start the sender if idle and the mailbox has content
                if (_send_task is None or _send_task.done()) and _latest_ops is not None:
                    _send_task = asyncio.create_task(_paced_send(client))

            if _dry_run_engine is not None:
                _dry_run_engine.maybe_log_summary()  # also flushes trade log + state

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
            # Flush trade log periodically (both modes)
            if _trade_logger is not None:
                _trade_logger.flush()
        except Exception as e:
            logger.error(f"❌ Error in track_balance: {e}", exc_info=True)
        await asyncio.sleep(300)


def _append_line(path, line):
    with open(path, "a") as f:
        f.write(line)


async def quote_telemetry_loop(
    interval=None,
    stale_after=None,
    quota_stuck_cooldown=None,
):
    """Background task: emit quote telemetry from the latest hot-loop snapshot."""
    if interval is None:
        interval = QUOTE_TELEMETRY_INTERVAL_SEC
    if stale_after is None:
        stale_after = QUOTE_TELEMETRY_STALE_AFTER_SEC
    if quota_stuck_cooldown is None:
        quota_stuck_cooldown = QUOTA_STUCK_WARNING_COOLDOWN_SEC

    last_quota_stuck_log = 0.0

    while True:
        await asyncio.sleep(interval)
        try:
            now = time.monotonic()
            snap = _quote_telemetry

            if (logger.isEnabledFor(logging.INFO)
                    and snap.updated_at > 0
                    and now - snap.updated_at <= stale_after
                    and (snap.buy_0 is not None or snap.sell_0 is not None)
                    and snap.mid is not None):
                bid_str = (
                    f"${snap.buy_0:.4f} (-{(snap.mid - snap.buy_0) / snap.mid * 100:.4f}%)"
                    if snap.buy_0 is not None else "LIMIT"
                )
                ask_str = (
                    f"${snap.sell_0:.4f} (+{(snap.sell_0 - snap.mid) / snap.mid * 100:.4f}%)"
                    if snap.sell_0 is not None else "LIMIT"
                )
                mode = "1-sided" if (snap.buy_0 is None or snap.sell_0 is None) else "2-sided"
                logger.info(
                    "QUOTING (%s) | Pos: %+.4f | Mid: $%.4f | Bid: %s | Ask: %s | MaxPos: $%.0f | Quota: %s | Thr: %.0fbp",
                    mode,
                    snap.position_size,
                    snap.mid,
                    bid_str,
                    ask_str,
                    snap.max_pos_usd,
                    snap.quota_remaining if snap.quota_remaining is not None else "?",
                    snap.threshold_bps,
                )

            quota_stuck = (
                not DRY_RUN
                and _volume_quota_remaining is not None
                and _volume_quota_remaining <= 0
                and now - _last_send_time > 300
            )
            if quota_stuck:
                if last_quota_stuck_log <= 0 or now - last_quota_stuck_log >= quota_stuck_cooldown:
                    logger.warning("Quota stuck at 0 for >5min with no sends — may need manual restart")
                    last_quota_stuck_log = now
            else:
                last_quota_stuck_log = 0.0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Quote telemetry task failed: %s", exc, exc_info=True)


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
                await asyncio.sleep(0)  # yield to let hot-path callbacks run
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

def _supervise_task(task: asyncio.Task, name: str) -> asyncio.Task:
    """Surface unexpected background-task death instead of failing silently.

    Long-lived tasks (WS feeds, watchdogs, reconcilers) are fire-and-forget;
    if one dies the bot keeps running without that subsystem.  Log CRITICAL
    and pause trading so no quotes rest on a dead feed.
    """
    task.set_name(name)

    def _on_done(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.critical("Background task %r died: %s", name, exc, exc_info=exc)
            risk_controller.trigger_pause(f"background task {name} died: {exc}")
        else:
            logger.critical("Background task %r exited unexpectedly", name)
            risk_controller.trigger_pause(f"background task {name} exited")
    task.add_done_callback(_on_done)
    return task


async def main():
    global ws_task, stale_order_task, order_state_task, _dry_run_engine, _trade_logger
    global _latest_reconcile_event, _pending_trades_scheduled, _pending_dry_run_fill_check
    global _live_fill_accounting_started, _live_fill_position_size
    global _live_fill_entry_vwap, _live_fill_realized_pnl
    global _cj_estimator, _last_cj_refresh, _last_cj_estimator_ready, _last_cj_gate_log
    global _account_trade_accept_after_ms, _last_live_accounting_sync_log

    _order_event_queue.clear()
    _pending_trades.clear()
    _pending_live_fill_contexts.clear()
    _processed_account_trade_ids.clear()
    _latest_reconcile_event = None
    _reconcile_pending_event.clear()
    _pending_trades_scheduled = False
    _pending_dry_run_fill_check = False
    _live_fill_accounting_started = False
    _live_fill_position_size = 0.0
    _live_fill_entry_vwap = 0.0
    _live_fill_realized_pnl = 0.0
    _account_trade_accept_after_ms = 0 if DRY_RUN else int(time.time() * 1000)
    _last_live_accounting_sync_log = 0.0

    if DRY_RUN:
        logger.info("🚀 === Market Maker v2 Starting — DRY-RUN MODE (no exchange writes) ===")
    else:
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

    _cython_vobi = VolObiCalculator.__module__ == '_vol_obi_fast'
    logger.info(
        "⚡ Acceleration: CBookSide=%s, VolObiCalculator=%s",
        "Cython" if _CYTHON_AVAILABLE else "Python",
        "Cython" if _cython_vobi else "Python",
    )

    _cj_estimator = None
    _last_cj_refresh = 0.0
    _last_cj_estimator_ready = False
    _last_cj_gate_log = 0.0

    if QUOTE_ENGINE == "cartea_jaimungal":
        state.vol_obi_state.calculator = CarteaJaimungalCalculator(
            tick_size=state.config.price_tick_float,
            params=_base_cj_params(),
            min_warmup_samples=VOL_OBI_MIN_WARMUP_SAMPLES,
        )
        if CJ_USE_ESTIMATOR:
            _cj_estimator = LighterCJEstimator(
                window_seconds=CJ_ESTIMATOR_WINDOW_SECONDS,
                markout_seconds=CJ_ESTIMATOR_MARKOUT_SECONDS,
                min_trades_per_side=CJ_ESTIMATOR_MIN_TRADES_PER_SIDE,
                min_markouts_per_side=CJ_ESTIMATOR_MIN_MARKOUTS_PER_SIDE,
                kappa_min=CJ_ESTIMATOR_KAPPA_MIN,
                kappa_max=CJ_ESTIMATOR_KAPPA_MAX,
                min_kappa_points=CJ_ESTIMATOR_MIN_KAPPA_POINTS,
                min_kappa_r2=CJ_ESTIMATOR_MIN_KAPPA_R2,
                epsilon_floor=CJ_ESTIMATOR_EPSILON_FLOOR,
                epsilon_cap=CJ_ESTIMATOR_EPSILON_CAP,
                default_lambda=CJ_ESTIMATOR_DEFAULT_LAMBDA,
                default_kappa=CJ_ESTIMATOR_DEFAULT_KAPPA,
                default_epsilon=CJ_ESTIMATOR_DEFAULT_EPSILON,
                default_sigma2=CJ_SIGMA2_PER_SEC,
            )
        logger.info(
            "📈 Quote engine: cartea_jaimungal | estimator=%s | lambda=%.3f kappa=%.3f "
            "epsilon=%.2f spread_mult=%.2f min/max=%.1f/%.1fbps refresh=%.0fs require_ready=%s",
            bool(_cj_estimator),
            CJ_LAMBDA,
            CJ_KAPPA,
            CJ_EPSILON,
            CJ_SPREAD_MULTIPLIER,
            CJ_MIN_HALF_SPREAD_BPS,
            CJ_MAX_HALF_SPREAD_BPS,
            CJ_REFRESH_SECONDS,
            CJ_REQUIRE_ESTIMATOR_READY,
        )
    else:
        # Initialize legacy vol_obi spread calculator
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
            max_position_dollar=500.0,  # placeholder; updated dynamically each loop
        )
        logger.info(
            "📈 Spread mode: vol_obi (Volatility + OBI) | "
            "vol_to_half=%.2f | min_bps=%.1f | skew=%.2f | warmup=%d samples",
            VOL_OBI_VOL_TO_HALF_SPREAD, VOL_OBI_MIN_HALF_SPREAD_BPS,
            VOL_OBI_SKEW, VOL_OBI_MIN_WARMUP_SAMPLES,
        )

    # Start Binance feeds (if applicable)
    binance_bbo_task = None
    binance_depth_task = None
    if QUOTE_ENGINE != "cartea_jaimungal" and ALPHA_SOURCE == "binance":
        binance_sym = lighter_to_binance_symbol(MARKET_SYMBOL)
        if binance_sym is not None:
            # Feed 1: @bookTicker → SharedBBO (lowest-latency BBO)
            shared_bbo = SharedBBO(min_samples=BINANCE_BBO_MIN_SAMPLES)
            state.binance_bbo = shared_bbo
            bbo_client = BinanceBookTickerClient(
                binance_symbol=binance_sym,
                shared_bbo=shared_bbo,
                stale_threshold=BINANCE_BBO_STALE_SECONDS,
            )
            binance_bbo_task = _supervise_task(
                asyncio.create_task(bbo_client.run()), "binance_bbo")
            logger.info("Binance BBO: %s@bookTicker", binance_sym)

            # Feed 2: @depth@100ms → SharedAlpha (local book + imbalance alpha)
            shared_alpha = SharedAlpha(min_samples=BINANCE_OBI_MIN_SAMPLES)
            state.binance_alpha = shared_alpha
            depth_client = BinanceDiffDepthClient(
                binance_symbol=binance_sym,
                shared_alpha=shared_alpha,
                window_size=BINANCE_OBI_WINDOW,
                looking_depth=BINANCE_OBI_LOOKING_DEPTH,
                stale_threshold=BINANCE_STALE_SECONDS,
                snapshot_limit=BINANCE_DEPTH_SNAPSHOT_LIMIT,
                on_alpha_update=_on_external_alpha_update,
            )
            binance_depth_task = _supervise_task(
                asyncio.create_task(depth_client.run()), "binance_depth")
            logger.info(
                "Binance depth: %s@depth@100ms | window=%d stale=%.0fs min_samples=%d",
                binance_sym, BINANCE_OBI_WINDOW, BINANCE_STALE_SECONDS, BINANCE_OBI_MIN_SAMPLES,
            )
        else:
            logger.info("No Binance mapping for %s; using Lighter OBI", MARKET_SYMBOL)

    client = None
    if not DRY_RUN:
        if not API_KEY_PRIVATE_KEY:
            logger.error("❌ API_KEY_PRIVATE_KEY not set. Create a .env file or set the env var. Exiting.")
            await api_client.close()
            return
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
        await _detect_maker_only_api_key(client)

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

    if not DRY_RUN:
        # Initialize TxWebSocket for sending transactions via WS
        global _tx_ws
        _tx_ws = TxWebSocket(WEBSOCKET_URL)
        await _tx_ws.connect()

    # Start WebSocket Tasks
    ws_task = _supervise_task(
        asyncio.create_task(subscribe_to_market_data(state.config.market_id)), "market_data_ws")
    ticker_task = _supervise_task(
        asyncio.create_task(subscribe_to_ticker(state.config.market_id)), "ticker_ws")
    public_trade_task = None
    if _cj_estimator is not None:
        public_trade_task = _supervise_task(
            asyncio.create_task(subscribe_to_public_trades(state.config.market_id)),
            "public_trades_ws",
        )
        logger.info("✅ CJ estimator public trade subscription started for market %d", state.config.market_id)
    order_state_task = _supervise_task(
        asyncio.create_task(order_state_reconcile_loop()), "order_state_reconcile")
    logger.info("✅ ticker WS subscription started for market %d", state.config.market_id)

    user_stats_task = None
    account_all_task = None
    if DRY_RUN:
        # Separate dry-run wallet — no need for real account WS
        _dr_default_capital = 1000.0
        if TEST_MODE_DURATION is not None:
            # Test mode: isolated state files that don't interfere with real dry-run
            _dr_state_path = os.path.join(LOG_DIR, "test_dry_run_state.json")
        else:
            _dr_state_path = os.path.join(LOG_DIR, "dry_run_state.json")

        if DRY_RUN_CAPITAL is not None:
            # Explicit --capital: reset to fresh wallet
            state.account.available_capital = DRY_RUN_CAPITAL
            state.account.portfolio_value = DRY_RUN_CAPITAL
            state.account.position_size = 0.0
            # Clear old state file
            if os.path.exists(_dr_state_path):
                os.remove(_dr_state_path)
            logger.info("DRY-RUN wallet RESET to $%.2f", DRY_RUN_CAPITAL)
        elif os.path.exists(_dr_state_path):
            # Saved state exists — pre-load capital so logs/checks aren't None
            # before DryRunEngine.load_state runs the full restore.
            try:
                with open(_dr_state_path) as _sf:
                    _saved = json.loads(_sf.read())
                state.account.available_capital = _saved.get("available_capital", _dr_default_capital)
                state.account.portfolio_value = _saved.get("portfolio_value", _dr_default_capital)
                state.account.position_size = _saved.get("position", 0.0)
            except Exception:
                state.account.available_capital = _dr_default_capital
                state.account.portfolio_value = _dr_default_capital
                state.account.position_size = 0.0
            logger.info("DRY-RUN: found saved state at %s", _dr_state_path)
        else:
            # First run — default capital
            state.account.available_capital = _dr_default_capital
            state.account.portfolio_value = _dr_default_capital
            state.account.position_size = 0.0
            logger.info("DRY-RUN wallet: $%.2f (first run)", _dr_default_capital)

        account_state_received.set()
        account_all_received.set()
    else:
        user_stats_task = _supervise_task(
            asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX)), "user_stats_ws")
        account_all_task = _supervise_task(
            asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX)), "account_all_ws")

    account_orders_task = None
    if not DRY_RUN and client is not None and API_KEY_PRIVATE_KEY:
        account_orders_task = _supervise_task(
            asyncio.create_task(
                subscribe_to_account_orders(client, state.config.market_id, ACCOUNT_INDEX)
            ),
            "account_orders_ws",
        )
        logger.info("✅ account_orders authenticated WS task started")
    else:
        logger.info("ℹ️  account_orders WS skipped (no credentials)")

    stale_order_task = None
    if not DRY_RUN and client is not None and API_KEY_PRIVATE_KEY:
        stale_order_task = _supervise_task(
            asyncio.create_task(
                stale_order_reconciler_loop(client, state.config.market_id, ACCOUNT_INDEX)
            ),
            "stale_order_reconciler",
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

        if DRY_RUN:
            from dry_run import DryRunEngine
            from trade_log import TradeLogger
            _test_suffix = "_test" if TEST_MODE_DURATION is not None else ""
            _trade_logger = TradeLogger(LOG_DIR, MARKET_SYMBOL + _test_suffix)
            if DRY_RUN_CAPITAL is not None or TEST_MODE_DURATION is not None:
                _trade_logger.clear()  # reset trade log on --capital or --test

            # Try to restore from saved state, otherwise create fresh
            if DRY_RUN_CAPITAL is None and TEST_MODE_DURATION is None:
                _dry_run_engine = DryRunEngine.load_state(
                    _dr_state_path,
                    state=state,
                    order_manager=order_manager,
                    client_to_exchange_id=_client_to_exchange_id,
                    leverage=LEVERAGE,
                    logger=logger,
                    trade_logger=_trade_logger,
                    rejection_callback=_record_order_rejection,
                )
            if _dry_run_engine is None:
                # Seed defaults if load_state failed on corrupt file
                if DRY_RUN_CAPITAL is None:
                    # State load was attempted but failed — old trade CSV is
                    # inconsistent with the fresh wallet, so reset it.
                    _trade_logger.clear()
                if state.account.available_capital is None:
                    state.account.available_capital = _dr_default_capital
                    state.account.portfolio_value = _dr_default_capital
                    state.account.position_size = 0.0
                    logger.warning("DRY-RUN: state load failed — using default $%.2f", _dr_default_capital)
                _dry_run_engine = DryRunEngine(
                    state=state,
                    order_manager=order_manager,
                    client_to_exchange_id=_client_to_exchange_id,
                    leverage=LEVERAGE,
                    logger=logger,
                    trade_logger=_trade_logger,
                    state_path=_dr_state_path,
                    rejection_callback=_record_order_rejection,
                    maker_fee_rate=MAKER_FEE_RATE,
                )
                _dry_run_engine.capture_initial_state()
            logger.info("DRY-RUN engine initialized — run with --live for real trading")
        else:
            from trade_log import TradeLogger
            global _live_state_store, _live_metrics
            _trade_logger = TradeLogger(LOG_DIR, MARKET_SYMBOL)
            _live_state_store = LiveStateStore(LOG_DIR, MARKET_SYMBOL)
            _live_metrics = LiveMetricsTracker(
                LOG_DIR,
                MARKET_SYMBOL,
                horizons=LIVE_MARKOUT_HORIZONS,
                window_seconds=LIVE_QUALITY_WINDOW_SECONDS,
                adaptive_enabled=LIVE_QUALITY_ADAPTIVE_ENABLED,
                adaptive_horizon=LIVE_QUALITY_ADAPTIVE_HORIZON,
                adverse_threshold_bps=LIVE_QUALITY_ADVERSE_THRESHOLD_BPS,
                spread_widen_per_bps=LIVE_QUALITY_SPREAD_WIDEN_PER_BPS,
                max_spread_multiplier=LIVE_QUALITY_MAX_SPREAD_MULTIPLIER,
                size_reduce_per_bps=LIVE_QUALITY_SIZE_REDUCE_PER_BPS,
                min_size_multiplier=LIVE_QUALITY_MIN_SIZE_MULTIPLIER,
                metrics_flush_seconds=LIVE_QUALITY_METRICS_FLUSH_SECONDS,
            )
            _initialize_live_fill_accounting_from_account()

            # Emergency close any leftover position from a previous unclean shutdown
            if abs(state.account.position_size) > EPSILON:
                dust_reason = _non_actionable_close_reason(state.account.position_size, state.market.mid_price)
                if dust_reason is not None:
                    logger.info(
                        "Detected non-actionable startup dust position (%.8f, $%.2f): %s. Skipping emergency close.",
                        state.account.position_size,
                        abs(state.account.position_size) * (state.market.mid_price or 0),
                        dust_reason,
                    )
                elif not PANIC_CLOSE_ON_STARTUP:
                    logger.warning(
                        "Detected startup inventory %.8f ($%.2f). panic_close_on_startup=false; "
                        "preserving position and letting inventory-aware quoting manage it.",
                        state.account.position_size,
                        abs(state.account.position_size) * (state.market.mid_price or 0),
                    )
                else:
                    logger.warning(
                        "Detected open position (%.6f) on startup — panic_close_on_startup=true, attempting emergency close.",
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

            if API_KEY_MAKER_ONLY is True:
                logger.info(
                    "Skipping leverage update: API key %d is maker-only and cannot send this transaction type",
                    API_KEY_INDEX,
                )
            else:
                logger.info(f"⚙️ Attempting to set leverage to {LEVERAGE}x with {MARGIN_MODE} margin...")
                _, _, err = await adjust_leverage(client, state.config.market_id, LEVERAGE, MARGIN_MODE, logger=logger)
                if err:
                    if _is_maker_only_restriction(err):
                        _mark_api_key_maker_only("leverage update rejected")
                    logger.error(f"❌ Failed to adjust leverage: {err}. Continuing with default leverage.")
                else:
                    logger.info(f"✅ Successfully set leverage to {LEVERAGE}x")

        balance_task = _supervise_task(
            asyncio.create_task(track_balance()), "balance_tracker")
        sanity_task = _supervise_task(
            asyncio.create_task(periodic_orderbook_sanity_check()), "orderbook_sanity")
        lifecycle_watchdog_task = _supervise_task(
            asyncio.create_task(order_lifecycle_watchdog_loop()), "lifecycle_watchdog")
        quote_telemetry_task = _supervise_task(
            asyncio.create_task(quote_telemetry_loop()), "quote_telemetry")

        # Test mode: auto-exit after configured duration
        if TEST_MODE_DURATION is not None:
            async def _test_mode_timer():
                await asyncio.sleep(TEST_MODE_DURATION)
                logger.info("🧪 TEST MODE: %ds elapsed — shutting down (no errors detected)", TEST_MODE_DURATION)
            _test_timer_task = asyncio.create_task(_test_mode_timer())
            # Race: main loop vs timer. Timer finishing means success.
            done, pending = await asyncio.wait(
                [asyncio.create_task(market_making_loop(client)), _test_timer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            # Re-raise if the loop task ended with an error
            for t in done:
                if t.exception() is not None:
                    raise t.exception()
        else:
            await market_making_loop(client)

    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for initial data from websockets.")
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 === Shutdown signal received - Stopping... ===")
    finally:
        logger.info("🧹 === Market Maker Cleanup Starting ===")
        tasks_to_cancel = []
        if 'user_stats_task' in locals() and user_stats_task is not None:
            tasks_to_cancel.append(user_stats_task)
        if 'account_all_task' in locals() and account_all_task is not None:
            tasks_to_cancel.append(account_all_task)
        if account_orders_task is not None:
            tasks_to_cancel.append(account_orders_task)
        if stale_order_task is not None:
            tasks_to_cancel.append(stale_order_task)
        if 'balance_task' in locals():
            tasks_to_cancel.append(balance_task)
        if 'sanity_task' in locals():
            tasks_to_cancel.append(sanity_task)
        if 'lifecycle_watchdog_task' in locals():
            tasks_to_cancel.append(lifecycle_watchdog_task)
        if 'quote_telemetry_task' in locals():
            tasks_to_cancel.append(quote_telemetry_task)
        if order_state_task is not None:
            tasks_to_cancel.append(order_state_task)
        if 'ticker_task' in locals():
            tasks_to_cancel.append(ticker_task)
        if 'public_trade_task' in locals() and public_trade_task is not None:
            tasks_to_cancel.append(public_trade_task)
        # ws_task is a module global (declared ``global`` above), so it is
        # never in locals() — check the global directly or the market-data
        # task survives cleanup and keeps processing during shutdown.
        if ws_task is not None:
            tasks_to_cancel.append(ws_task)
        if binance_bbo_task is not None:
            tasks_to_cancel.append(binance_bbo_task)
        if binance_depth_task is not None:
            tasks_to_cancel.append(binance_depth_task)

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        # Bounded teardown: a task that ignores cancellation must not hang
        # shutdown forever — in live mode the exchange-order cancel below
        # still has to run.  A task can wedge inside its own cleanup (e.g. a
        # WS close handshake that never completes); a second cancel
        # interrupts that await, so escalate before abandoning.
        if tasks_to_cancel:
            _done, _pending = await asyncio.wait(tasks_to_cancel, timeout=15.0)
            if _pending:
                for task in _pending:
                    logger.warning(
                        "Cleanup: task %r did not exit within 15s — cancelling again",
                        task.get_name(),
                    )
                    task.cancel()
                _done, _pending = await asyncio.wait(_pending, timeout=5.0)
                for task in _pending:
                    logger.error(
                        "Cleanup: task %r still alive — abandoning it",
                        task.get_name(),
                    )

        if DRY_RUN:
            if _dry_run_engine is not None:
                if TEST_MODE_DURATION is None:
                    _dry_run_engine.save_state()
                else:
                    # Test mode: clean up isolated state file
                    try:
                        os.remove(_dr_state_path)
                    except FileNotFoundError:
                        pass
                if _dry_run_engine._trade_logger is not None:
                    _dry_run_engine._trade_logger.flush()
                _dry_run_engine.maybe_log_summary()
                logger.info(
                    "DRY-RUN FINAL | realized=$%.4f | unrealized=$%.4f | total=$%.4f | fills=%d",
                    _dry_run_engine._realized_pnl, _dry_run_engine.unrealized_pnl,
                    _dry_run_engine.total_pnl, _dry_run_engine._fill_count,
                )
        else:
            if _trade_logger is not None:
                _trade_logger.flush()
            try:
                logger.info("🛡️ Final safety measure: attempting to cancel all orders.")
                await asyncio.wait_for(cancel_all_orders(client), timeout=10)
            except asyncio.TimeoutError:
                logger.error("Timeout during final order cancellation.")
            except Exception as e:
                logger.error(f"Error during final order cancellation: {e}")

            # Preserve inventory by default.  A normal service restart should
            # cancel quotes, then let the next startup resume inventory-aware
            # quoting.  Aggressive close is reserved for explicit panic mode.
            if abs(state.account.position_size) > EPSILON:
                if not PANIC_CLOSE_ON_SHUTDOWN:
                    logger.warning(
                        "Open position at shutdown (%.8f, $%.2f). panic_close_on_shutdown=false; "
                        "orders cancelled, inventory preserved for next startup.",
                        state.account.position_size,
                        abs(state.account.position_size) * (state.market.mid_price or 0),
                    )
                else:
                    # If the orderbook is gone (WS tasks cancelled), fetch REST prices directly.
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

                    dust_reason = _non_actionable_close_reason(state.account.position_size, state.market.mid_price)
                    if dust_reason is not None:
                        logger.info(
                            "Detected non-actionable shutdown dust position (%.8f, $%.2f): %s. Skipping emergency close.",
                            state.account.position_size,
                            abs(state.account.position_size) * (state.market.mid_price or 0),
                            dust_reason,
                        )
                    else:
                        logger.warning(
                            "Open position detected at shutdown (%.6f) — panic_close_on_shutdown=true, attempting emergency close.",
                            state.account.position_size,
                        )
                        try:
                            await asyncio.wait_for(
                                emergency_close_position(client, reason="shutdown"),
                                timeout=15,
                            )
                        except asyncio.TimeoutError:
                            logger.error("Timeout during shutdown emergency position close!")
                        except Exception as e:
                            logger.error("Error during shutdown emergency position close: %s", e)
            _persist_live_state()

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
            if client is not None:
                await client.close()
        await api_client.close()
        logger.info("🛑 Market maker stopped.")

# ============ Entrypoint with signal handling ============
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Lighter market maker")
    parser.add_argument("--symbol", default=os.getenv("MARKET_SYMBOL", "BTC"), help="Market symbol to trade")
    parser.add_argument("--live", action="store_true", help="Live trading mode (default is dry-run/paper-trading)")
    parser.add_argument("--capital", type=float, default=None, help="Reset dry-run wallet to this USD amount (default: 1000 on first run)")
    parser.add_argument("--test", type=int, nargs="?", const=180, metavar="SECONDS",
                        help="Smoke-test mode: 60s warmup, isolated state, auto-exit after SECONDS (default 180)")
    parser.add_argument("--grid", type=str, default=None, metavar="CONFIG",
                        help="Parallel grid dry-run: run N parameter combos against shared orderbook")
    args = parser.parse_args()
    MARKET_SYMBOL = args.symbol.upper()
    os.environ["MARKET_SYMBOL"] = MARKET_SYMBOL

    # Grid mode: parallel paper-trading only — never live
    if args.grid:
        if args.live:
            print("ERROR: --grid is paper-trading only and cannot be combined with --live")
            _sys.exit(1)
        from grid_dry_run import GridRunner
        setup_logging(__name__, log_dir=LOG_DIR, log_filename="grid_debug.log")
        runner = GridRunner(args.grid, MARKET_SYMBOL)
        asyncio.run(runner.run())
        _sys.exit(0)

    DRY_RUN = not args.live
    DRY_RUN_CAPITAL = args.capital
    TEST_MODE_DURATION = args.test  # None if not passed, else seconds

    if TEST_MODE_DURATION is not None:
        DRY_RUN = True
        WARMUP_SECONDS = 60
        # Tighter spreads to generate fills during the short test window
        VOL_OBI_VOL_TO_HALF_SPREAD = 12.0
        VOL_OBI_MIN_HALF_SPREAD_BPS = 1.5
        QUOTE_UPDATE_THRESHOLD_BPS = 2.0
        logger.info("🧪 TEST MODE: warmup=60s, auto-exit after %ds, isolated state", TEST_MODE_DURATION)

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
