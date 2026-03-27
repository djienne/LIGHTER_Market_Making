from contextlib import contextmanager

import numpy as np
import pandas as pd

import market_maker_v2 as mm


# Returns (obj, attr) for scalars or (obj, attr, index) for list-indexed fields.
# List-indexed entries proxy level-0 for backward compatibility.
_STATE_MAP = {
    'current_bid_order_id':    lambda: (mm.state.orders, 'bid_order_ids', 0),
    'current_ask_order_id':    lambda: (mm.state.orders, 'ask_order_ids', 0),
    'current_bid_price':       lambda: (mm.state.orders, 'bid_prices', 0),
    'current_ask_price':       lambda: (mm.state.orders, 'ask_prices', 0),
    'current_bid_size':        lambda: (mm.state.orders, 'bid_sizes', 0),
    'current_ask_size':        lambda: (mm.state.orders, 'ask_sizes', 0),
    'last_client_order_index': lambda: (mm.state.orders, 'last_client_order_index'),
    'MARKET_ID':               lambda: (mm.state.config, 'market_id'),
    '_PRICE_TICK_FLOAT':       lambda: (mm.state.config, 'price_tick_float'),
    '_AMOUNT_TICK_FLOAT':      lambda: (mm.state.config, 'amount_tick_float'),
    'local_order_book':        lambda: (mm.state.market, 'local_order_book'),
    'current_mid_price_cached':lambda: (mm.state.market, 'mid_price'),
    'ws_connection_healthy':   lambda: (mm.state.market, 'ws_connection_healthy'),
    'last_order_book_update':  lambda: (mm.state.market, 'last_order_book_update'),
    'available_capital':       lambda: (mm.state.account, 'available_capital'),
    'portfolio_value':         lambda: (mm.state.account, 'portfolio_value'),
    'current_position_size':   lambda: (mm.state.account, 'position_size'),
    'account_positions':       lambda: (mm.state.account, 'positions'),
    'recent_trades':           lambda: (mm.state.account, 'recent_trades'),
    'vol_obi_calc':            lambda: (mm.state.vol_obi_state, 'calculator'),
    '_tx_ws':                  lambda: (mm, '_tx_ws'),
    '_last_send_time':             lambda: (mm, '_last_send_time'),
    '_last_sdk_write_time':        lambda: (mm, '_last_send_time'),  # compat alias
    '_global_backoff_until':       lambda: (mm, '_global_backoff_until'),
    '_op_timestamps':              lambda: (mm, '_op_timestamps'),
    '_volume_quota_remaining':     lambda: (mm, '_volume_quota_remaining'),
    '_consecutive_successes':      lambda: (mm, '_consecutive_successes'),
    '_quota_warning_level':        lambda: (mm, '_quota_warning_level'),
    '_quota_recovery_in_progress': lambda: (mm, '_quota_recovery_in_progress'),
    '_quota_recovery_last_attempt':lambda: (mm, '_quota_recovery_last_attempt'),
    '_send_task':                  lambda: (mm, '_send_task'),
}


@contextmanager
def temp_mm_attrs(**overrides):
    originals = {}
    # Save and clear the ID mapping to prevent cross-test pollution
    saved_id_mapping = dict(mm._client_to_exchange_id)
    mm._client_to_exchange_id.clear()

    for name, value in overrides.items():
        if name in _STATE_MAP:
            entry = _STATE_MAP[name]()
            if len(entry) == 3:
                obj, attr, idx = entry
                originals[name] = (obj, attr, idx, getattr(obj, attr)[idx])
                getattr(obj, attr)[idx] = value
            else:
                obj, attr = entry
                originals[name] = (obj, attr, None, getattr(obj, attr))
                setattr(obj, attr, value)
        else:
            # Module-level constants (QUOTE_UPDATE_THRESHOLD_BPS, REQUIRE_PARAMS, etc.)
            originals[name] = (mm, name, None, getattr(mm, name))
            setattr(mm, name, value)
    try:
        yield
    finally:
        for name, (obj, attr, idx, orig) in originals.items():
            if idx is not None:
                getattr(obj, attr)[idx] = orig
            else:
                setattr(obj, attr, orig)
        mm._client_to_exchange_id.clear()
        mm._client_to_exchange_id.update(saved_id_mapping)


@contextmanager
def temp_event_state(event, set_value=None):
    original = event.is_set()
    if set_value is not None:
        if set_value:
            event.set()
        else:
            event.clear()
    try:
        yield
    finally:
        if original:
            event.set()
        else:
            event.clear()


class _NonceMgr:
    """Deterministic nonce manager for testing."""

    def __init__(self):
        self._counter = 0
        self.failures = []

    def next_nonce(self):
        self._counter += 1
        return (0, self._counter)

    def acknowledge_failure(self, api_key_index):
        self.failures.append(api_key_index)

    def hard_refresh_nonce(self, api_key_index):
        pass


class DummyClient:
    """Unified mock client for testing.

    All methods are always present; errors are controlled via constructor
    kwargs.
    """

    def __init__(self, modify_err=None, cancel_err=None, cancel_exc=None,
                 sign_err=None, send_code=0, send_message=""):
        self.create_order_calls = []
        self.modify_order_calls = []
        self.cancel_order_calls = []
        self.nonce_manager = _NonceMgr()
        self.sign_create_calls = []
        self.sign_cancel_calls = []
        self.sign_modify_calls = []
        self.send_tx_batch_calls = []
        self._modify_err = modify_err
        self._cancel_err = cancel_err
        self._cancel_exc = cancel_exc
        self._sign_err = sign_err
        self._send_code = send_code
        self._send_message = send_message

    async def create_order(self, *args, **kwargs):
        self.create_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", None

    async def modify_order(self, *args, **kwargs):
        self.modify_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", self._modify_err

    async def cancel_order(self, *args, **kwargs):
        self.cancel_order_calls.append({"args": args, "kwargs": kwargs})
        if self._cancel_exc:
            raise self._cancel_exc
        return "tx", "hash", self._cancel_err

    def sign_create_order(self, **kwargs):
        self.sign_create_calls.append(kwargs)
        return (1, b"tx_info_create", b"hash", self._sign_err)

    def sign_modify_order(self, **kwargs):
        self.sign_modify_calls.append(kwargs)
        return (2, b"tx_info_modify", b"hash", self._sign_err)

    def sign_cancel_order(self, **kwargs):
        self.sign_cancel_calls.append(kwargs)
        return (3, b"tx_info_cancel", b"hash", self._sign_err)

    async def send_tx_batch(self, tx_types, tx_infos):
        self.send_tx_batch_calls.append((tx_types, tx_infos))
        resp = type("Resp", (), {
            "code": self._send_code,
            "message": self._send_message,
            "volume_quota_remaining": "100",
        })()
        return resp


class DummyCancelClient:
    """Client with cancel_all_orders (different semantics from cancel_order)."""
    def __init__(self):
        self.cancel_all_calls = []

    async def cancel_all_orders(self, *args, **kwargs):
        self.cancel_all_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "response", None


def make_mid_price_df(n_seconds=900, start_price=100.0, drift=0.0, noise_std=0.01):
    """Create a synthetic mid-price DataFrame with DatetimeIndex at 1s frequency."""
    rng = np.random.default_rng(42)
    index = pd.date_range("2025-01-01", periods=n_seconds, freq="s")
    log_returns = drift / n_seconds + noise_std * rng.standard_normal(n_seconds) / np.sqrt(n_seconds)
    log_returns[0] = 0.0
    prices = start_price * np.exp(np.cumsum(log_returns))
    return pd.DataFrame({"mid_price": prices}, index=index)


def make_trades_df(n_trades=50, base_price=100.0, price_noise=0.1, side="buy"):
    """Create a synthetic trades DataFrame with DatetimeIndex."""
    rng = np.random.default_rng(42)
    index = pd.date_range("2025-01-01", periods=n_trades, freq="10s")
    prices = base_price + rng.uniform(-price_noise, price_noise, n_trades)
    sizes = rng.uniform(0.01, 1.0, n_trades)
    return pd.DataFrame({"price": prices, "size": sizes, "side": side}, index=index)
