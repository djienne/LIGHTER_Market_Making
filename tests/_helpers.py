from contextlib import contextmanager

import numpy as np
import pandas as pd

import market_maker_v2 as mm


@contextmanager
def temp_mm_attrs(**overrides):
    originals = {}
    for name, value in overrides.items():
        originals[name] = getattr(mm, name)
        setattr(mm, name, value)
    try:
        yield
    finally:
        for name, value in originals.items():
            setattr(mm, name, value)


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


class DummyOrderClient:
    def __init__(self):
        self.create_order_calls = []

    async def create_order(self, *args, **kwargs):
        self.create_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", None


class DummyCancelClient:
    def __init__(self):
        self.cancel_all_calls = []

    async def cancel_all_orders(self, *args, **kwargs):
        self.cancel_all_calls.append({"args": args, "kwargs": kwargs})
        return None


class DummyModifyClient:
    """Client that supports modify_order, create_order, and cancel_order."""

    def __init__(self, modify_err=None):
        self.modify_order_calls = []
        self.create_order_calls = []
        self.cancel_order_calls = []
        self._modify_err = modify_err  # set to a string to simulate modify failure

    async def modify_order(self, *args, **kwargs):
        self.modify_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", self._modify_err

    async def create_order(self, *args, **kwargs):
        self.create_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", None

    async def cancel_order(self, *args, **kwargs):
        self.cancel_order_calls.append({"args": args, "kwargs": kwargs})
        return "tx", "hash", None


class DummyCancelSingleClient:
    """Client that supports cancel_order with configurable error/exception."""

    def __init__(self, cancel_err=None, raise_exc=None):
        self.cancel_order_calls = []
        self._cancel_err = cancel_err
        self._raise_exc = raise_exc

    async def cancel_order(self, *args, **kwargs):
        self.cancel_order_calls.append({"args": args, "kwargs": kwargs})
        if self._raise_exc:
            raise self._raise_exc
        return "tx", "hash", self._cancel_err


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
