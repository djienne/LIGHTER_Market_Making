from contextlib import contextmanager

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
