"""
conftest.py for live integration tests.

Unlike tests/conftest.py this does NOT mock the ``lighter`` module —
the whole point of these tests is to hit the real Lighter exchange
public endpoints and validate assumptions against live data.

The entire test suite is auto-skipped when:
  - The ``lighter`` native library cannot be imported (e.g. native Windows).
  - The exchange REST API is unreachable (no network / firewall).
"""

import os
import sys
import pathlib

import pytest
from dotenv import load_dotenv

# Ensure project root is on sys.path so ``from orderbook import …`` etc. work.
_project_root = str(pathlib.Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def pytest_configure(config):
    config.addinivalue_line("markers", "live: marks tests that hit live exchange endpoints (network required)")
    config.addinivalue_line("markers", "live_trading: marks tests that require authenticated trading credentials")


def pytest_collection_modifyitems(config, items):
    """Auto-skip every test in this directory when the live environment is unavailable."""
    # Check 1: lighter native lib must be importable
    try:
        import lighter  # noqa: F401
    except (ImportError, OSError):
        skip = pytest.mark.skip(reason="lighter native library not available (Windows?)")
        for item in items:
            item.add_marker(skip)
        return

    # Check 2: REST API must be reachable
    import requests
    try:
        resp = requests.get(
            "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks",
            timeout=10,
        )
        resp.raise_for_status()
    except Exception:
        skip = pytest.mark.skip(reason="Lighter REST API unreachable (no network?)")
        for item in items:
            item.add_marker(skip)
        return

    # Check 3: if any test in this session requires trading credentials, verify they exist
    has_trading_tests = any(item.get_closest_marker("live_trading") for item in items)
    if has_trading_tests:
        load_dotenv()
        if not os.getenv("API_KEY_PRIVATE_KEY") or not os.getenv("ACCOUNT_INDEX"):
            skip = pytest.mark.skip(reason="Trading credentials not in .env (API_KEY_PRIVATE_KEY, ACCOUNT_INDEX)")
            for item in items:
                if item.get_closest_marker("live_trading"):
                    item.add_marker(skip)
