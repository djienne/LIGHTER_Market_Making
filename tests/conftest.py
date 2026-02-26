"""
conftest.py – mock the ``lighter`` C-library dependency so the test suite
can run on any platform (Windows native, CI runners without the libc shim, etc.).

This file is loaded by pytest **before** any test module is collected, so the
mock is in place before ``import market_maker_v2`` or ``import utils`` triggers
``import lighter``.

No real network calls, wallets, or funds are ever used.
"""

import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Build a fake ``lighter`` package that satisfies every attribute used at
# module-import time by market_maker_v2.py and utils.py.
# ---------------------------------------------------------------------------

_lighter = types.ModuleType("lighter")

# lighter.SignerClient with the class-level constants the code references
_signer = MagicMock()
_signer.ORDER_TYPE_LIMIT = 0
_signer.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
_signer.CANCEL_ALL_TIF_IMMEDIATE = 2
_signer.CROSS_MARGIN_MODE = "cross"
_signer.ISOLATED_MARGIN_MODE = "isolated"
_lighter.SignerClient = _signer

# lighter.ApiClient, Configuration, OrderApi, AccountApi
_lighter.ApiClient = MagicMock
_lighter.Configuration = MagicMock
_lighter.OrderApi = MagicMock
_lighter.AccountApi = MagicMock

# lighter.exceptions.ApiException
_exceptions = types.ModuleType("lighter.exceptions")
_exceptions.ApiException = type("ApiException", (Exception,), {})
_lighter.exceptions = _exceptions

# Register all sub-modules so ``from lighter.exceptions import …`` works
sys.modules["lighter"] = _lighter
sys.modules["lighter.exceptions"] = _exceptions
sys.modules["lighter.signer_client"] = types.ModuleType("lighter.signer_client")
sys.modules["lighter.libc"] = types.ModuleType("lighter.libc")

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so ``import market_maker_v2``,
# ``import utils``, ``import volatility``, etc. resolve correctly.
# ---------------------------------------------------------------------------
import pathlib

_project_root = str(pathlib.Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Also add tests/ dir so ``from _helpers import …`` works
_tests_dir = str(pathlib.Path(__file__).resolve().parent)
if _tests_dir not in sys.path:
    sys.path.insert(0, _tests_dir)
