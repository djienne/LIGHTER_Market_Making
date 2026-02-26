import json
import math
import os
import tempfile
import unittest

import utils


class TestFiniteNonneg(unittest.TestCase):
    def test_positive_float(self):
        self.assertTrue(utils._finite_nonneg(5.0))

    def test_negative_float(self):
        self.assertFalse(utils._finite_nonneg(-1.0))

    def test_nan(self):
        self.assertFalse(utils._finite_nonneg(float("nan")))

    def test_non_numeric(self):
        self.assertFalse(utils._finite_nonneg("abc"))


class TestGetFallbackTickSize(unittest.TestCase):
    def test_btc(self):
        self.assertEqual(utils.get_fallback_tick_size("BTC"), 0.1)

    def test_paxg(self):
        self.assertEqual(utils.get_fallback_tick_size("PAXG"), 0.01)

    def test_unknown(self):
        self.assertEqual(utils.get_fallback_tick_size("UNKNOWN"), 0.01)


class TestSaveAvellanedaParamsAtomic(unittest.TestCase):
    def test_valid_params_writes_file(self):
        params = {
            "limit_orders": {"delta_a": 0.5, "delta_b": 0.3},
            "market_data": {"sigma": 0.01},
        }
        with tempfile.TemporaryDirectory() as tmp:
            original_dir = utils.PARAMS_DIR
            try:
                utils.PARAMS_DIR = tmp
                ok = utils.save_avellaneda_params_atomic(params, "TEST")
                self.assertTrue(ok)

                out_path = os.path.join(tmp, "avellaneda_parameters_TEST.json")
                self.assertTrue(os.path.exists(out_path))

                with open(out_path, "r") as f:
                    loaded = json.load(f)
                self.assertEqual(loaded["limit_orders"]["delta_a"], 0.5)
                self.assertEqual(loaded["limit_orders"]["delta_b"], 0.3)
            finally:
                utils.PARAMS_DIR = original_dir

    def test_invalid_params_returns_false(self):
        params = {
            "limit_orders": {"delta_a": -1.0, "delta_b": 0.3},
        }
        ok = utils.save_avellaneda_params_atomic(params, "BAD")
        self.assertFalse(ok)
