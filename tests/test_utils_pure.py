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


