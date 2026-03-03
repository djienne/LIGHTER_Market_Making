import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestToRawPrice(unittest.TestCase):
    """Tests for _to_raw_price float-to-SDK-integer conversion."""

    def test_btc_price_conversion(self):
        """BTC price_tick=0.1: 50000.0 → 500000, 50000.1 → 500001."""
        with temp_mm_attrs(_PRICE_TICK_FLOAT=0.1):
            self.assertEqual(mm._to_raw_price(50000.0), 500000)
            self.assertEqual(mm._to_raw_price(50000.1), 500001)
            self.assertEqual(mm._to_raw_price(99999.9), 999999)

    def test_eth_price_conversion(self):
        """ETH price_tick=0.01: 3000.01 → 300001."""
        with temp_mm_attrs(_PRICE_TICK_FLOAT=0.01):
            self.assertEqual(mm._to_raw_price(3000.01), 300001)
            self.assertEqual(mm._to_raw_price(3000.00), 300000)

    def test_returns_int(self):
        with temp_mm_attrs(_PRICE_TICK_FLOAT=0.1):
            result = mm._to_raw_price(100.0)
            self.assertIsInstance(result, int)

    def test_raises_when_tick_not_set(self):
        with temp_mm_attrs(_PRICE_TICK_FLOAT=0.0):
            with self.assertRaises(ValueError):
                mm._to_raw_price(100.0)

    def test_round_trip_lossless(self):
        """price → raw → price must be lossless for tick-aligned values."""
        tick = 0.1
        with temp_mm_attrs(_PRICE_TICK_FLOAT=tick):
            for price in [50000.0, 50000.1, 99999.9, 100.0, 0.1]:
                raw = mm._to_raw_price(price)
                reconstructed = raw * tick
                self.assertAlmostEqual(reconstructed, price, places=9)


class TestToRawAmount(unittest.TestCase):
    """Tests for _to_raw_amount float-to-SDK-integer conversion."""

    def test_btc_amount_conversion(self):
        """BTC amount_tick=0.00001: 0.0002 → 20, 1.0 → 100000."""
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=0.00001):
            self.assertEqual(mm._to_raw_amount(0.0002), 20)
            self.assertEqual(mm._to_raw_amount(1.0), 100000)
            self.assertEqual(mm._to_raw_amount(0.00001), 1)

    def test_sol_amount_conversion(self):
        """SOL amount_tick=0.001: 0.5 → 500, 1.234 → 1234."""
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=0.001):
            self.assertEqual(mm._to_raw_amount(0.5), 500)
            self.assertEqual(mm._to_raw_amount(1.234), 1234)

    def test_returns_int(self):
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=0.00001):
            result = mm._to_raw_amount(0.5)
            self.assertIsInstance(result, int)

    def test_raises_when_tick_not_set(self):
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=0.0):
            with self.assertRaises(ValueError):
                mm._to_raw_amount(0.5)

    def test_round_trip_lossless(self):
        """amount → raw → amount must be lossless for tick-aligned values."""
        tick = 0.00001
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=tick):
            for amount in [0.00001, 0.0002, 0.01, 1.0, 0.12345]:
                raw = mm._to_raw_amount(amount)
                reconstructed = raw * tick
                self.assertAlmostEqual(reconstructed, amount, places=9)
