import unittest

import numpy as np
import pandas as pd

from _helpers import make_mid_price_df
import volatility


class TestRollingVolatility(unittest.TestCase):
    def test_constant_price_near_zero_vol(self):
        # Flat price -> sigma should be very close to 0
        df = make_mid_price_df(n_seconds=1800, start_price=100.0, drift=0.0, noise_std=0.0)
        # Override to be truly constant
        df["mid_price"] = 100.0
        result = volatility.calculate_rolling_volatility(df, window_minutes=15, freq_str="15min")
        # All values should be 0 or NaN (constant price -> zero std)
        for val in result:
            if not pd.isna(val):
                self.assertAlmostEqual(val, 0.0, places=5)

    def test_trending_price_positive_vol(self):
        df = make_mid_price_df(n_seconds=1800, start_price=100.0, drift=0.05, noise_std=0.02)
        result = volatility.calculate_rolling_volatility(df, window_minutes=15, freq_str="15min")
        valid = [v for v in result if not pd.isna(v)]
        self.assertTrue(len(valid) > 0)
        for val in valid:
            self.assertGreater(val, 0.0)

    def test_returns_list_of_correct_length(self):
        # 1800 seconds = 2 full 15-min periods
        df = make_mid_price_df(n_seconds=1800, start_price=100.0, noise_std=0.01)
        result = volatility.calculate_rolling_volatility(df, window_minutes=15, freq_str="15min")
        num_periods = len(df.index.floor("15min").unique())
        self.assertEqual(len(result), num_periods)

    def test_fallback_when_few_periods(self):
        # Only 1 period worth of data - should not crash
        df = make_mid_price_df(n_seconds=900, start_price=100.0, noise_std=0.01)
        result = volatility.calculate_rolling_volatility(df, window_minutes=15, freq_str="15min")
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) >= 1)
