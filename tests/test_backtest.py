import unittest

import numpy as np
import pandas as pd

from _helpers import make_mid_price_df, make_trades_df
import backtest


class TestRunBacktest(unittest.TestCase):
    def _make_data(self, n_seconds=900, base_price=100.0):
        mid_df = make_mid_price_df(n_seconds=n_seconds, start_price=base_price, noise_std=0.005)
        mid_prices = mid_df["mid_price"]
        buy_trades = make_trades_df(n_trades=30, base_price=base_price, price_noise=0.5, side="buy")
        sell_trades = make_trades_df(n_trades=30, base_price=base_price, price_noise=0.5, side="sell")
        return mid_prices, buy_trades, sell_trades

    def test_no_trades_zero_pnl(self):
        mid_df = make_mid_price_df(n_seconds=900, start_price=100.0, noise_std=0.001)
        mid_prices = mid_df["mid_price"]
        # Empty trade DataFrames
        empty_buys = pd.DataFrame({"price": [], "size": []}, index=pd.DatetimeIndex([]))
        empty_sells = pd.DataFrame({"price": [], "size": []}, index=pd.DatetimeIndex([]))

        result = backtest.run_backtest(
            mid_prices, empty_buys, empty_sells,
            gamma=0.5, k_bid=1.0, k_ask=1.0, sigma=0.01,
            window_minutes=15, horizon_minutes=60,
        )
        # With no trades to trigger fills, PnL should be 0 throughout
        self.assertAlmostEqual(result["pnl"][-1], 0.0, places=5)

    def test_returns_expected_keys(self):
        mid_prices, buy_trades, sell_trades = self._make_data()
        result = backtest.run_backtest(
            mid_prices, buy_trades, sell_trades,
            gamma=0.5, k_bid=1.0, k_ask=1.0, sigma=0.01,
            window_minutes=15, horizon_minutes=60,
        )
        expected_keys = {"pnl", "x", "q", "spread_bid", "spread_ask", "r", "r_a", "r_b"}
        self.assertEqual(set(result.keys()), expected_keys)


class TestCalculateFinalQuotes(unittest.TestCase):
    def test_ask_above_bid_in_final_quotes(self):
        mid_df = make_mid_price_df(n_seconds=900, start_price=100.0, noise_std=0.005)
        period_start = mid_df.index[0]
        period_end = mid_df.index[-1]

        result = backtest.calculate_final_quotes(
            gamma=0.5, sigma=0.01,
            A_bid=1.0, k_bid=1.0,
            A_ask=1.0, k_ask=1.0,
            window_minutes=15, mid_price_df=mid_df,
            ma_window=1, period_start=period_start, period_end=period_end,
            ticker="TEST", time_horizon_days=0.5,
        )
        self.assertGreater(result["limit_orders"]["ask_price"], result["limit_orders"]["bid_price"])
