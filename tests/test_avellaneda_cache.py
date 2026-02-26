import unittest

import market_maker_v2 as mm


class TestAvellanedaCache(unittest.TestCase):
    def test_creation_and_access(self):
        cache = mm.AvellanedaCache(
            gamma=0.5, sigma=0.01, k_bid=1.0, k_ask=1.5, time_remaining=0.25,
        )
        self.assertEqual(cache.gamma, 0.5)
        self.assertEqual(cache.sigma, 0.01)
        self.assertEqual(cache.k_bid, 1.0)
        self.assertEqual(cache.k_ask, 1.5)
        self.assertEqual(cache.time_remaining, 0.25)

    def test_immutability(self):
        cache = mm.AvellanedaCache(
            gamma=0.5, sigma=0.01, k_bid=1.0, k_ask=1.5, time_remaining=0.25,
        )
        with self.assertRaises(AttributeError):
            cache.gamma = 1.0
