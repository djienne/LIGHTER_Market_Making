import unittest
from unittest import mock

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestNextClientOrderIndex(unittest.TestCase):
    def test_next_client_order_index_monotonic(self):
        with temp_mm_attrs(last_client_order_index=0):
            with mock.patch.object(mm.time, "time_ns", return_value=1234):
                first = mm.next_client_order_index()
                second = mm.next_client_order_index()

            self.assertEqual(first, 1234)
            self.assertEqual(second, 1235)
