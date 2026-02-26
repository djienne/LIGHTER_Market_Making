import unittest

import market_maker_v2 as mm


class TestTrimException(unittest.TestCase):
    def test_multiline(self):
        exc = Exception("line1\nline2\nerror")
        self.assertEqual(mm.trim_exception(exc), "error")

    def test_single_line(self):
        exc = Exception("simple error")
        self.assertEqual(mm.trim_exception(exc), "simple error")
