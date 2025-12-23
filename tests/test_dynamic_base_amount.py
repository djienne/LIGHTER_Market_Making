import unittest
from decimal import Decimal

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestCalculateDynamicBaseAmount(unittest.IsolatedAsyncioTestCase):
    async def test_calculate_dynamic_base_amount(self):
        with temp_mm_attrs(available_capital=1000.0, AMOUNT_TICK_SIZE=Decimal("0.01")):
            result = await mm.calculate_dynamic_base_amount(200.0)
            self.assertAlmostEqual(float(result), 0.6, places=8)

            mm.available_capital = None
            fallback = await mm.calculate_dynamic_base_amount(200.0)
            self.assertEqual(fallback, Decimal(str(mm.BASE_AMOUNT)))
