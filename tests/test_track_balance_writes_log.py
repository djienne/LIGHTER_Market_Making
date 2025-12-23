import asyncio
import os
import tempfile
import unittest
from unittest import mock

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestTrackBalanceWritesLog(unittest.IsolatedAsyncioTestCase):
    async def test_track_balance_writes_log(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with temp_mm_attrs(
                LOG_DIR=tmp_dir,
                current_position_size=0,
                portfolio_value=123.45,
            ):
                with mock.patch.object(
                    mm.asyncio,
                    "sleep",
                    new=mock.AsyncMock(side_effect=asyncio.CancelledError),
                ):
                    with self.assertRaises(asyncio.CancelledError):
                        await mm.track_balance()

                log_path = os.path.join(tmp_dir, "balance_log.txt")
                self.assertTrue(os.path.exists(log_path))
                with open(log_path, "r", encoding="utf-8") as handle:
                    content = handle.read()
                self.assertIn("Portfolio Value: $123.45", content)
