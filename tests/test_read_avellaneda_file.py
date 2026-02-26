import json
import os
import tempfile
import unittest

import market_maker_v2 as mm


class TestReadAvellanedaFileSync(unittest.TestCase):
    def test_reads_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "params.json")
            data = {"optimal_parameters": {"gamma": 0.5}, "market_data": {"sigma": 0.01}}
            with open(path, "w") as f:
                json.dump(data, f)

            result = mm._read_avellaneda_file_sync([path])
            self.assertEqual(result["optimal_parameters"]["gamma"], 0.5)
            self.assertEqual(result["market_data"]["sigma"], 0.01)

    def test_returns_none_missing_file(self):
        result = mm._read_avellaneda_file_sync(["/nonexistent/path/params.json"])
        self.assertIsNone(result)

    def test_returns_none_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bad.json")
            with open(path, "w") as f:
                f.write("{invalid json!!")

            result = mm._read_avellaneda_file_sync([path])
            self.assertIsNone(result)
