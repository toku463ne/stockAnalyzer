import __init__
import unittest
from datetime import datetime
import lib.data_import as imp

class TestDataImport(unittest.TestCase):
    def test_random(self):
        codes = imp.get_random_codes(2)
        st = datetime(year=2022, month=2, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=2, day=10, hour=9).timestamp()

        imp.import_codes(codes, st, ed, "D", 1)

        for code in codes:
            cnt = imp.get_count(code, st, ed, "D")
            self.assertGreater(cnt, 0)

    def test_json(self):
        imp.run("tests/data_imptest.json")    


if __name__ == "__main__":
    unittest.main()

