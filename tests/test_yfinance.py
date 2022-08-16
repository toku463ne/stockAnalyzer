import __init__
import unittest
import data_getter.yfinance_getter as yfinance_getter
from datetime import datetime

class TestYFinance(unittest.TestCase):
    def test_yfinance(self):
        yg = yfinance_getter.YFinanceGetter("MSFT", "D")
        
        st = datetime(year=2021, month=11, day=1).timestamp()
        ed = datetime(year=2021, month=12, day=1).timestamp()
        p = yg.getPrices(st, ed)
        self.assertEqual(['EP', 'DT', 'O', 'H', 'L', 'C', 'V'], list(p.columns.values))
        self.assertEqual(21, len(p.index))
        #print(p)

if __name__ == "__main__":
    unittest.main()
