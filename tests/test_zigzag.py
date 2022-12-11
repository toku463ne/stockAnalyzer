import unittest
import __init__
from ticker.zigzag import Zigzag
from datetime import datetime
import pandas as pd
import ticker
from consts import *

class TestZigzag(unittest.TestCase):
    def test_zigzag(self):
        st = datetime(year=2022, month=2, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=4, day=1, hour=9).timestamp()
        
        t = Zigzag("^N225", "D", st, ed, size=5)
        ep = datetime(year=2022, month=2, day=17, hour=9).timestamp()
        self.assertTrue(t.tick(ep))
        self.assertEqual(t.err, TICKER_NODATA)
        

        self.assertTrue(t.tick())
        (ep, dt, d, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 10)
        self.assertEqual(pd.to_datetime(dt).month, 2)
        self.assertTrue(ep > 0.0)
        self.assertTrue(p > 0.0)
        

        self.assertTrue(t.tick())
        (ep, dt, d, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 10)
        self.assertEqual(pd.to_datetime(dt).month, 2)
        
        ep = datetime(year=2022, month=3, day=3, hour=9).timestamp()
        self.assertTrue(t.tick(ep))
        (ep, dt, d, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 24)
        self.assertEqual(pd.to_datetime(dt).month, 2)
        
        self.assertTrue(t.tick())
        self.assertTrue(t.tick())
        self.assertTrue(t.tick())
        (ep_err, dt, d, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 1)
        self.assertEqual(pd.to_datetime(dt).month, 3)
        



if __name__ == "__main__":
    unittest.main()
