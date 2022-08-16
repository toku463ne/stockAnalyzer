import unittest
import __init__
from ticker.zigzag import Zigzag
from datetime import datetime
import pandas as pd
import ticker.ticker as ticker

class TestZigzag(unittest.TestCase):
    def test_zigzag(self):
        st = datetime(year=2021, month=11, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=4, day=1, hour=9).timestamp()
        
        t = Zigzag("MSFT", "D", st, ed, size=5)
        self.assertTrue(t.tick())
        (ep, dt, d, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 5)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        self.assertTrue(ep > 0.0)
        self.assertTrue(p > 0.0)
        
        self.assertTrue(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 8)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep + t.unitsecs*2))
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 10)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep - t.unitsecs*6))
        (ep_err, dt, p) = t.data
        self.assertEqual(ep_err, 0)
        self.assertEqual(t.err, ticker.ERR_NODATA)
        

        self.assertTrue(t.tick(ep + t.unitsecs*20))
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 30)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertFalse(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(ep, 0)
        self.assertEqual(t.err, ticker.ERR_EOF)
        


if __name__ == "__main__":
    unittest.main()
