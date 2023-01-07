import unittest
import __init__
from ticker.sma import SMA
from datetime import datetime
import pandas as pd
from consts import *
import lib

class TestSma(unittest.TestCase):
    def test_sma(self):
        st = lib.dt2epoch(datetime(year=2021, month=11, day=1))
        ed = lib.dt2epoch(datetime(year=2021, month=12, day=1))
        
        t = SMA("^N225", "D", st, ed, span=5, buffNbars=0)
        self.assertTrue(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 8)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        self.assertTrue(ep > 0.0)
        self.assertTrue(p > 0.0)
        
        self.assertTrue(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 9)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep + t.unitsecs*2))
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 11)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep - t.unitsecs*6))
        (ep_err, dt, p) = t.data
        self.assertEqual(ep_err, 0)
        self.assertEqual(t.err, TICKER_NODATA)
        

        self.assertTrue(t.tick(ep + t.unitsecs*20))
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 1)
        self.assertEqual(pd.to_datetime(dt).month, 12)
        
        self.assertFalse(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(ep, 0)
        self.assertEqual(t.err, TICKER_ERR_EOF)
        


if __name__ == "__main__":
    unittest.main()
