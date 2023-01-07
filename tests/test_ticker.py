import unittest
import __init__
from ticker import Ticker
import ticker
from datetime import datetime
import pandas as pd
from consts import *

class TestTicker(unittest.TestCase):
    def test_ticker(self):
        st = datetime(year=2021, month=11, day=1).timestamp()
        ed = datetime(year=2021, month=12, day=1).timestamp()
        
        t = Ticker("^N225", "D", st, ed, buffNbars=0)
        self.assertTrue(t.tick())
        (ep, dt, o, h, l, c, v) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 1)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        self.assertTrue(ep > 0.0)
        self.assertTrue(o > 0.0)
        self.assertTrue(h > 0.0)
        self.assertTrue(l > 0.0)
        self.assertTrue(c > 0.0)
        self.assertTrue(v > 0.0)
        
        self.assertTrue(t.tick())
        (ep, dt, o, h, l, c, v) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 2)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep + t.unitsecs*2))
        (ep, dt, o, h, l, c, v) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 4)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertTrue(t.tick(ep - t.unitsecs*4))
        (ep_err, dt, o, h, l, c, v) = t.data
        self.assertEqual(ep_err, 0)
        self.assertEqual(t.err, TICKER_NODATA)
        

        self.assertTrue(t.tick(ep + t.unitsecs*26))
        (ep, dt, o, h, l, c, v) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 30)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertFalse(t.tick())
        (ep, dt, o, h, l, c, v) = t.data
        self.assertEqual(ep, 0)
        self.assertEqual(t.err, TICKER_ERR_EOF)
        


if __name__ == "__main__":
    unittest.main()
