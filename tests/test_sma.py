import unittest
import __init__
from ticker.sma import SMA
from datetime import datetime
import pandas as pd
import ticker.tick as tick

class TestSma(unittest.TestCase):
    def test_sma(self):
        st = datetime(year=2021, month=11, day=1).timestamp()
        ed = datetime(year=2021, month=12, day=1).timestamp()
        
        t = SMA("MSFT", "D", st, ed, span=5)
        self.assertTrue(t.tick())
        (ep, dt, p) = t.data
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
        self.assertEqual(t.err, tick.ERR_NODATA)
        

        self.assertTrue(t.tick(ep + t.unitsecs*20))
        (ep, dt, p) = t.data
        self.assertEqual(pd.to_datetime(dt).day, 30)
        self.assertEqual(pd.to_datetime(dt).month, 11)
        
        self.assertFalse(t.tick())
        (ep, dt, p) = t.data
        self.assertEqual(ep, 0)
        self.assertEqual(t.err, tick.ERR_EOF)
        


if __name__ == "__main__":
    unittest.main()
