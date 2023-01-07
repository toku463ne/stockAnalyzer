import __init__
import unittest
import data_getter.onmem_getter as onmem_getter
import data_getter.yfinance_getter as yfinance_getter
import db.mysql as mydb
from datetime import datetime
import lib

import pandas as pd

class TestYfMyGetter(unittest.TestCase):
    def test_onmem_getter(self):
        yg = yfinance_getter.YFinanceGetter("^N225", "D")
        mg = onmem_getter.OnMemGetter(yg, "onmemtest", 
                is_dgtest=True,memSize=40, extendSize=5)
        
        st = lib.dt2epoch(datetime(year=2021, month=11, day=1, hour=0))
        ed = lib.dt2epoch(datetime(year=2021, month=12, day=1, hour=0))
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(21, len(ep))
        self.assertEqual(21, len(mg.ep))
        self.assertEqual(1635724800, ep[0])
        self.assertEqual(1638316800, ep[-1])
        
        st = lib.dt2epoch(datetime(year=2021, month=11, day=10, hour=0))
        ed = lib.dt2epoch(datetime(year=2021, month=11, day=20, hour=0))
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(8, len(ep))
        self.assertEqual(21, len(mg.ep))
        self.assertEqual(1636502400, ep[0])
        self.assertEqual(1637280000, ep[-1])
        

        st = lib.dt2epoch(datetime(year=2021, month=11, day=15, hour=0))
        ed = lib.dt2epoch(datetime(year=2021, month=12, day=22, hour=0))
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(28, len(ep))
        self.assertEqual(37, len(mg.ep))
        self.assertEqual(1636934400, ep[0])
        self.assertEqual(1640131200, ep[-1])
        self.assertEqual(1640131200, mg.ep[-1])
        
        epoch1 = lib.dt2epoch(datetime(year=2021, month=11, day=8, hour=0))
        (epoch2, dt, o, h, l, c, v) = mg.getPrice(epoch1)
        self.assertEqual(epoch1, epoch2)
        
        st = lib.dt2epoch(datetime(year=2021, month=10, day=15, hour=0))
        ed = lib.dt2epoch(datetime(year=2021, month=11, day=15, hour=0))
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(20, len(ep))
        self.assertEqual(47, len(mg.ep))
        self.assertEqual(1634256000, ep[0])
        self.assertEqual(1636934400, ep[-1])
        
        st = lib.dt2epoch(datetime(year=2021, month=12, day=10, hour=0))
        ed = lib.dt2epoch(datetime(year=2021, month=12, day=25, hour=0))
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(12, len(ep))
        self.assertEqual(40, len(mg.ep))
        self.assertEqual(1635811200, mg.ep[0])
        self.assertEqual(1639094400, ep[0])
        self.assertEqual(1640304000, ep[-1])
        
        epoch1 = lib.dt2epoch(datetime(year=2022, month=1, day=7, hour=0))
        (epoch2, dt, o, h, l, c, v) = mg.getPrice(epoch1)
        self.assertEqual(epoch1, epoch2)
        self.assertEqual(40, len(mg.ep))
        


        mg.drop()
        
        #print(p)

if __name__ == "__main__":
    unittest.main()
