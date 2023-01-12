import unittest
import __init__
from ticker.zigzag import Zigzag
from datetime import datetime
import pandas as pd
import ticker
from consts import *
import lib

class TestZigzag(unittest.TestCase):
    def test_getData(self):
        st = lib.dt2epoch(datetime(year=2022, month=6, day=1, hour=9))
        ed = lib.dt2epoch(datetime(year=2022, month=11, day=1, hour=9))
        t = Zigzag("2160.T", "D", st, ed, size=5)
        ep = lib.dt2epoch(datetime(year=2022, month=9, day=29, hour=9))
        self.assertTrue(t.tick(ep))
        self.assertEqual(t.zz_dt[t.curr_zi].month, 9)
        self.assertEqual(t.zz_dt[t.curr_zi].day, 7)

        (_, dts, dirs, _) = t.getData(n=5, zz_mode=ZZ_MODE_RETURN_MIDDLE)
        self.assertEqual(len(dts), 5)
        self.assertEqual(dts[-1].month, 9)
        self.assertEqual(dts[-1].day, 7)
        self.assertEqual(dirs[-1], -1)
        self.assertEqual(dts[-2].month, 9)
        self.assertEqual(dts[-2].day, 1)
        self.assertEqual(dirs[-2], -1)
        
        (_, dts, dirs, _) = t.getData(n=5, zz_mode=ZZ_MODE_RETURN_COMPLETED)
        self.assertEqual(len(dts), 4)
        self.assertEqual(dts[-1].month, 8)
        self.assertEqual(dts[-1].day, 16)
        self.assertEqual(dirs[-1], 2)
        self.assertEqual(dts[-2].month, 7)
        self.assertEqual(dts[-2].day, 4)
        self.assertEqual(dirs[-2], -2)

        (_, dts, dirs, _) = t.getData(n=5, zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE)
        self.assertEqual(len(dts), 5)
        self.assertEqual(dts[-1].month, 9)
        self.assertEqual(dts[-1].day, 7)
        self.assertEqual(dirs[-1], -1)
        self.assertEqual(dts[-2].month, 8)
        self.assertEqual(dts[-2].day, 16)
        self.assertEqual(dirs[-2], 2)

        self.assertTrue(t.tick())
        (_, dts, dirs, _) = t.getData(n=5, zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE)
        self.assertEqual(len(dts), 5)
        self.assertEqual(dts[-1].month, 9)
        self.assertEqual(dts[-1].day, 28)
        self.assertEqual(dirs[-1], -2)
        


    def test_zigzag(self):
        st = lib.dt2epoch(datetime(year=2022, month=6, day=1, hour=9))
        ed = lib.dt2epoch(datetime(year=2022, month=11, day=1, hour=9))
        t = Zigzag("2160.T", "D", st, ed, size=5)
        self.assertEqual(t.zz_dt[3].month, 7)
        self.assertEqual(t.zz_dt[3].day, 4)
        self.assertEqual(t.zz_dirs[3], -2)
        self.assertEqual(t.zz_dt[4].month, 7)
        self.assertEqual(t.zz_dt[4].day, 11)
        self.assertEqual(t.zz_dirs[4], 1)
        self.assertEqual(t.zz_dt[8].month, 8)
        self.assertEqual(t.zz_dt[8].day, 12)
        self.assertEqual(t.zz_dirs[8], -1)
        self.assertEqual(t.zz_dt[9].month, 8)
        self.assertEqual(t.zz_dt[9].day, 16)
        self.assertEqual(t.zz_dirs[9], 2)
        

        st = datetime(year=2022, month=9, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=12, day=1, hour=9).timestamp()        
        t = Zigzag("^N225", "D", st, ed, size=5)
        self.assertEqual(t.zz_dt[3].month, 10)
        self.assertEqual(t.zz_dt[3].day, 13)
        self.assertEqual(t.zz_dirs[3], -2)
        self.assertEqual(t.zz_dt[4].month, 10)
        self.assertEqual(t.zz_dt[4].day, 19)
        self.assertEqual(t.zz_dirs[4], 1)
        self.assertEqual(t.zz_dt[7].month, 11)
        self.assertEqual(t.zz_dt[7].day, 8)
        self.assertEqual(t.zz_dirs[7], 1)
        self.assertEqual(t.zz_dt[8].month, 11)
        self.assertEqual(t.zz_dt[8].day, 11)
        self.assertEqual(t.zz_dirs[8], 1)
        
        
        st = datetime(year=2021, month=2, day=1, hour=9).timestamp()
        ed = datetime(year=2021, month=7, day=1, hour=9).timestamp()        
        t = Zigzag("^N225", "D", st, ed, size=5)
        self.assertEqual(t.zz_dt[1].month, 2)
        self.assertEqual(t.zz_dt[1].day, 26)
        self.assertEqual(t.zz_dirs[1], -1)
        self.assertEqual(t.zz_dt[2].month, 3)
        self.assertEqual(t.zz_dt[2].day, 5)
        self.assertEqual(t.zz_dirs[2], -2)
        self.assertEqual(t.zz_dt[4].month, 3)
        self.assertEqual(t.zz_dt[4].day, 24)
        self.assertEqual(t.zz_dirs[4], -2)
        self.assertEqual(t.zz_dt[5].month, 3)
        self.assertEqual(t.zz_dt[5].day, 29)
        self.assertEqual(t.zz_dirs[5], 1)
        

        st = datetime(year=2020, month=4, day=1, hour=9).timestamp()
        ed = datetime(year=2020, month=8, day=1, hour=9).timestamp()        
        t = Zigzag("1973.T", "D", st, ed, size=5)
        self.assertEqual(t.zz_dt[1].month, 4)
        self.assertEqual(t.zz_dt[1].day, 24)
        self.assertEqual(t.zz_dirs[1], -2)
        self.assertEqual(t.zz_dt[2].month, 4)
        self.assertEqual(t.zz_dt[2].day, 30)
        self.assertEqual(t.zz_dirs[2], 1)
        self.assertEqual(t.zz_dt[5].month, 6)
        self.assertEqual(t.zz_dt[5].day, 2)
        self.assertEqual(t.zz_dirs[5], 2)
        

        st = datetime(year=2021, month=11, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=4, day=1, hour=9).timestamp()
        
        t = Zigzag("^N225", "D", st, ed, size=5)

        ep = datetime(year=2021, month=11, day=11, hour=9).timestamp()
        self.assertTrue(t.tick(ep))
        self.assertEqual(t.err, TICKER_ERR_NONE)

        ep = datetime(year=2022, month=2, day=18, hour=9).timestamp()
        self.assertTrue(t.tick(ep))
        
        (ep, dt, d, p) = t.data
        self.assertEqual(dt.day, 15)
        self.assertEqual(dt.month, 2)

        """
        for _ in range(1,9):
            self.assertTrue(t.tick())
        (ep, dt, d, p) = t.data
        self.assertEqual(dt.day, 24)
        self.assertEqual(dt.month, 2)
        """

        

if __name__ == "__main__":
    unittest.main()
