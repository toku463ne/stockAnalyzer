import __init__
import unittest
import data_getter.yfinance_getter as yfinance_getter
import data_getter.my_getter as my_getter
import db.mysql as mydb
from datetime import datetime

import pandas as pd

class TestYfMyGetter(unittest.TestCase):
    def test_yfmygetter(self):
        yg = yfinance_getter.YFinanceGetter("MSFT", "D")
        mg = my_getter.MyGetter(yg, "mytest", is_dgtest=True)
        my = mydb.MySqlDB()
        
        st = datetime(year=2021, month=11, day=1, hour=9).timestamp()
        ed = datetime(year=2021, month=12, day=1, hour=9).timestamp()
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(pd.to_datetime(dt[0]).day, 1)
        self.assertEqual(pd.to_datetime(dt[-2]).day, 30)
        self.assertEqual(22, len(ep))
        self.assertEqual(35, my.countTable(mg.tableName))
        
        st = datetime(year=2021, month=11, day=15, hour=9).timestamp()
        ed = datetime(year=2021, month=12, day=14, hour=9).timestamp()
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(21, len(ep))
        self.assertEqual(pd.to_datetime(dt[0]).day, 15)
        self.assertEqual(pd.to_datetime(dt[-1]).day, 14)
        self.assertEqual(44, my.countTable(mg.tableName))
        
        st = datetime(year=2021, month=10, day=11, hour=9).timestamp()
        ed = datetime(year=2021, month=10, day=15, hour=9).timestamp()
        (ep, dt, o, h, l, c, v) = mg.getPrices(st, ed)
        self.assertEqual(pd.to_datetime(dt[0]).day, 11)
        self.assertEqual(pd.to_datetime(dt[-1]).day, 15)
        self.assertEqual(59, my.countTable(mg.tableName))
        

        mg.drop()
        
        #print(p)

if __name__ == "__main__":
    unittest.main()
