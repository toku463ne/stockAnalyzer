import __init__
import unittest
import data_getter.yfinance_getter as yfinance_getter
import data_getter.mydf_getter as mydf_getter
import db.mysql as mydb
from datetime import datetime

class TestYfMyGetter(unittest.TestCase):
    def test_yfmygetter(self):
        yg = yfinance_getter.YFinanceGetter("MSFT", "D")
        mg = mydf_getter.MyDfGetter(yg, "test", is_dgtest=True)
        my = mydb.MySqlDB()
        
        self.assertTrue(my.tableExists("ohlcvinfo"))

        sql = "select tablename from ohlcvinfo where tablename = '%s'" % mg.tableName
        (tablename,) = my.select1rec(sql)
        self.assertEqual(tablename, mg.tableName)

        st = datetime(year=2021, month=11, day=1, hour=9).timestamp()
        ed = datetime(year=2021, month=12, day=1, hour=9).timestamp()
        p = mg.getPrices(st, ed)
        self.assertEqual(['EP', 'DT', 'O', 'H', 'L', 'C', 'V'], list(p.columns.values))
        self.assertEqual(p.iloc[0]['DT'].day, 1)
        self.assertEqual(p.iloc[-2]['DT'].day, 30)
        self.assertEqual(22, len(p.index))
        self.assertEqual(35, my.countTable(mg.tableName))
        
        st = datetime(year=2021, month=11, day=15, hour=9).timestamp()
        ed = datetime(year=2021, month=12, day=14, hour=9).timestamp()
        p = mg.getPrices(st, ed)
        self.assertEqual(21, len(p.index))
        self.assertEqual(p.iloc[0]['DT'].day, 15)
        self.assertEqual(p.iloc[-1]['DT'].day, 14)
        self.assertEqual(44, my.countTable(mg.tableName))
        
        st = datetime(year=2021, month=10, day=11, hour=9).timestamp()
        ed = datetime(year=2021, month=10, day=15, hour=9).timestamp()
        p = mg.getPrices(st, ed)
        self.assertEqual(p.iloc[0]['DT'].day, 11)
        self.assertEqual(p.iloc[-1]['DT'].day, 15)
        self.assertEqual(59, my.countTable(mg.tableName))
        

        mg.drop()
        
        #print(p)

if __name__ == "__main__":
    unittest.main()
