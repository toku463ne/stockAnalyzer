import __init__
import unittest
from datetime import datetime
from db.mysql import MySqlDB
import pandas as pd

from analyzer.zz.code_manager import CodeManager
import lib

redb = MySqlDB()
updb = MySqlDB()


def strStEd2ep(start, end):
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"
    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)
    return startep, endep

start = "2019-01-01"
end = "2020-12-31"
(startep, endep) = strStEd2ep(start, end)

config = {
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": [],
    "startep": startep,
    "endep": endep,
    "use_master": False
}

class TestDataManager(unittest.TestCase):
    def test_code_manager(self):
        config["codenames"]=["1301.T", "1928.T"]
        t = "anal_zzcodes_D"
        config["recreate"] = True
        c = CodeManager(config)
        
        self.assertEqual(redb.countTable(t), 0)

        c.register()
        self.assertEqual(redb.countTable(t, ["codename = '1301.T'"]), 1)
        self.assertEqual(redb.countTable(t, ["codename = '1928.T'"]), 1)
        self.assertGreater(redb.select1value(t, "nbars", ["codename = '1301.T'"]), 0)
        self.assertGreater(redb.select1value(t, "min_nth_volume", ["codename = '1301.T'"]), 0)



    def test_normalizeItem(self):
        from analyzer.zz.peaks_manager import PeaksManager
        a = PeaksManager(config)
         
        ep = [1,2,3,4,5,6,7]
        prices = [1000, 1100, 1200, 1300, 1400,1500,1600]
        vals = a._normalizeItem(ep, prices)
        (nep, nprices) = a.devideNormalizedVals(vals)
        self.assertEqual(nep[0], 0)
        self.assertEqual(nep[-2], 1)
        self.assertGreater(nep[-1], 1)
        self.assertEqual(nprices[0], 0)
        self.assertEqual(nprices[-2], 1)
        self.assertGreater(nprices[-1], 1)

        ep = [1,2,3,4,5,6,7]
        prices = [1000, 1100, 1400, 1300, 900,1000,1100]
        vals = a._normalizeItem(ep, prices)
        (nep, nprices) = a.devideNormalizedVals(vals)
        self.assertEqual(nep[0], 0)
        self.assertEqual(nep[-2], 1)
        self.assertGreater(nep[-1], 1)
        self.assertEqual(nprices[4], 0)
        self.assertEqual(nprices[2], 1)
        self.assertGreater(nprices[-1], 0)

    def test_peaks_manager(self):
        from analyzer.zz.peaks_manager import PeaksManager
        from ticker.zigzag import Zigzag

        
        config["recreate"] = True
        config["codenames"]=["1301.T", "1928.T"]
        
        m = PeaksManager(config)
        m.dropAllZzTables()
        m.register()
        
        tz = "tick_zigzag_D_5_2"
        (cnt,) = redb.select1rec("select count(*) from %s where codename = '1928.T';" % tz)
        self.assertGreater(cnt, 0)

        self.assertGreater(redb.countTable(tz, ["codename = '1928.T'", "dir = -2"]), 0)
        self.assertGreater(redb.countTable(tz, ["codename = '1928.T'", "dir = -1"]), 0)
        self.assertGreater(redb.countTable(tz, ["codename = '1928.T'", "dir = 1"]), 0)
        self.assertGreater(redb.countTable(tz, ["codename = '1928.T'", "dir = 2"]), 0)

        sql = "select startep, endep from tick_tableeps where table_name = '%s' and codename = '1301.T';" % tz
        (db_start, db_end) = redb.select1rec(sql)
        self.assertEqual(db_start, startep)
        self.assertGreater(db_end, endep)

        tp = "anal_zzpeaks_D_5_5"
        (ma,) = redb.select1rec("select max(x0) from %s;" % tp)
        self.assertEqual(ma, 0.0)
        (cnt1,cnt2) = redb.select1rec("select min(x5), max(x5) from %s;" % tp)
        self.assertEqual(cnt1, 1.0)
        self.assertEqual(cnt2, 1.0)
        
        (cnt,) = redb.select1rec("select count(*) from %s where codename = '1928.T';" % tp)
        self.assertGreater(cnt, 0)





if __name__ == "__main__":
    unittest.main()
