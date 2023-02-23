import __init__
import unittest
from datetime import datetime
from db.mysql import MySqlDB
import pandas as pd

import analyzer.zz_analyzer as zz
import lib


"""
drop table anal_zzdata_D_5;
drop table anal_zzitems_D_5_5;
drop table anal_zzkmcandlestats_D_5_3;
drop table anal_zzkmgroups_D_5_5;
drop table anal_zzkmgroups_predicted_D_5_5;
drop table anal_zzkmstats_D_5_5;
drop table anal_zzcandles_D_5_3;
drop table anal_zzcandles_predicted_D_5_5;
drop table anal_zzkmsetids;
"""
"""
{
    "codename": "1928.T",
    "granularity": "D",
    "start_date": "2022-08-01",
    "end_date": "2022-12-01",
    "indicators": {
        "zigzag5": {
            "size": 5,
            "type": "zigzag"
        }
    }
}
"""

config = {
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": []
}

class TestZzAnalyzer(unittest.TestCase):
    def test_normalizeItem(self):
        (startep, endep) = zz.strStEd2ep("2020-01-04", "2022-12-31")
        a = zz.ZzAnalyzer(startep, endep, config=config, use_master=False)

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

    def test_getkmcandlename(self):
        (startep, endep) = zz.strStEd2ep("2020-01-04", "2022-12-31")
        a = zz.ZzAnalyzer(startep, endep, config=config, use_master=False)
        
        o = [0, 0, 0]
        h = [0.5, 0.6, 0.7]
        l = [0.1, 0.2, 0.3]
        c = [0, 0, 0]
        df = pd.DataFrame([o,h,l,c])
        #df = df.transpose()
        v = df[0].tolist() + df[1].tolist()  + df[2].tolist() 
        g = a._getKmCandleName(v)
        self.assertEqual(g, 'cdl012012')
        
        h = [0.7, 0.6, 0.5]
        l = [0.3, 0.2, 0.1]
        df = pd.DataFrame([o,h,l,c])
        v = df[0].tolist() + df[1].tolist()  + df[2].tolist() 
        g = a._getKmCandleName(v)
        self.assertEqual(g, 'cdl210210')
        

    def test_init_reg_km(self):
        db = MySqlDB()
        stage = "all"
        
        config["codenames"] = ["1301.T", "1928.T"]
        (startep, endep) = zz.strStEd2ep("2020-01-04", "2022-12-31")    
        a = zz.ZzAnalyzer(startep, endep, config=config, use_master=False)

        if stage == "init" or stage == "all":
            a.dropAllTables()

            a = zz.ZzAnalyzer(startep, endep, config=config, use_master=False)
            a.initZzCodeTable()

            self.assertTrue(db.tableExists("anal_zzcode_D"))

            sql = "select codename from anal_zzcode_D where obsyear = 2022 and codename = '1301.T';"
            (codename,) = db.select1rec(sql)
            self.assertTrue(codename, "1301.T")

            self.assertEqual(db.countTable("anal_zzcode_D"), 2)

            codenames = a.getCodenamesFromDB()
            self.assertEqual(len(codenames), 1)
            self.assertEqual(codenames[0], "1928.T")
        
        if stage == "register" or stage == "all":
            db.truncateTable("anal_zzdata_D_5")
            db.truncateTable("anal_zzitems_D_5_5")
            db.truncateTable("anal_zzcandles_D_5_3")
            a.registerData()
            sql = "select distinct codename from anal_zzdata_D_5"
            sql = "select count(*) from (%s) a;" % (sql)
            (cnt,) = db.select1rec(sql)
            self.assertEqual(cnt, 1)

            sql = "select year(from_unixtime(min(ep))), year(from_unixtime(max(ep))) from anal_zzdata_D_5;"
            (startyear, endyear) = db.select1rec(sql)
            self.assertEqual(startyear, 2020)
            self.assertEqual(endyear, 2022)

            sql = "select distinct codename from anal_zzitems_D_5_5"
            sql = "select count(*) from (%s) a;" % (sql)
            (cnt,) = db.select1rec(sql)
            self.assertEqual(cnt, 1)

            sql = "select year(from_unixtime(min(endep))), year(from_unixtime(max(endep))) from anal_zzitems_D_5_5;"
            (startyear, endyear) = db.select1rec(sql)
            self.assertEqual(startyear, 2020)
            self.assertEqual(endyear, 2022)

            sql = "select distinct x0, x5, min(last_dir), max(last_dir) from anal_zzitems_D_5_5"
            sqlcnt = "select count(*) from (%s) a;" % sql
            (cnt,) = db.select1rec(sqlcnt)
            self.assertEqual(cnt, 1)
            (x0, x5, mindir, maxdir) = db.select1rec(sql)
            self.assertEqual(x0, 0)
            self.assertEqual(x5, 1)
            self.assertEqual(mindir, -2)
            self.assertEqual(maxdir, 2)

            cnt = db.countTable("anal_zzitems_D_5_5", ["x6 = 1"])
            self.assertEqual(cnt, 0)

            cnt = db.countTable("anal_zzitems_D_5_5", ["x6 < 1"])
            self.assertEqual(cnt, 0)

            cnt = db.countTable("anal_zzitems_D_5_5")
            self.assertGreater(cnt, 0)

            sql = "select count(*) from (select distinct codename, endep from anal_zzitems_D_5_5) a;"
            (itemcnt,) = db.select1rec(sql)
            candlecnt = db.countTable("anal_zzcandles_D_5_3")
            self.assertEqual(itemcnt, candlecnt)

            cnt = db.countTable("anal_zzcandles_D_5_3", ["dir = -1"])
            self.assertGreater(cnt, 0)

            cnt = db.countTable("anal_zzcandles_D_5_3", ["dir = 1"])
            self.assertGreater(cnt, 0)

            cnt = db.countTable("anal_zzcandles_D_5_3", ["dir = -2"])
            self.assertGreater(cnt, 0)

            cnt = db.countTable("anal_zzcandles_D_5_3", ["dir = 2"])
            self.assertGreater(cnt, 0)

        km_setname = "kmtest"
        if stage == "kmeans" or stage == "all":
            import os
            home = os.environ["HOME"]
            kmpath = home + "/" + "app/stockAnalyzer/test/zz/" + km_setname
            lib.removeDir(kmpath)
            db.truncateTable("anal_zzkmgroups_D_5_5")
            
            km_setid = a.execPeakKmeans(km_setname)
            self.assertTrue(os.path.exists(kmpath))

            sql = "select distinct km_setid from anal_zzkmgroups_D_5_5"
            sql = "select count(*) from (%s) a;" % (sql)
            (cnt,) = db.select1rec(sql)
            self.assertEqual(cnt, 1)

            sql = "select distinct km_setid from anal_zzkmgroups_D_5_5"
            (km_setid,) = db.select1rec(sql)
            self.assertEqual(km_setid, 1)

            a.testKm(km_setname)

        
            #check candles
            a.execCandleKmeans(km_setname)
            a.testKmCandle(km_setname)
            


        if stage == "kmstat" or stage == "all":
            db.truncateTable("anal_zzkmstats_D_5_5")
            a.calcKmClusterStats(km_setname)

        if stage == "kmcandlestat" or stage == "all":
            db.truncateTable("anal_zzkmcandlestats_D_5_2")
            a.calcKmCandleStats(km_setname)
            

      

if __name__ == "__main__":
    unittest.main()
