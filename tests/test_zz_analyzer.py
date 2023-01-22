import __init__
import unittest
import analyzer.zz_analyzer as zz
from datetime import datetime
from db.mysql import MySqlDB

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

        ep = [1,2,3,4,5]
        prices = [1000, 1100, 1200, 1300, 1400]
        vals = a._normalizeItem(ep, prices)
        (nep, nprices) = a.devideNormalizedVals(vals)
        self.assertEqual(nep[0], 0)
        self.assertEqual(nep[-2], 1)
        self.assertGreater(nep[-1], 1)
        self.assertEqual(nprices[0], 0)
        self.assertEqual(nprices[-2], 1)
        self.assertGreater(nprices[-1], 1)

        ep = [1,2,3,4,5]
        prices = [1000, 1100, 1400, 1300, 900]
        vals = a._normalizeItem(ep, prices)
        (nep, nprices) = a.devideNormalizedVals(vals)
        self.assertEqual(nep[0], 0)
        self.assertEqual(nep[-2], 1)
        self.assertGreater(nep[-1], 1)
        self.assertEqual(nprices[0], 0)
        self.assertEqual(nprices[2], 1)
        self.assertLess(nprices[-1], 0)


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

            sql = "select distinct x0, x3, min(last_dir), max(last_dir) from anal_zzitems_D_5_5"
            sqlcnt = "select count(*) from (%s) a;" % sql
            (cnt,) = db.select1rec(sqlcnt)
            self.assertEqual(cnt, 1)
            (x0, x3, mindir, maxdir) = db.select1rec(sql)
            self.assertEqual(x0, 0)
            self.assertEqual(x3, 1)
            self.assertEqual(mindir, -2)
            self.assertEqual(maxdir, 2)

            cnt = db.countTable("anal_zzitems_D_5_5", ["x4 <= 1"])
            self.assertEqual(cnt, 0)

            cnt = db.countTable("anal_zzitems_D_5_5")
            self.assertEqual(cnt, 78)

        km_setid = "kmtest"
        if stage == "kmeans" or stage == "all":
            db.truncateTable("anal_zzkmgroups_D_5_5")
            km_setid = a.execKmeans(km_setid)
            import os
            home = os.environ["HOME"]
            kmpath = home + "/" + "app/stockAnalyzer/test/zz/" + km_setid
            self.assertTrue(os.path.exists(kmpath))

            sql = "select distinct obsyear from anal_zzkmgroups_D_5_5"
            sql = "select count(*) from (%s) a;" % (sql)
            (cnt,) = db.select1rec(sql)
            self.assertEqual(cnt, 1)

            sql = "select distinct obsyear from anal_zzkmgroups_D_5_5"
            (year,) = db.select1rec(sql)
            self.assertEqual(year, 2022)

        if stage == "kmstat" or stage == "all":
            db.truncateTable("anal_zzkmstats_D_5_5")
            a.calcKmClusterStats(km_setid)

            a.loadKmModel(km_setid)
            sql = """select x0,y0,x1,y1,x2,y2,x3,y3,k.km_id from anal_zzitems_D_5_5 i
inner join anal_zzkmgroups_D_5_5 k on i.zzitemid = k.zzitemid
where k.obsyear = %d limit 10;""" % (2022)
            for row in db.execSql(sql):
                km_id1 = row[-1]
                
                ep = []
                prices = []
                for i in range(4):
                    ep.append(row[i*2])
                    prices.append(row[i*2+1])
                
                (_, _, km_id2) = a.predictNext(ep, prices)
                self.assertEqual(km_id1, km_id2)
                

      

if __name__ == "__main__":
    unittest.main()
