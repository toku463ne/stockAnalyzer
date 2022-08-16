import __init__
import unittest
from analyzer.zz_analyzer import ZzAnalyzer
from datetime import datetime
from db.mysql import MySqlDB


class TestZzAnalyzer(unittest.TestCase):
    def test_zzanalyzer(self):
        codenames = ["^N225", "1301.T"]
        granularity = "D"
        size = 5
        st = datetime(year=2021, month=1, day=1, hour=9).timestamp()
        ed = datetime(year=2022, month=1, day=1, hour=9).timestamp()
        
        a = ZzAnalyzer(codenames, granularity, size, st, ed)
        db = MySqlDB()

        table_names = [
                    "anal_zzdata",
                    "anal_zzitems_D_5",
                    "anal_zzgroups",
                    "anal_zzkmstats_D_5"]
        for table_name in table_names:
            sql = "delete from %s;" % (table_name)
            db.execSql(sql)
        
        
        a.registerData()

        self.assertGreater(db.countTable("anal_zzdata"), 0) 
        self.assertGreater(db.countTable("anal_zzitems_D_5"), 0) 
        
        sql = "select granularity, size from anal_zzgroups limit 1;"
        (granularity, size) = db.select1rec(sql)
        self.assertEqual(granularity, "D") 
        self.assertEqual(size, 5)
        
        a.execKmeans()
        sql = "select km_groupid from anal_zzitems_D_5 limit 1;"
        (km_groupid,) = db.select1rec(sql)
        self.assertNotEqual(km_groupid, None)

        a.calcClusterStats()
        self.assertGreater(db.countTable("anal_zzkmstats_D_5"), 0) 

        #print(p)

if __name__ == "__main__":
    unittest.main()
