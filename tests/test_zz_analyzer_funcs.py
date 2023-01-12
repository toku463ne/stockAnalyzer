import __init__
import unittest
from analyzer.zz_analyzer import ZzAnalyzer
from datetime import datetime
from db.mysql import MySqlDB


class TestZzAnalyzerFuncs(unittest.TestCase):
    def test_calcKmRootGroupId(self):
        granularity = "D"
        kmpkl_file = "/tmp/zz_test_km.pkl"
        n_points = 5
        
        a = ZzAnalyzer(granularity, n_points, kmpkl_file)

        v = [1,2,3,4]

        #print(p)

if __name__ == "__main__":
    unittest.main()
