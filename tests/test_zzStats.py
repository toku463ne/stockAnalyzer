import __init__

import unittest
from strategy.ZzStatsStrategy import ZzStatsStrategy
import lib
import lib.tradelib as tradelib
from time_ticker import TimeTicker
from executor import Executor
from trade_manager import TradeManager
from portforio import Portoforio

class TestZzStatsStrategy(unittest.TestCase):

    def _run(self, st, ed, os, profit):
        granularity = "D"
        args = {"codenames": ["^N225", "1332.T", "1379.T", 
        "1716.T", "1720.T", "1973.T", "2121.T", "2160.T",
        "2695.T", "2923.T", "3001.T",
        "3092.T", "3197.T", 
        "4080.T", "4337.T", 
        "5020.T", "5108.T", 
        "6042.T", "6360.T", "7014.T", "7150.T", "7157.T",
        "7175.T", "7203.T", "8001.T", "8002.T", "8244.T",
        "8306.T", "8308.T", 
        "9005.T", "9066.T", ], 
            "granularity": granularity, 
            "min_profit": profit,
            "min_km_count": 5,
            "max_std": 0.3,
            "max_fund": 10000000,
            "kmpkl_file": "/home/ubuntu/stockanaldata/zz/km_2015-2020.pkl"}
        #    "kmpkl_file": "tests/test_zzStats.d/km_2015-2021.pkl"}
        
        #args["codenames"] = ["^N225", "1332.T"]
        args["codenames"] = []
        args["market"] = "prime"
        strategy = ZzStatsStrategy(args, use_master=True)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), st, ed)
        executor = Executor()
        portforio = Portoforio("zzstats_test")
        tm = TradeManager("zigzag stats strategy", ticker, strategy, executor, portforio)
        report = tm.run(orderstopep=os)

        return report


    def testCase1(self):
        st = lib.str2epoch("2022-01-01T00:00:00") 
        ed = lib.str2epoch("2022-12-01T00:00:00")
        os = lib.str2epoch("2022-11-20T00:00:00")
        report = self._run(st, ed, os, 10000)
        self.assertEqual(report["trade_count"],1)
        self.assertEqual(report["codename"],"^N225")
        self.assertGreater(report["sell_offline"], 0)

        #print(report)
        #import json
        #print(json.dumps(tm.portforio.history, indent=4))

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()