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
        args = {"codenames": ["^N225", "1301.T"], 
            "granularity": granularity, 
            "profit": profit,
            "min_km_count": 5,
            "max_std": 0.3,
            "kmpkl_file": "tests/test_zzStats.d/km_2015-2021.pkl"}
        strategy = ZzStatsStrategy(args, use_master=True)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), st, ed)
        executor = Executor()
        portforio = Portoforio()
        tm = TradeManager("zigzag stats strategy", ticker, strategy, executor, portforio)
        report = tm.run(orderstopep=os)

        return report


    def testCase1(self):
        st = lib.str2epoch("2021-05-01T00:00:00") 
        ed = lib.str2epoch("2021-12-01T00:00:00")
        os = lib.str2epoch("2021-11-20T00:00:00")
        report = self._run(st, ed, os, 500)
        self.assertEqual(report["trade_count"],1)
        self.assertEqual(report["codename"],"^N225")
        self.assertGreater(report["sell_offline"], 0)

        #print(report)
        #import json
        #print(json.dumps(tm.portforio.history, indent=4))

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()