import __init__

import unittest
from strategy.SimpleMarketStrategy import SimpleMarketStrategy
import lib
import lib.tradelib as tradelib
from time_ticker import TimeTicker
from executor import Executor
from trade_manager import TradeManager
from portforio import Portoforio

class TestSimpleMarketStrategy(unittest.TestCase):

    def _run(self, st, ed, os, profit):
        codename = "^N225"
        granularity = "D"
        args = {"codename": codename, 
        "granularity": granularity, 
        "profit": profit}
        strategy = SimpleMarketStrategy(args)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), st, ed)
        executor = Executor()
        portforio = Portoforio()
        tm = TradeManager("market strategy", ticker, strategy, executor, portforio)
        report = tm.run(orderstopep=os)

        return report


    def testCase1(self):
        
        st = lib.str2epoch("2021-06-01T00:00:00")
        ed = lib.str2epoch("2021-12-01T00:00:00")
        os = lib.str2epoch("2021-11-20T00:00:00")
        report = self._run(st, ed, os, 1000)
        self.assertEqual(report["trade_count"], 7)
        self.assertEqual(report["buy_offline"], -1000.0)
        
        report = self._run(st, ed, os, 700)
        self.assertEqual(report["trade_count"], 11)
        self.assertEqual(report["buy_offline"], 700.0)
        
        report = self._run(st, ed, os, 500)
        self.assertEqual(report["trade_count"], 17)
        self.assertEqual(report["sell_offline"], -500.0)
        
        st = lib.str2epoch("2021-06-01T00:00:00")
        ed = lib.str2epoch("2021-09-01T00:00:00")
        os = lib.str2epoch("2021-08-20T00:00:00")
        report = self._run(st, ed, os, 1000)
        self.assertEqual(report["trade_count"], 5)
        self.assertEqual(report["sell_offline"], -1000.0)
        self.assertEqual(report["buy_offline"], -2000.0)
        

        #print(report)
        #import json
        #print(json.dumps(tm.portforio.history, indent=4))

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()