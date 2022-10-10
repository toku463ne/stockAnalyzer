import __init__

import unittest
from strategy.simpleMarket import SimpleMarketStrategy
import lib
import lib.tradelib as tradelib
from time_ticker import TimeTicker
from executor import Executor
from trade_manager import TradeManager
from portforio import Portoforio

class TestSimpleMarketStrategy(unittest.TestCase):


    def testCase1(self):
        
        instrument = "^N225"
        granularity = "D"
        st = lib.str2epoch("2021-06-01T00:00:00")
        ed = lib.str2epoch("2021-12-01T00:00:00")
        
        strategy = SimpleMarketStrategy(instrument, granularity)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), st, ed)
        executor = Executor()
        portforio = Portoforio()
        tm = TradeManager("market strategy", ticker, strategy, executor, portforio)
        tm.run()

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()