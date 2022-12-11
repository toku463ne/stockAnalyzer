import __init__
import unittest
import lib
from trade_manager import TradeManager
from time_ticker import TimeTicker

class TestTradeManager(unittest.TestCase):
    def testCase1(self):
        from executor import Executor
        from trade_manager import TradeManager
        
        interval = 60*60*24
        startep = lib.str2epoch("2021-11-01T00:00:00")
        endep = lib.str2epoch("2021-12-01T00:00:00")
        
        ticker = TimeTicker(interval, startep, endep)
        executor = Executor()
        tm = TradeManager("tm test", ticker, None, executor)

        

        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()