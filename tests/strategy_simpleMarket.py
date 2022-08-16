'''
Created on 2019/11/16

@author: kot
'''
import unittest
from lib.backtests import runTestingBacktest
from strategy.simpleMarket import SimpleMarketStrategy
import lib

class TestSimpleMarketStrategy(unittest.TestCase):


    def testCase1(self):
        
        instrument = "USD_JPY"
        granularity = "D"
        st = lib.str2epoch("2019-11-15T00:00:00")
        ed = lib.str2epoch("2019-11-15T01:00:00")
        
        strategy = SimpleMarketStrategy(instrument,granularity, 5)
        history = runTestingBacktest("c1", instrument, 
                       st, ed, strategy)
        
        res = history[0]
        self.assertEqual(res.id, 1, "id")
        self.assertEqual(res.epoch, 
                         lib.str2epoch("2019-11-15T00:02:00"), "order start")
        self.assertEqual(res.trade_open_time, 
                         lib.str2epoch("2019-11-15T00:03:00"), "trade start")
        self.assertEqual(res.trade_close_time, 
                         lib.str2epoch("2019-11-15T00:09:00"), "trade close")
        self.assertEqual(res.trade_profit, -0.054, "profit")
        
        res = history[1]
        self.assertEqual(res.id, 2, "id")
        self.assertEqual(res.epoch, 
                         lib.str2epoch("2019-11-15T00:11:00"), "order start")
        
        res = history[6]
        self.assertEqual(res.id, 7, "id")
        self.assertEqual(res.epoch, 
                         lib.str2epoch("2019-11-15T00:41:00"), "order start")
        self.assertEqual(res.trade_close_time, 
                         lib.str2epoch("2019-11-15T00:57:00"), "trade close")
        #print(history)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()