'''
Created on 2019/04/22

@author: kot
'''
from ticker import Ticker

def runTestingBacktest(name, codename, granularity, 
                       startep, endep, strategy):
    from executor import Executor
    import trading
    from trade_manager import TradeManager
    
    ticker = Ticker(codename, granularity, startep, endep)
    executor = Executor()
    manager = TradeManager(name)
    return trading.run(ticker, executor, strategy, manager, False)
    


#if __name__ == "__main__":
    #runSimple2("USD_JPY", "2019-04-02T09:00:00", "2019-04-02T18:00:00")
    #po, pe = runSlope2("USD_JPY", "H1", "2018-12-01T10:00:00", 
    #                  "2019-01-01T00:00:00")
    #print(po.getTotalProfit())
