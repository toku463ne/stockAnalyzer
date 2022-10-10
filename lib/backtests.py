from time_ticker import TimeTicker

def getBacktestManager(name, interval, startep, endep, strategy):
    from executor import Executor
    from trade_manager import TradeManager
    
    ticker = TimeTicker(interval, startep, endep)
    executor = Executor()
    return TradeManager(name, ticker, strategy, executor)
    

def runBacktest(name, interval, startep, endep, strategy):
    manager = getBacktestManager(name, interval, startep, endep, strategy)
    return manager.run()

