from time_ticker import TimeTicker
import lib.tradelib as tradelib

def getBacktestManager(name, granularity, startep, endep, 
        strategy, buy_budget=1000000, sell_budget=1000000):
    from executor import Executor
    from trade_manager import TradeManager
    from portforio import Portoforio
    
    interval = tradelib.getUnitSecs(granularity)
    ticker = TimeTicker(interval, startep, endep)
    executor = Executor()
    portforio = Portoforio(name, buy_budget, sell_budget)
    return TradeManager(name, ticker, strategy, executor, portforio)
    

def runBacktest(name, granularity, startep, endep, strategy, 
    orderstopep=0, buy_budget=1000000, sell_budget=1000000):
    if orderstopep == 0:
        orderstopep = endep
    manager = getBacktestManager(name, granularity, startep, endep, strategy)
    return manager.run(orderstopep=orderstopep)

def runZzBacktest(name, granularity, startep, endep, km_setid, 
    orderstopep=0, buy_budget=1000000, sell_budget=1000000, max_fund=500000):
    args = {"granularity": granularity, 
        "min_profit": profit,
        "max_fund": max_fund,
        "trade_mode": TRADE_MODE_BOTH,
        "km_setid": km_setid
    }
    strategy = ZzStatsStrategy(args, use_master=True, load_zzdb=False)
    report = runBacktest(name, granularity, startep, endep, strategy, 
        orderstopep=orderstopep, buy_budget=buy_budget, sell_budget=sell_budget)
    print(report)