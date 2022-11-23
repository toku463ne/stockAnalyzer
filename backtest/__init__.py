import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from time_ticker import TimeTicker
from executor import Executor
from portforio import Portoforio
from trade_manager import TradeManager
import lib.tradelib as tradelib
import importlib

def run(strategy_name, strategy_args, start, end, order_stop, granularity):
    m = importlib.import_module('strategy.%s' % strategy_name)
    strategy_class = eval("m.%s" % (strategy_name))
    bt = Backtest(strategy_name, strategy_class, strategy_args)
    return bt.run(start, end, order_stop, granularity)
    

class Backtest(object):
    def __init__(self, strategy_name, strategy_class, strategy_args):
        self.strategy_name = strategy_name
        self.strategy_class = strategy_class
        self.strategy_args = strategy_args


    def run(self, start, end, order_stop, granularity):
        strategy = self.strategy_class(self.strategy_args)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), start, end)
        executor = Executor()
        portforio = Portoforio()
        desc = self.strategy_name
        if "description" in self.strategy_args.keys():
            desc = self.strategy_args["description"]
        tm = TradeManager(desc, ticker, strategy, executor, portforio)
        result = tm.run(orderstopep=order_stop)
        #
        # 
        
        print(result)
        return tm.getTrades()