from strategy import Strategy
import env
import lib.tradelib as tradelib
from ticker import Ticker
from consts import *

class SimpleMarketStrategy(Strategy):
    def __init__(self, instrument, granularity, profit=100):
        self.instrument = instrument
        self.unitsecs = tradelib.getUnitSecs(granularity)
        self.granularity = granularity
        self.ticker = None
        self.profit = profit
        self.id = -1
        self.curr_side = SIDE_BUY
        self.now = -1
    

    def onTick(self, epoch):
        if self.id >= 0:
            return
        if self.ticker == None:
            self.ticker = Ticker(self.instrument, self.granularity, epoch)

        if self.ticker.tick(epoch) == False:
            return None

        (now, _, _, h, l, c, _) = self.ticker.getPrice(epoch)
        self.now = now
        price = (h+l+c)/3

        orders = []
        if self.id == -1:
            self.curr_side *= -1
        
            #epoch, data_getter, side, units, price,
            #            validep=0, takeprofit=0, stoploss=0, desc=""
            order = self.createMarketOrder(now,
                    self.ticker.dg, self.curr_side, 1, price,
                    takeprofit=price+self.curr_side*self.profit, 
                    stoploss=price-self.curr_side*self.profit)
            self.id = order.id
            orders.append(order)
        return orders
        
        
    def onSignal(self, event):
        if self.id == event.id:
            if event.signal in [env.ESTATUS_ORDER_CLOSED,
                                      env.ESTATUS_TRADE_CLOSED]:
                self.id = -1
        