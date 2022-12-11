from strategy import Strategy
import env
import lib.tradelib as tradelib
from ticker import Ticker
from consts import *

class SimpleMarketStrategy(Strategy):
    def __init__(self, args):
        codename = args["codename"]
        granularity = args["granularity"]
        profit = 100
        if "profit" in args.keys():
            profit = args["profit"]

        self.codename = codename
        self.unitsecs = tradelib.getUnitSecs(granularity)
        self.granularity = granularity
        self.ticker = None
        self.profit = profit
        self.id = -1
        self.curr_side = SIDE_BUY
        self.now = -1
        self.localId = ""
    

    def onTick(self, epoch):
        if self.id >= 0:
            return []
        if self.ticker == None:
            self.ticker = Ticker(self.codename, self.granularity, epoch)

        if self.ticker.tick(epoch) == False:
            return []

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
            self.localId = order.localId
            orders.append(order)
        return orders
        
        
    def onSignal(self, epoch, event):
        if self.localId == event.localId:
            if event.status in [ESTATUS_ORDER_CLOSED,
                                      ESTATUS_TRADE_CLOSED]:
                self.id = -1
            else:
                self.id = event.id
        