'''
Created on 2019/11/16

@author: kot
'''
from strategy import Strategy
import env
import lib.tradelib as tradelib
from tools.subchart import SubChart

class SimpleMarketStrategy(Strategy):
    def __init__(self, instrument, granularity, profitpips):
        self.instrument = instrument
        self.unitsecs = tradelib.getUnitSecs(granularity)
        self.granularity = granularity
        self.subc = None
        self.profit = tradelib.pip2Price(profitpips, instrument)
        self.id = -1
        self.curr_side = env.SIDE_BUY
        self.now = -1
    
    def onTick(self, tickEvent):
        if self.subc == None:
            self.subc = SubChart("SimpleMarket",
                                 self.instrument, 
                                 self.granularity, 
                                 endep=tickEvent.time)
        now = self.subc.onTick(tickEvent)
        if now == self.now:
            return []
        self.now = now
            
        orders = []
        if self.id == -1:
            self.curr_side *= -1
            if self.curr_side == env.SIDE_BUY:
                price = tickEvent.ask
            else:
                price = tickEvent.bid
        
            order = self.createMarketOrder(
                    tickEvent, self.instrument, self.curr_side, 1, price,
                    takeprofit=price+self.curr_side*self.profit, 
                    stoploss=price-self.curr_side*self.profit)
            self.id = order.id
            orders.append(order)
        return orders
        
        
    def onSignal(self, signalEvent):
        if self.id == signalEvent.id:
            if signalEvent.signal in [env.ESTATUS_ORDER_CLOSED,
                                      env.ESTATUS_TRADE_CLOSED]:
                self.id = -1
        