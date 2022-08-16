'''
Created on 2019/11/16

@author: kot
'''
from strategy import Strategy
from consts import *
import lib.tradelib as tradelib

class SimpleStopStrategy(Strategy):
    def __init__(self, instrument, granularity, profit):
        self.instrument = instrument
        self.granularity = granularity
        self.profit = profit
        self.unitsecs = tradelib.getUnitSecs(granularity)
        self.id = -1
        self.curr_side = SIDE_BUY
        self.now = -1
    
    def onTick(self, tickEvent):
        orders = []
        if self.id == -1:
            self.curr_side *= -1
            price = tickEvent.price
        
            order = self.createStopOrder(
                    tickEvent, self.instrument, self.curr_side, 1, 
                    price,
                    validep=tickEvent.time+self.unitsecs,
                    takeprofit=price+self.curr_side*self.profit, 
                    stoploss=price-self.curr_side*self.profit)
            if order != None:
                self.id = order.id
                orders.append(order)
            
        return orders
        
        
    def onSignal(self, signalEvent):
        if self.id == signalEvent.id:
            if signalEvent.signal in [ESTATUS_ORDER_CLOSED,
                                      ESTATUS_TRADE_CLOSED]:
                self.id = -1
        