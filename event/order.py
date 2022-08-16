'''
Created on 2019/04/20

@author: kot
'''

from consts import *
import lib.tradelib as tradelib

class OrderEvent(object):
    def __init__(self, _id, cmd, epoch=0, instrument="", side=0, 
                 units=0, validep=0,
        price=0, takeprofit=0, stoploss=0, desc=""):
        self.id= _id
        self.epoch = epoch
        self.type = EVETYPE_ORDER
        self.instrument = instrument
        digit = tradelib.getDecimalPlace(instrument)
        self.digit = digit
        self.side = side
        self.cmd = cmd
        if cmd != CMD_CANCEL and _id < 0:
            raise Exception("Need id for cancel orders!")
        self.units = units
        self.validep = validep
        self.price = round(price,digit+1)
        self.status = ESTATUS_NONE
        self.takeprofit_price = round(takeprofit,digit+1)
        self.stoploss_price = round(stoploss,digit+1)
        self.desc = desc
        
        self.order_close_time = 0
        
        # trade part
        self.trade_open_time = 0
        self.trade_close_time = 0
        self.trade_open_price = 0
        self.trade_close_price = 0
        self.trade_profit = 0
        
    def open_trade(self, tickEvent, price, desc=""):
        self.status = ESTATUS_TRADE_OPENED
        self.trade_open_time = tickEvent.time
        self.trade_open_price = round(price,self.digit+1)
        self.desc = desc
        
    def close_trade(self, tickEvent, price, desc=""):
        self.status = ESTATUS_TRADE_CLOSED
        self.trade_close_time = tickEvent.time
        self.trade_close_price = round(price,self.digit+1)
        price = (self.trade_close_price-self.trade_open_price)*self.side
        self.trade_profit = round(price,self.digit+1)*self.units
        self.desc = desc
        
    def close_order(self, tickEvent, desc=""):
        self.status = ESTATUS_ORDER_CLOSED
        self.order_close_time = tickEvent.time
        self.desc = desc
        
    def is_valid(self, tickEvent):
        if tickEvent.time > self.validep:
            return False
            #self.close_order("Exceeded valid time=%s" % lib.epoch2str(self.validep))
        return True
    