from consts import *
import random

class OrderEvent(object):
    def __init__(self, cmd, data_getter, _id=-1, epoch=0, codename="", side=0, 
                 units=0, validep=0,
        price=0, takeprofit=0, stoploss=0, desc=""):
        self.dg = data_getter
        self.id= _id
        self.localId = "%d-%d" % (epoch, random.randint(10000000, 99999999))
        self.epoch = epoch
        self.type = EVETYPE_ORDER
        self.codename = codename
        self.side = side
        self.cmd = cmd
        if cmd == CMD_CANCEL and _id == -1:
            raise Exception("Need id for cancel orders!")
        self.units = units
        self.validep = validep
        #self.price = round(price,digit+1)
        self.price = price
        self.status = ESTATUS_NONE
        #self.takeprofit_price = round(takeprofit,digit+1)
        #self.stoploss_price = round(stoploss,digit+1)
        self.takeprofit_price = takeprofit
        self.stoploss_price = stoploss
        self.desc = desc
        
        self.order_close_time = 0
        
        # trade part
        self.trade_open_time = 0
        self.trade_close_time = 0
        self.trade_open_price = 0
        self.trade_close_price = 0
        self.trade_profit = 0

        # error
        self.error_type = ERROR_NONE
        self.error_msg = ""

    def setId(self, _id):
        self.id = _id

    def getPrice(self, epoch):
        return self.dg.getPrice(epoch)
        
    def openTrade(self, tickEvent, price, desc=""):
        self.status = ESTATUS_TRADE_OPENED
        self.trade_open_time = tickEvent.time
        price = round(price,self.digit+1)
        self.trade_open_price = price
        self.price = price
        self.desc = desc
        #return SignalEvent(self.id, ESTATUS_TRADE_OPENED)
        
    def closeTrade(self, tickEvent, price, desc=""):
        self.status = ESTATUS_TRADE_CLOSED
        self.trade_close_time = tickEvent.time
        price = round(price,self.digit+1)
        self.trade_close_price = price
        self.price = price
        price_diff = (price-self.trade_open_price)*self.side
        self.trade_profit = round(price_diff,self.digit+1)*self.units
        self.desc = desc
        #return SignalEvent(self.id, ESTATUS_TRADE_CLOSED)
        
    def closeOrder(self, tickEvent, desc=""):
        self.status = ESTATUS_ORDER_CLOSED
        self.order_close_time = tickEvent.time
        self.desc = desc
        #return SignalEvent(self.id, ESTATUS_ORDER_CLOSED)
        
    def isValid(self, tickEvent):
        if tickEvent.time > self.validep:
            return False
            #self.close_order("Exceeded valid time=%s" % lib.epoch2str(self.validep))
        return True
    
    def setError(self, error_type, msg):
        self.error_type = error_type
        self.error_msg = msg
        
    def getPrice(self, epoch):
        return self.dg.getPrice(epoch)