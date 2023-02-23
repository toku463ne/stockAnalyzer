from consts import *
import random

class OrderEvent(object):
    def __init__(self, cmd, data_getter=None, _id=-1, epoch=0, side=0, 
                 units=0, validep=0,
        price=0, takeprofit=0, stoploss=0, expiration=0, desc=""):
        self.dg = data_getter
        self.id= _id
        if data_getter != None:
            self.localId = "%s-%d-%d" % (data_getter.codename, epoch, random.randint(10000000, 99999999))
            self.codename = data_getter.codename
            self.granularity = data_getter.granularity
        self.epoch = epoch
        self.type = EVETYPE_ORDER
        self.side = side
        self.cmd = cmd
        if cmd == CMD_CANCEL and _id == -1:
            raise Exception("Need id for cancel orders!")
        self.units = units
        self.validep = validep
        self.price = price
        self.status = ESTATUS_ORDER_OPENED
        self.takeprofit_price = takeprofit
        self.stoploss_price = stoploss
        self.expiration = expiration
        self.desc = desc
        self.elapsed = 0
        
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
        
    def openTrade(self, epoch, price, desc=""):
        self.status = ESTATUS_TRADE_OPENED
        self.trade_open_time = epoch
        self.trade_open_price = price
        self.price = price
        self.desc = desc
        #return SignalEvent(self.id, ESTATUS_TRADE_OPENED)
        
    def closeTrade(self, epoch, price, desc=""):
        self.status = ESTATUS_TRADE_CLOSED
        self.trade_close_time = epoch
        self.trade_close_price = price
        self.price = price
        price_diff = (price-self.trade_open_price)*self.side
        self.trade_profit = price_diff*self.units
        self.desc = desc
        #return SignalEvent(self.id, ESTATUS_TRADE_CLOSED)
        
    def closeOrder(self, epoch, desc=""):
        self.status = ESTATUS_ORDER_CLOSED
        self.order_close_time = epoch
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