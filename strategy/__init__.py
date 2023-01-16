from event.order import OrderEvent
from consts import *
trade_mode = TRADE_MODE_ONLY_BUY
import lib

class Strategy(object):    

    def preProcess(self, timeTicker, portforio):
        self.timeTicker = timeTicker
        self.portforio = portforio

    def initAttrFromArgs(self, args, name, default=None):
        lib.initAttrFromArgs(self, args, name, default=default)
        

    # return list of order_events
    def onTick(self, epoch):
        pass
    
    # return void
    def onSignal(self, epoch, event):
        pass

    def onError(self, epoch, event):
        print("[error]%s: %s code=%s desc=%s" % (str(event.localId), lib.epoch2str(epoch, "%Y%m%d"), 
                event.codename, event.error_msg))
        pass

    def createMarketOrder(self, epoch, data_getter, side, units,
                        validep=0, takeprofit=0, stoploss=0, expiration=0, desc="Market Order"):
        (_, _, _, _, _, price, _) = data_getter.getPrice(epoch+data_getter.unitsecs)
        order = OrderEvent(CMD_CREATE_MARKET_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, 
                          expiration=expiration, desc=desc)
        #order.openTrade(epoch, price, desc)
        return order
    
    def createStopOrder(self, epoch, data_getter, side, units,
                        validep=0, takeprofit=0, stoploss=0, expiration=0, desc="Stop Order"):
        (_, price, _, _, _, _, _) = data_getter.getPrice(epoch+data_getter.unitsecs)
        order = OrderEvent(CMD_CREATE_STOP_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, 
                          expiration=expiration, desc=desc)
        #order.openTrade(epoch, price, desc)
        return order
        
    def cancelOrder(self, _id):
        return OrderEvent(_id, CMD_CANCEL)
          
          
    def getPlotElements(self, color="k"):
        return []