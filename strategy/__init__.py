from event.order import OrderEvent
from consts import *

class Strategy(object):
    # return list of order_events
    def onTick(self, epoch):
        pass
    
    # return void
    def onSignal(self, event):
        pass

    def createMarketOrder(self, epoch, data_getter, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        return OrderEvent(CMD_CREATE_MARKET_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, desc=desc)
    
    def createStopOrder(self, epoch, data_getter, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        return OrderEvent(CMD_CREATE_STOP_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, desc=desc)
        
    def cancelOrder(self, _id):
        return OrderEvent(_id, CMD_CANCEL)
          
          
    def getPlotElements(self, color="k"):
        return []