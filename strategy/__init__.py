from event.order import OrderEvent
from consts import *

class Strategy(object):
    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)
        


    # return list of order_events
    def onTick(self, epoch):
        pass
    
    # return void
    def onSignal(self, epoch, event):
        pass

    def createMarketOrder(self, epoch, data_getter, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        order = OrderEvent(CMD_CREATE_MARKET_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, desc=desc)
        #order.openTrade(epoch, price, desc)
        return order
    
    def createStopOrder(self, epoch, data_getter, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        order = OrderEvent(CMD_CREATE_STOP_ORDER, data_getter, 
                          epoch=epoch, side=side,
                          units=units, price=price, validep=validep, 
                          takeprofit=takeprofit, stoploss=stoploss, desc=desc)
        #order.openTrade(epoch, price, desc)
        return order
        
    def cancelOrder(self, _id):
        return OrderEvent(_id, CMD_CANCEL)
          
          
    def getPlotElements(self, color="k"):
        return []