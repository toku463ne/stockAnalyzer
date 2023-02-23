from event.order import OrderEvent
from consts import *
trade_mode = TRADE_MODE_ONLY_BUY
import lib

class Strategy(object):    

    def preProcess(self, timeTicker, portforio):
        self.timeTicker = timeTicker
        self.portforio = portforio

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
        
    def cancelOrder(self, epoch, data_getter, _id):
        return OrderEvent(CMD_CANCEL, epoch=epoch, _id=_id)
          
          
    def getPlotElements(self, color="k"):
        return []