from event.order import OrderEvent
import env
import lib
import lib.tradelib as tradelib


class Strategy(object):
    # self.manager is set on trading.Trading
    
    def setManager(self, manager):
        self.manager = manager

    def setTicker(self, ticker):
        self.ticker = ticker
        

    # return list of order_events
    def onTick(self):
        pass
    
    # return void
    def onSignal(self, signalEvent):
        pass

    
    
    def createMarketOrder(self, tickEvent, instrument, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        return self.createOrder(env.CMD_CREATE_MARKET_ORDER,
                          tickEvent.time, instrument, side,
                          units, price, validep,  
                          takeprofit, stoploss, desc)
    
    def createStopOrder(self, tickEvent, instrument, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        if (side == env.SIDE_BUY and price >= tickEvent.ask) or \
            (side == env.SIDE_SELL and price <= tickEvent.bid):
            return self.createOrder(env.CMD_ISSUE_ERROR, 
                                    tickEvent.time, instrument, side,
                                    units, price, desc="stop order")
        
        
        return self.createOrder(env.CMD_CREATE_STOP_ORDER,
                          tickEvent.time, instrument, side,
                          units, price, validep,  
                          takeprofit, stoploss, desc)
        
        
    def createOrder(self, cmd, epoch, instrument, side, units, price,
                        validep=0, takeprofit=0, stoploss=0, desc=""):
        _id = self.manager.genID()
        digit = tradelib.getDecimalPlace(instrument) + 1
        price = lib.truncFromDecimalPlace(price, digit)
        takeprofit = lib.truncFromDecimalPlace(takeprofit, digit)
        stoploss = lib.truncFromDecimalPlace(stoploss, digit)
        return OrderEvent(_id, cmd,
                          epoch, instrument, side,
                          units, validep, price, 
                          takeprofit, stoploss, desc)
        
    def createCancelEvent(self, _id):
        return OrderEvent(_id, env.CMD_CANCEL)
          
          
    def getPlotElements(self, color="k"):
        return []