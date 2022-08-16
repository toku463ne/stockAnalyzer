from event.signal import SignalEvent
from consts import *

class Executor(object):
    def receiveOrder(self, orderEvent):
        self.manager.appendTransaction(orderEvent)
    
    def setManager(self, manager):
        self.manager = manager

      
    # return list of signal_events
    def onTick(self, tickEvent):
        signal_events = []
        
        # operate existing orders
        orders = self.manager.getOrders()
        for _id in orders.keys():
            orderEvent = orders[_id]
            if self.checkOrderChange(tickEvent, orderEvent):
                signal_events.append(SignalEvent(_id, 
                                        orderEvent.status))

            
        # operate new orders
        while len(self.manager.transactions) > 0:
            orderEvent = self.manager.popTransaction()
            _id = orderEvent.id
            orgEvent = self.manager.getOrder(_id)
            
            if orderEvent.cmd == CMD_CANCEL:
                self.cancelOrder(tickEvent, orgEvent)
                self.manager.closeOrder(orgEvent, tickEvent, "canceled")
                signal_events.append(SignalEvent(_id, 
                                        orgEvent.status))
                continue
            
            if orgEvent != None:
                raise Exception("id=%d already exists!" % _id)
            
            if orderEvent.cmd in [CMD_CREATE_STOP_ORDER,
                             CMD_CREATE_LIMIT_ORDER,
                             CMD_CREATE_MARKET_ORDER]:
                self.issueOrder(tickEvent, orderEvent)
                self.manager.openOrder(tickEvent, orderEvent)
                signal_events.append(SignalEvent(_id, orderEvent.status))
    
            if orderEvent.cmd == CMD_ISSUE_ERROR:
                self.manager.issueError(tickEvent, orderEvent)
                signal_events.append(SignalEvent(_id, orderEvent.status))
    
        return signal_events
        
    def issueOrder(self, tickEvent, orderEvent):
        pass
    
    def cancelOrder(self, tickEvent, orderEvent):
        if orderEvent.side == SIDE_BUY:
            price = tickEvent.bid
        else:
            price = tickEvent.ask
        
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            orderEvent.close_order(tickEvent, "Order cancel")
        if orderEvent.status == ESTATUS_TRADE_OPENED:
            orderEvent.close_trade(tickEvent, price, "Trade cancel")
    
    # make sure to update orderEvent status accordingly
    # True is status has changed
    def checkOrderChange(self, tickEvent, orderEvent):
        if orderEvent.side == SIDE_BUY:
            price = tickEvent.ask
        else:
            price = tickEvent.bid
        
        side = orderEvent.side
        h = tickEvent.h
        l = tickEvent.l
        ret = False
        
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            if orderEvent.cmd == CMD_CREATE_MARKET_ORDER:
                self.manager.openTrade(tickEvent, orderEvent, price, "Market order")
                ret = True
                
            elif orderEvent.cmd == CMD_CREATE_STOP_ORDER:
                
                if orderEvent.is_valid(tickEvent) == False:
                    self.manager.closeOrder(tickEvent, orderEvent, "expired")
                    ret = True
                else:
                    if side == SIDE_BUY and orderEvent.price > l:
                        self.manager.openTrade(tickEvent, orderEvent, price, "Stop order")
                        ret = True
                    if side == SIDE_SELL and orderEvent.price < h:
                        self.manager.openTrade(tickEvent, orderEvent, price, "Stop order")
                        ret = True
  
            else:
                raise Exception("Non supported cmd")
        
        elif orderEvent.status == ESTATUS_TRADE_OPENED:
            tp = orderEvent.takeprofit_price
            sl = orderEvent.stoploss_price
            spread = abs(tickEvent.bid - tickEvent.ask)
            iswin = 0
            
            if side == SIDE_BUY:
                if l < sl:
                    iswin = -1
                    price = sl - spread
                elif h > tp:
                    iswin = 1
                    price = tp
            if side == SIDE_SELL:
                if h > sl:
                    iswin = -1
                    price = sl + spread
                elif l < tp:
                    iswin = 1
                    price = tp
            
            price = round(price, tickEvent.digit+1)
            
            if iswin == 1:
                self.manager.closeTrade(tickEvent, orderEvent, price, "takeprofit")
                ret = True
            
            if iswin == -1:
                self.manager.closeTrade(tickEvent, orderEvent, price, "stoploss")
                ret = True
            
        return ret