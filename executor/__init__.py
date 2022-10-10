from consts import *

class Executor(object):
    def cancelOrder(self, epoch, orderEvent):
        (_, _, _, h, l, c, _) = orderEvent.getPrice(epoch)
        price = (h+l+c)/3
        
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            orderEvent.closeOrder(epoch, "Order cancel")
        if orderEvent.status == ESTATUS_TRADE_OPENED:
            orderEvent.closeTrade(epoch, price, "Trade cancel")
    

    def checkOrder(self, epoch, orderEvent):
        (_, _, _, h, l, c, _) = orderEvent.getPrice(epoch)
        price = (h+l+c)/3
        side = orderEvent.side

        if orderEvent.cmd == CMD_CREATE_STOP_ORDER:
            if (side == SIDE_BUY and price >= price) or \
                (side == SIDE_SELL and price <= price):
                orderEvent.setError(ERROR_BAD_ORDER, "Wrong price in stop order.")
                return False
        return True


    # TODO: Issue error when the order is strange
    def detectOrderChange(self, epoch, orderEvent):
        (_, _, _, h, l, _, _) = orderEvent.getPrice(epoch)
        side = orderEvent.side
        
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            if orderEvent.cmd == CMD_CREATE_MARKET_ORDER:
                orderEvent.openTrade(epoch, orderEvent, price, "Market order")
                return orderEvent
                
            elif orderEvent.cmd == CMD_CREATE_STOP_ORDER:
                
                
                if orderEvent.is_valid(epoch) == False:
                    orderEvent.closeOrder(epoch, orderEvent, "expired")
                    return orderEvent
                else:
                    if side == SIDE_BUY and orderEvent.price > l:
                        orderEvent.openTrade(epoch, orderEvent, price, "Stop order")
                        return orderEvent
                    if side == SIDE_SELL and orderEvent.price < h:
                        orderEvent.openTrade(epoch, orderEvent, price, "Stop order")
                        return orderEvent
  
            else:
                raise Exception("Non supported cmd")
        
        elif orderEvent.status == ESTATUS_TRADE_OPENED:
            tp = orderEvent.takeprofit_price
            sl = orderEvent.stoploss_price
            iswin = 0
            
            if side == SIDE_BUY:
                if l < sl:
                    iswin = -1
                    price = sl 
                elif h > tp:
                    iswin = 1
                    price = tp
            if side == SIDE_SELL:
                if h > sl:
                    iswin = -1
                    price = sl 
                elif l < tp:
                    iswin = 1
                    price = tp
            
            if iswin == 1:
                orderEvent.closeTrade(epoch, orderEvent, price, "takeprofit")
                return orderEvent
            if iswin == -1:
                orderEvent.closeTrade(epoch, orderEvent, price, "stoploss")
                return orderEvent

        return None

