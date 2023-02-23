from consts import *

class Executor(object):
    def cancelOrder(self, epoch, orderEvent):
        (_, _, _, h, l, c, _) = orderEvent.getPrice(epoch)
        price = (h+l+c)/3
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            orderEvent.closeOrder(epoch, "Order cancel")
        if orderEvent.status == ESTATUS_TRADE_OPENED:
            orderEvent.closeTrade(epoch, price, "Trade cancel")
        orderEvent.cmd = CMD_CANCEL
        return orderEvent
        


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
        (_, _, _, h, l, c, _) = orderEvent.getPrice(epoch)
        side = orderEvent.side
        price = (h+l+c)*1.0/3.0
        orderEvent.price = price
        expr = orderEvent.expiration

        if orderEvent.cmd == CMD_CANCEL:
            return
        
        if orderEvent.status == ESTATUS_ORDER_OPENED:
            if orderEvent.cmd == CMD_CREATE_MARKET_ORDER:
                orderEvent.openTrade(epoch, price, orderEvent.desc)
                return orderEvent
                
            elif orderEvent.cmd == CMD_CREATE_STOP_ORDER:
                
                
                if orderEvent.is_valid(epoch) == False:
                    orderEvent.closeOrder(epoch, "expired")
                    return orderEvent
                else:
                    if side == SIDE_BUY and orderEvent.price > l:
                        orderEvent.openTrade(epoch, price, orderEvent.desc)
                        return orderEvent
                    if side == SIDE_SELL and orderEvent.price < h:
                        orderEvent.openTrade(epoch, price, orderEvent.desc)
                        return orderEvent
  
            else:
                raise Exception("Non supported cmd")
        
        elif orderEvent.status == ESTATUS_TRADE_OPENED:
            tp = orderEvent.takeprofit_price
            sl = orderEvent.stoploss_price
            iswin = 0
            expired = False
            if expr > 0 and expr <= epoch:
                expired = True
            msg = ""
            
            if side == SIDE_BUY:
                if l < sl:
                    iswin = -1
                    msg = "stoploss"
                    price = sl 
                elif h > tp:
                    iswin = 1
                    msg = "takeprofit"
                    price = tp
                elif expired:
                    if orderEvent.trade_open_price < price:
                        iswin = 1
                    else:
                        iswin = -1
                    msg = "expired"
            if side == SIDE_SELL:
                if h > sl:
                    iswin = -1
                    msg = "stoploss"
                    price = sl 
                elif l < tp:
                    iswin = 1
                    msg = "takeprofit"
                    price = tp
                elif expired:
                    if orderEvent.trade_open_price > price:
                        iswin = 1
                    else:
                        iswin = -1
                    msg = "expired"
                
            
            if iswin == 1 or iswin == -1:
                orderEvent.closeTrade(epoch, price, msg)
                return orderEvent
        elif orderEvent.status == ESTATUS_NONE:
            if orderEvent.cmd == CMD_CREATE_MARKET_ORDER or orderEvent.cmd == CMD_CREATE_STOP_ORDER:
                orderEvent.openTrade(epoch, price)
                return orderEvent



        return None

