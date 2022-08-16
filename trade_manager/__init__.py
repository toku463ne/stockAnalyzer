import env
import lib
import copy

class TradeManager(object):
    def __init__(self, name):
        self.name = name
        self.maxID = 0
        self.orders = {}
        self.transactions = []
        self.history = []
        
    def genID(self):
        self.maxID += 1
        return self.maxID
    
    def IDExists(self, _id):
        if _id in self.orders.keys():
            return True
        else:
            return False
        
    def openOrder(self, tickEvent, orderEvent):
        _id = orderEvent.id
        if not _id in self.orders.keys():
            orderEvent.epoch = tickEvent.time
            orderEvent.status = env.ESTATUS_ORDER_OPENED
            self.orders[_id] = orderEvent
            if orderEvent.validep == 0:
                validstr = ""
            else:
                validstr = "valid=%s" % lib.epoch2str(orderEvent.validep)
            lib.printMsg(orderEvent.epoch, "[%s] Order %d opened. %f side=%d tp=%f sl=%f %s" % (
                        self.name,
                        _id, 
                        orderEvent.price, orderEvent.side,
                        orderEvent.takeprofit_price, 
                        orderEvent.stoploss_price,
                        validstr
                        ))
        else:
            raise Exception("Order %d already open." % _id)
    
    def openTrade(self, tickEvent, orderEvent, price, desc=""):
        orderEvent.open_trade(tickEvent, price, desc)
        self.updateOrder(orderEvent)
        lib.printMsg(tickEvent.time, "[%s] Trade %d opened. %f" % (
                        self.name,
                        orderEvent.id, 
                        price))
        
    def closeOrder(self, tickEvent, orderEvent, desc=""):
        _id = orderEvent.id
        orderEvent.close_order(tickEvent, desc)
        if _id in self.orders.keys():
            self.history.append(orderEvent)
            del self.orders[_id]
            lib.printMsg(tickEvent.time, "[%s] Order %d closed. %s" % (
                        self.name,
                        _id, 
                        orderEvent.desc))
    
    def closeTrade(self, tickEvent, orderEvent, price, desc=""):
        orderEvent.close_trade(tickEvent, price, desc)
        self.history.append(orderEvent)
        del self.orders[orderEvent.id]
        lib.printMsg(tickEvent.time, "[%s] Trade %d closed. %f profit=%f %s" % (
                        self.name,
                        orderEvent.id, 
                        orderEvent.trade_close_price, 
                        orderEvent.trade_profit, 
                        orderEvent.desc))
    
    def flushHistory(self):
        self.history = []
      
    def getOrder(self, _id):
        if _id in self.orders.keys():
            return self.orders[_id]
        else:
            return None
    
    def getOrders(self):
        return copy.deepcopy(self.orders)
    
    def updateOrder(self, orderEvent):
        _id = orderEvent.id
        if _id in self.orders.keys():
            self.orders[_id] = orderEvent
    
    def appendTransaction(self, orderEvent):
        return self.transactions.append(orderEvent)
            
    def popTransaction(self):
        return self.transactions.pop(0)
    
    def issueError(self, tickEvent, orderEvent):
        _id = orderEvent.id
        orderEvent.close_order(tickEvent, orderEvent.desc)
        lib.printMsg(tickEvent.time, "[%s] Order %d error. %f %s" % (
                        self.name,
                        orderEvent.id, 
                        orderEvent.price,
                        orderEvent.desc))