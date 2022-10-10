from env import *
from consts import *

class TradeManager(object):
    def __init__(self, name, timeTicker, strategy, executor, portforio):
        self.name = name
        self.maxId = 0
        self.orders = {}
        self.trans = []
        self.history = []
        self.executor = executor
        self.strategy = strategy
        self.timeTicker = timeTicker
        self.portforio = portforio
        
        
    def run(self, endep=-1):
        order_events = []
        strategy = self.strategy
        executor = self.executor
        portforio = self.portforio
        
        t = self.timeTicker
        while True:
            epoch = t.epoch
            if endep > 0 and epoch > endep:
                break
            
            portforio.onTick(self.orders)
            events = self.checkEvents(epoch)
            while len(events) > 0:
                event = events.pop(0)
                strategy.onSignal(event)
                portforio.onSignal(event)
            
            order_events = strategy.onTick(epoch)
            while len(order_events) > 0:
                order = order_events.pop(0)
                if executor.checkOrder(epoch, order):
                    self.receiveOrder(order)
                else:
                    strategy.onError(order)

                
            
            for _id in self.orders.keys():
                if self.orders[_id].status in [ESTATUS_ORDER_CLOSED, ESTATUS_TRADE_CLOSED]:
                    del self.orders[_id]
            
            if t.tick() == False:
                break


    def genId(self):
        self.maxId += 1
        return self.maxId

    
    def receiveOrder(self, orderEvent):
        orderEvent.setId(self.genId())
        self.trans.append(orderEvent)

    def getOrder(self, _id):
        if _id in self.orders.keys():
            return self.orders[_id]
        else:
            return None

    def checkEvents(self, epoch):
        signal_events = []

        for _id in self.orders.keys():
            orderEvent = self.orders[_id]
            signal = self.executor.detectOrderChange(epoch, orderEvent)
            if signal != None:
                signal_events.append(signal)

        while len(self.trans) > 0:
            orderEvent = self.trans.pop()
            _id = orderEvent.id
            if _id in self.orders.keys():
                if orderEvent.cmd == CMD_CANCEL:
                    signal = self.executor.cancelOrder(epoch, _id)
                    signal_events.append(signal)
                    continue
                else:
                    raise Exception("id=%d already exists!" % _id)

            if orderEvent.cmd in [CMD_CREATE_STOP_ORDER,
                             CMD_CREATE_LIMIT_ORDER,
                             CMD_CREATE_MARKET_ORDER]:
                self.orders[_id] = orderEvent

        return signal_events
