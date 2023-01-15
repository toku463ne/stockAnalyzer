from env import *
from consts import *

class TradeManager(object):
    def __init__(self, name, buy_budget, sell_budget,
        timeTicker, strategy, executor, portforio):
        self.name = name
        self.buy_budget = buy_budget
        self.sell_budget = sell_budget
        self.maxId = 0
        self.orders = {}
        self.trans = []
        self.executor = executor
        self.strategy = strategy
        self.timeTicker = timeTicker
        self.portforio = portforio
        

    def run(self, endep=-1, orderstopep=-1):
        order_events = []
        strategy = self.strategy
        strategy.preProcess(self.timeTicker, self.portforio)
        portforio = self.portforio
        
        portforio.clearDB()
        
        t = self.timeTicker
        while True:
            epoch = t.epoch
            if endep > 0 and epoch > endep:
                break

            #portforio.onTick(self.orders)
            if epoch <= orderstopep:
                order_events = strategy.onTick(epoch)
            else:
                order_events = []
            while order_events is not None and len(order_events) > 0:
                order = order_events.pop(0)
                if self.receiveOrder(epoch, order) == False:
                    strategy.onError(epoch, order)
            
            if t.tick() == False:
                break

            events = self.checkEvents(epoch)
            while len(events) > 0:
                event = events.pop(0)
                strategy.onSignal(epoch, event)
                portforio.onSignal(epoch, event)

        return self.getReport()


    def genId(self):
        self.maxId += 1
        return self.maxId

    
    def receiveOrder(self, epoch, orderEvent):
        if orderEvent.cmd in [CMD_CREATE_STOP_ORDER,
                            CMD_CREATE_LIMIT_ORDER,
                            CMD_CREATE_MARKET_ORDER]:
            purch_price = orderEvent.price * orderEvent.units
            portforio = self.portforio
            buy_offline = portforio.getBuyOffLine()
            if orderEvent.side == SIDE_BUY and buy_offline + purch_price > self.buy_budget:
                orderEvent.error_msg = "Over Buy budget"
                orderEvent.cmd = CMD_ORDER_ERROR
                return False
            sell_offline = portforio.getSellOffLine()
            if orderEvent.side == SIDE_SELL and sell_offline + purch_price > self.sell_budget:
                orderEvent.error_msg = "Over Sell budget"
                orderEvent.cmd = CMD_ORDER_ERROR
                return False

        if self.executor.checkOrder(epoch, orderEvent) == False:
            return False
        orderEvent.setId(self.genId())
        self.trans.append(orderEvent)
        orderEvent = self.trans.pop()
        _id = orderEvent.id
        if _id in self.orders.keys():
            if orderEvent.cmd != CMD_CANCEL:
                raise Exception("id=%d already exists!" % _id)

        if orderEvent.cmd in [CMD_CREATE_STOP_ORDER,
                            CMD_CREATE_LIMIT_ORDER,
                            CMD_CREATE_MARKET_ORDER]:
            self.orders[_id] = orderEvent
        return True


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

        return signal_events

    def getReport(self):
        return self.portforio.getReport()

    def getHistory(self):
        return self.portforio.getHistory()

    def getTrades(self):
        return self.portforio.getTrades()