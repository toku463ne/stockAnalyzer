from consts import *

class Portoforio(object):
    def __init__(self, unitSize=1000):
        self.pos = -1
        self.unitSize = unitSize
        self.eval = []
        self.buy_offline = [] # <= 0
        self.buy_online = []
        self.sell_offline = [] # >= 0
        self.sell_online = []

    def onTick(self, orders):
        self.pos += 1
        if self.pos % self.unitSize == 0:
            us = self.unitSize
            self.eval.extend([0.0]*us)
            self.buy_offline.extend([0.0]*us)
            self.buy_online.extend([0.0]*us)
            self.sell_offline.extend([0.0]*us)
            self.sell_online.extend([0.0]*us)

        for _id in orders.keys():
            orderEvent = orders[_id]
            if orderEvent.status in [ESTATUS_ORDER_CLOSED, ESTATUS_TRADE_CLOSED]:
                pass

    def onSignal(self, event):
        price = event.price
        side = event.side
        status = event.status
        if status == ESTATUS_TRADE_OPENED:
            if side == SIDE_BUY:
                self.buy_offline[self.pos] -= price
                self.buy_online[self.pos] += price
            if side == SIDE_SELL:
                self.sell_offline[self.pos] += price
                self.sell_online[self.pos] -= price
        if status == ESTATUS_TRADE_CLOSED:
            if side == SIDE_BUY:
                self.buy_offline[self.pos] += price
                self.buy_online[self.pos] -= price
            if side == SIDE_SELL:
                self.sell_offline[self.pos] -= price
                self.sell_online[self.pos] += price
