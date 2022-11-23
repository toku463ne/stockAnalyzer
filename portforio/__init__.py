from consts import *

import copy

class Portoforio(object):
    def __init__(self, unitSize=1000):
        self.unitSize = unitSize
        self.history = {}
        self.last_hist = {}
        self.order_hist = {}
        self.trade_count = 0
        self.trades = {}

    def getId(self, epoch, orderId):
        return "%d_%d" % (epoch, orderId)

    def onSignal(self, epoch, event):
        orderId = event.id
        #epoch = event.epoch
        _id = self.getId(epoch, orderId)
        price = event.price
        units = event.units
        total = price * units
        side = event.side
        status = event.status
        history = self.history
        h = {}
        h["buy_offline"] = 0
        h["buy_online"] = 0
        h["sell_offline"] = 0
        h["sell_online"] = 0
        if status == ESTATUS_TRADE_OPENED or status == ESTATUS_TRADE_CLOSED:
            if len(history) > 0:
                h = self.last_hist
            h["epoch"] = epoch
            h["orderId"] = orderId
            h["codename"] = event.codename
            h["granularity"] = event.granularity
            h["price"] = price
            h["units"] = units

        if status == ESTATUS_TRADE_OPENED:
            self.trades[orderId] = {
                "codename": event.codename,
                "open": {
                    "price": price,
                    "epoch": epoch
                    },
                "side": side
                }
            self.order_hist[orderId] = [_id]
            if side == SIDE_BUY:
                h["buy_offline"] -= total
                h["buy_online"] += total
            if side == SIDE_SELL:
                h["sell_offline"] += total
                h["sell_online"] -= total
            self.trade_count += 1
            h["trade_count"] = self.trade_count
            
        if status == ESTATUS_TRADE_CLOSED:
            self.trades[orderId]["close"] = {
                "price": price,
                "epoch": epoch
            }
            diff = self.trades[orderId]["close"]["price"] - self.trades[orderId]["open"]["price"]
            order_hist = self.order_hist[orderId]
            h1 = self.history[order_hist[0]]
            order_hist.append(_id)
            last_total = h1["price"] * h1["units"]
            if side == SIDE_BUY:
                h["buy_offline"] += total
                h["buy_online"] -= last_total
                if diff > 0:
                    self.trades[orderId]["result"] = "win"
                else:
                    self.trades[orderId]["result"] = "lose"
            if side == SIDE_SELL:
                h["sell_offline"] -= total
                h["sell_online"] += last_total
                if diff < 0:
                    self.trades[orderId]["result"] = "win"
                else:
                    self.trades[orderId]["result"] = "lose"
        self.last_hist = h
        if len(h.keys()) > 0:
            self.history[_id] = copy.deepcopy(h)
            

    
    def getReport(self):
        return self.last_hist

    def getHistory(self):
        return self.history

    def getTrades(self):
        return self.trades