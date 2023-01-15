from consts import *
import lib
import copy
from db.mysql import MySqlDB
        

class Portoforio(object):
    def __init__(self, trade_name):
        self.trade_name = trade_name
        self.history = {}
        self.last_hist = {}
        self.order_hist = {}
        self.trade_count = 0
        self.trades = {}
        self.wins = 0
        self.loses = 0
        db = MySqlDB()
        db.createTable("trades")
        db.createTable("trade_history")
        self.db = db

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
            h["side"] = side
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
                    "epoch": epoch,
                    "desc": event.desc
                    },
                "side": side,
                "takeprofit_price": event.takeprofit_price,
                "stoploss_price": event.stoploss_price,
                "expiration": event.expiration,
                "units": units
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
                "epoch": epoch,
                "desc": event.desc
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
                    self.wins += 1
                else:
                    self.trades[orderId]["result"] = "lose"
                    self.loses += 1
            if side == SIDE_SELL:
                h["sell_offline"] -= total
                h["sell_online"] += last_total
                if diff < 0:
                    self.trades[orderId]["result"] = "win"
                    self.wins += 1
                else:
                    self.trades[orderId]["result"] = "lose"
                    self.loses += 1
            t = self.trades[orderId]
            print("[%s]%s: %s-%s code=%s open=%2f close=%2f side=%d desc=%s" % (t["result"],
                orderId, lib.epoch2str(t["open"]["epoch"], "%Y%m%d"),lib.epoch2str(epoch, "%Y%m%d"), 
                t["codename"], t["open"]["price"], t["close"]["price"], 
                t["side"], t["open"]["desc"]))
            self.insertResult(orderId)
        self.insertTradeHistory(h)
        self.last_hist = h
        if len(h.keys()) > 0:
            self.history[_id] = copy.deepcopy(h)
    
    def getReport(self):
        return self.last_hist

    def getHistory(self):
        return self.history

    def getBuyOffLine(self):
        h = self.last_hist
        if len(h) == 0:
            return 0
        return abs(h["buy_offline"])

    def getSellOffLine(self):
        h = self.last_hist
        if len(h) == 0:
            return 0
        return abs(h["sell_offline"])
    

    def getTrades(self):
        return self.trades

    def clearDB(self):
        self.db.execSql("delete from trades where trade_name = '%s'" % (self.trade_name))
        self.db.execSql("delete from trade_history where trade_name = '%s'" % (self.trade_name))

    def insertTradeHistory(self, h):
        sql = """insert into trade_history(trade_name, epoch, reference_datetime, order_id,
codename, side, price, units, 
buy_offline, buy_online, sell_offline, sell_online) 
values('%s', %d, '%s', '%s',
'%s', %d, %f, %d,
%f, %f, %f, %f)""" % (self.trade_name, h["epoch"], lib.epoch2str(h["epoch"]), h["orderId"],
h["codename"], h["side"], h["price"], h["units"],
h["buy_offline"], h["buy_online"], h["sell_offline"], h["sell_online"])

        self.db.execSql(sql)
        

    def insertResult(self, orderId):
        trade_name = self.trade_name
        sql = """insert into trades(trade_name,order_id,codename,result,profit,side,units,
expiration_epoch,expiration_datetime,
open_price,open_epoch,open_datetime,open_desc,
takeprofit_price,stoploss_price,
close_price,close_epoch,close_datetime,close_desc)
values"""

        t = self.trades[orderId]
        side = t["side"]
        units = t["units"]
        profit = 0
        open_price = t["open"]["price"]
        close_price = t["close"]["price"]
        if side == SIDE_BUY:
            profit = (close_price - open_price)*units
        if side == SIDE_SELL:
            profit = (open_price - close_price)*units
        sql += """('%s','%s','%s','%s',%f,%d,%d,
%d,'%s',
%f,%d,'%s','%s',
%f,%f,
%f,%d,'%s','%s'
)""" % (trade_name,orderId,t["codename"],t["result"],profit,side,units,
t["expiration"],lib.epoch2str(t["expiration"]),
open_price,t["open"]["epoch"],lib.epoch2str(t["open"]["epoch"]),t["open"]["desc"],
t["takeprofit_price"], t["stoploss_price"],
close_price,t["close"]["epoch"],lib.epoch2str(t["close"]["epoch"]),t["close"]["desc"])

        self.db.execSql(sql)




