from strategy import Strategy
import lib.tradelib as tradelib
import lib
from consts import *
from ticker.zigzag import Zigzag
import data_getter
from predictor.zzPredictor import ZzPredictor

import pandas as pd
import math
import copy

min_profit = 10000
zz_size = 5
n_points = 5
min_trade_len = 5
max_fund = 100000
min_unit = 100
min_volume = 100000
max_trades_a_day = 1
trade_mode = TRADE_MODE_BOTH

class ZzStatsStrategy(Strategy):
    
    def __init__(self, args, use_master=False, load_zzdb=False):
        self.initAttrFromArgs(args, "granularity")
        self.initAttrFromArgs(args, "km_setname")
        self.initAttrFromArgs(args, "n_points", n_points)
        self.initAttrFromArgs(args, "zz_size", zz_size)
        self.unitsecs = tradelib.getUnitSecs(self.granularity)
        self.initAttrFromArgs(args, "min_profit", min_profit)
        self.initAttrFromArgs(args, "min_trade_len", min_trade_len)
        self.initAttrFromArgs(args, "max_fund", max_fund)
        self.initAttrFromArgs(args, "min_unit", min_unit)
        self.initAttrFromArgs(args, "min_volume", min_volume)
        self.initAttrFromArgs(args, "market", "")
        self.initAttrFromArgs(args, "max_trades_a_day", max_trades_a_day)
        self.initAttrFromArgs(args, "trade_mode", trade_mode)
        self.load_zzdb = load_zzdb

        self.tickers = {}
        self.predictor = ZzPredictor(config=args)
        self.codenames = self.predictor.codenames
        self.orders = {}


    def preProcess(self, timeTicker, portforio):
        super().preProcess(timeTicker, portforio)
        granularity = self.granularity
        startep = timeTicker.startep - self.n_points * self.zz_size * 3 * self.unitsecs
        endep = timeTicker.endep
        for codename in self.codenames:
            self.tickers[codename] = Zigzag(codename, granularity, 
                    startep, endep=endep, size=zz_size, load_db=self.load_zzdb)
        

        
    def onTick(self, epoch):
        granularity = self.granularity
        min_epoch = epoch + self.unitsecs * self.min_trade_len
        max_duration = self.zz_size*self.unitsecs*3
        max_fund = self.max_fund
        min_unit = self.min_unit
        trade_mode = self.trade_mode
        tickers = self.tickers
        orders = []
        
        
        df = self.predictor.search(tickers, epoch)
        
        lkeys = []
        for localId in self.orders.keys():
            lkeys.append(localId)
        for localId in lkeys:
            order = self.orders[localId]
            _id = order.id
            #if ticker.updated and _id >= 0 and order.expected_end > epoch:
            if _id >= 0 and order.expected_end < epoch:
                ticker = tickers[order.codename]
                #trade_open_price
                (_, _, o, h, l, c, _) = ticker.getRecentOhlcv(self.n_points)
                do_cancel = False
                side = order.side
                #trade_open_price = order.trade_open_price
                if h[-1] == max(h):
                    if side == SIDE_BUY:
                        do_cancel = True
                elif l[-1] == min(l):
                    if side == SIDE_SELL:
                        do_cancel = True
                if do_cancel:
                    orders.append(self.cancelOrder(epoch, ticker.dg, _id=_id))
                    del self.orders[localId]

        if len(df) == 0:
            return orders


        df = df.sort_values(by=["score"], ascending=False)

        granularity = self.granularity
        min_profit = self.min_profit
        n = 1
        for r in df.itertuples():
            dg = data_getter.getDataGetter(r.codename, granularity)
            (now, _, _, _, _, price, v) = dg.getPrice(epoch)
            if v < self.min_volume:
                continue
            
            expected_end = max(epoch + r.x, min_epoch)
            expiration = epoch + max_duration

            if expiration < min_epoch:
                continue

            if r.x-epoch > max_duration:
                continue

            tp = price + r.y
            profit = r.y


            
            ideal_unit = math.ceil(min_profit/abs(profit))
            unit = min_unit * math.floor(ideal_unit/min_unit)
            if unit == 0:
                unit = min_unit

            if abs(profit) * unit < min_profit:
                continue


            if unit * price > max_fund:
                continue

            side = SIDE_BUY
            if profit < 0:
                side = SIDE_SELL

            #tp = r.y
            sl = price - profit


            if trade_mode == TRADE_MODE_ONLY_BUY and side != SIDE_BUY:
                continue

            if trade_mode == TRADE_MODE_ONLY_SELL and side != SIDE_SELL:
                continue
            
            #expiration = 0

            #print("create order: %s code=%s price=%2f tp=%2f sl=%2f side=%d unit=%d km=%s lose_rate=%2f" % (
            #    lib.epoch2dt(epoch), r.codename, price, tp, sl, side, unit, r.km_groupid, r.lose_rate))
            order = self.createMarketOrder(now,
                    dg, side, unit,
                    takeprofit=tp, 
                    stoploss=sl,
                    expiration=expiration,
                    desc="%s" % (r.km_id)) 

            order.expected_end = expected_end

            orders.append(order)
            self.orders[order.localId] = order

            n += 1
            if n > self.max_trades_a_day:
                break
        return orders
        
        
    def onSignal(self, epoch, event):
        if event.status == ESTATUS_TRADE_CLOSED:
            delkeys = []
            if event.localId in self.orders.keys():
                delkeys.append(event.localId)
            
            for localId in delkeys:
                del self.orders[localId] 