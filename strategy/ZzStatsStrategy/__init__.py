from strategy import Strategy
import lib.tradelib as tradelib
import lib
from consts import *
from ticker.zigzag import Zigzag
import data_getter
from predictor.zz import ZzPredictor

import pandas as pd
import math

min_profit = 10000
zz_size = 5
n_points = 5
min_trade_len = 2
max_fund = 100000
min_unit = 100
min_volume = 100000
max_trades_a_day = 1
trade_mode = TRADE_MODE_BOTH

class ZzStatsStrategy(Strategy):
    
    def __init__(self, args, use_master=False, load_zzdb=False):
        self.initAttrFromArgs(args, "granularity")
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

        self.predictor = ZzPredictor(config=args)
        self.codenames = self.predictor.codenames


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
        max_duration = self.zz_size*self.unitsecs*2
        max_fund = self.max_fund
        min_unit = self.min_unit
        trade_mode = self.trade_mode
        deflectedKms = self.deflectedKms

        df = self.predictor.search(self.tickers, epoch)

        if len(df) == 0:
            return []

        df = df.sort_values(by=["last_price"], ascending=True)

        granularity = self.granularity
        min_profit = self.min_profit
        orders = []
        n = 1
        for r in df.itertuples():
            dg = data_getter.getDataGetter(r.codename, granularity)
            (now, _, _, _, _, price, v) = dg.getPrice(epoch)
            if v < self.min_volume:
                continue
            
            if r.x < min_epoch:
                continue

            profit = r.y
            if profit == 0:
                continue

            ideal_unit = math.ceil(min_profit/abs(profit))
            unit = min_unit * math.ceil(ideal_unit/min_unit)

            if unit * price > max_fund:
                continue

            side = SIDE_BUY
            if r.last_dir > 0:
                side = SIDE_SELL

            if side * profit <= 0:
                continue 

            tp = price + profit
            sl = price - profit

            if r.score1 > r.score2:
                tmp = tp
                tp = sl
                sl = tmp
                side *= -1

            if trade_mode == TRADE_MODE_ONLY_BUY and side != SIDE_BUY:
                continue

            if trade_mode == TRADE_MODE_ONLY_SELL and side != SIDE_SELL:
                continue
            
            expiration = epoch + max_duration

            #print("create order: %s code=%s price=%2f tp=%2f sl=%2f side=%d unit=%d km=%s lose_rate=%2f" % (
            #    lib.epoch2dt(epoch), r.codename, price, tp, sl, side, unit, r.km_groupid, r.lose_rate))
            order = self.createMarketOrder(now,
                    dg, side, unit,
                    takeprofit=tp, 
                    stoploss=sl,
                    expiration=expiration,
                    desc="%s" % (r.km_groupid)) 

            orders.append(order)

            n += 1
            if n > self.max_trades_a_day:
                break
        return orders
        
        
    def onSignal(self, epoch, event):
        pass