from strategy import Strategy
import lib.tradelib as tradelib
from consts import *
from ticker.zigzag import Zigzag
from analyzer.zz_analyzer import ZzAnalyzer
import data_getter

import pandas as pd

min_profit = 100
zz_size = 5
n_points = 5
min_km_count = 10
max_orders = 3
max_std = 0.2
min_trade_len = 2

class ZzStatsStrategy(Strategy):
    
    def __init__(self, args, use_master=False):
        self.initAttrFromArgs(args, "codenames")
        self.initAttrFromArgs(args, "granularity")
        self.initAttrFromArgs(args, "kmpkl_file")
        self.initAttrFromArgs(args, "n_points", n_points)
        self.initAttrFromArgs(args, "zz_size", zz_size)
        self.unitsecs = tradelib.getUnitSecs(self.granularity)
        self.initAttrFromArgs(args, "min_profit", min_profit)
        self.initAttrFromArgs(args, "min_km_count", min_km_count)
        self.initAttrFromArgs(args, "max_orders", max_orders)
        self.initAttrFromArgs(args, "max_std", max_std)
        self.initAttrFromArgs(args, "min_trade_len", min_trade_len)
        

        self.analyzer = ZzAnalyzer(self.granularity, 
            self.n_points, 
            self.kmpkl_file, 
            zz_size=self.zz_size,
            use_master=use_master)
        self.analyzer.loadKmModel()
        self.tickers = {}
        
        
    
    def onTick(self, epoch):
        granularity = self.granularity
        n_points = self.n_points
        zz_size = self.zz_size
        startep = epoch - n_points * zz_size * 3 * self.unitsecs
        
        if len(self.tickers) == 0:
            for codename in self.codenames:
                self.tickers[codename] = Zigzag(codename, granularity, 
                        startep, endep=epoch, size=zz_size)

        index = []
        cnts = []
        xs = []
        ys = []
        stdxs = []
        stdys = []
        nstdxs = []
        nstdys = []
        last_dirs = []
        min_epoch = epoch - self.unitsecs * self.min_trade_len
        min_cnt = self.min_km_count
        for codename in self.codenames:
            z = self.tickers[codename]
            z.tick(epoch)
            if z.updated:
                (ep, _ , dirs, prices) = z.getData(n=n_points-1)
                if len(ep) < n_points-1:
                    continue
                (cnt, x, y, stdx, stdy, nstdx, nstdy) = self.analyzer.predictNext(ep, prices)
                if cnt >= min_cnt:
                    index.append(codename)
                    cnts.append(cnt)
                    xs.append(x)
                    ys.append(y)
                    stdxs.append(stdx)
                    stdys.append(stdy)
                    nstdxs.append(nstdx)
                    nstdys.append(nstdy)
                    last_dirs.append(dirs[-1])

        if len(index) == 0:
            return []

        df = pd.DataFrame({
                "codename": index,
                "cnt": cnts, "x": xs, "y": ys, 
                "stdx": stdxs, "stdy": stdys,
                "nstdx": nstdxs, "nstdy": nstdys,
                "last_dirs": last_dirs
            }, 
        index=index)

        df = df[df["nstdx"] <= self.max_std]
        df = df[df["nstdy"] <= self.max_std]
        df = df[df["x"] - df["stdx"] >= min_epoch]

        if len(df) == 0:
            return []

        granularity = self.granularity
        min_profit = self.min_profit
        orders = []
        for r in df.itertuples():
            dg = data_getter.getDataGetter(r.codename, granularity)
            (now, _, _, h, l, c, _) = dg.getPrice(epoch)
            price = (h+l+c)/3

            profit = r.y - r.stdy - price
            if abs(profit) < min_profit:
                continue
            side = SIDE_BUY
            if profit < 0:
                side = SIDE_SELL

            tp = r.y
            sl = price - abs(tp-price)*side
            
            order = self.createMarketOrder(now,
                    dg, side, 1, price,
                    takeprofit=tp, 
                    stoploss=sl)

            orders.append(order)
        return orders
        
        
    def onSignal(self, epoch, event):
        pass