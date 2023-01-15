from strategy import Strategy
import lib.tradelib as tradelib
import lib
from consts import *
from ticker.zigzag import Zigzag
from analyzer.zz_analyzer import ZzAnalyzer
import data_getter

import pandas as pd
import math

min_profit = 10000
zz_size = 5
n_points = 5
min_km_count = 20
min_feed_year_rate = 0.5
min_feed_score = 0.6
max_orders = 3
max_std = 0.3
max_lose_rate = 0.3
min_trade_len = 2
max_fund = 100000
min_unit = 100
min_volume = 100000
max_trades_a_day = 1
trade_mode = TRADE_MODE_ONLY_BUY

class ZzStatsStrategy(Strategy):
    
    def __init__(self, args, use_master=False, load_zzdb=False):
        self.initAttrFromArgs(args, "codenames", [])
        self.initAttrFromArgs(args, "granularity")
        self.initAttrFromArgs(args, "kmpkl_file")
        self.initAttrFromArgs(args, "n_points", n_points)
        self.initAttrFromArgs(args, "zz_size", zz_size)
        self.unitsecs = tradelib.getUnitSecs(self.granularity)
        self.initAttrFromArgs(args, "min_profit", min_profit)
        self.initAttrFromArgs(args, "min_km_count", min_km_count)
        self.initAttrFromArgs(args, "max_orders", max_orders)
        self.initAttrFromArgs(args, "max_std", max_std)
        self.initAttrFromArgs(args, "max_lose_rate", max_lose_rate)
        self.initAttrFromArgs(args, "min_trade_len", min_trade_len)
        self.initAttrFromArgs(args, "max_fund", max_fund)
        self.initAttrFromArgs(args, "min_unit", min_unit)
        self.initAttrFromArgs(args, "min_volume", min_volume)
        self.initAttrFromArgs(args, "market", "")
        self.initAttrFromArgs(args, "max_trades_a_day", max_trades_a_day)
        self.initAttrFromArgs(args, "trade_mode", trade_mode)
        self.initAttrFromArgs(args, "min_feed_year_rate", min_feed_year_rate)
        self.initAttrFromArgs(args, "min_feed_score", min_feed_score)
        
        self.load_zzdb = load_zzdb

        self.analyzer = ZzAnalyzer(self.granularity, 
            self.n_points, 
            self.kmpkl_file, 
            zz_size=self.zz_size,
            use_master=use_master)
        self.analyzer.loadKmModel()
        self.n_feed_years = self.analyzer.getNFeededYears()
        
        kms = self.analyzer.getDeflectedKmGroups()
        kms = kms[kms["total"] >= self.n_feed_years*self.min_feed_year_rate]
        kms = kms[(kms["score1"] > min_feed_score) | (kms["score2"] > min_feed_score)]
        self.deflectedKms = kms

        self.tickers = {}


    def preProcess(self, timeTicker, portforio):
        super().preProcess(timeTicker, portforio)
        granularity = self.granularity
        startep = timeTicker.startep - self.n_points * self.zz_size * 3 * self.unitsecs
        endep = timeTicker.endep
        if len(self.codenames) == 0:
            self.codenames = self.analyzer.getCodenamesFromDB(lib.epoch2dt(startep).year, 
                self.market, "<=")
        for codename in self.codenames:
            self.tickers[codename] = Zigzag(codename, granularity, 
                    startep, endep=endep, size=zz_size, load_db=self.load_zzdb)
        

        
    def onTick(self, epoch):
        granularity = self.granularity
        n_points = self.n_points
        n_feed_years = self.n_feed_years
        
        index = []
        cnts = []
        lose_rates = []
        xs = []
        ys = []
        stdxs = []
        stdys = []
        nstdxs = []
        nstdys = []
        last_dirs = []
        last_prices = []
        km_groupids = []
        min_epoch = epoch + self.unitsecs * self.min_trade_len
        max_duration = self.zz_size*self.unitsecs*2
        min_cnt = self.min_km_count
        max_fund = self.max_fund
        min_unit = self.min_unit
        trade_mode = self.trade_mode
        deflectedKms = self.deflectedKms


        #if epoch == 1641513600:
        #    print(epoch)

        for codename in self.codenames:
            z = self.tickers[codename]
            z.tick(epoch)
            if z.updated:
                (ep, dt , dirs, prices) = z.getData(n=n_points-1, zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE)
                if len(ep) < n_points-1:
                    continue
                vals = self.analyzer._normalizeItem(ep, prices)
                if vals is None:
                    continue
                (cnt, lose_cnt, 
                x, y, stdx, stdy, 
                nstdx, nstdy, km_groupid) = self.analyzer.predictNext(vals, ep, prices)

                if km_groupid not in deflectedKms.index:
                    continue
                
                lose_rate = lose_cnt/cnt
                #if lose_rate < 1-max_lose_rate and lose_rate > max_lose_rate:
                #    continue

                #if cnt < min_cnt:
                #    continue

                if x < min_epoch:
                    continue

                index.append(codename)
                cnts.append(cnt)
                lose_rates.append(lose_cnt*1.0/cnt)
                xs.append(x)
                ys.append(y)
                stdxs.append(stdx)
                stdys.append(stdy)
                nstdxs.append(nstdx)
                nstdys.append(nstdy)
                last_dirs.append(dirs[-1])
                last_prices.append(prices[-1])
                km_groupids.append(km_groupid)

        if len(index) == 0:
            return []

        df = pd.DataFrame({
                "codename": index,
                "cnt": cnts, 
                "lose_rate": lose_rates, 
                "x": xs, "y": ys, 
                "stdx": stdxs, "stdy": stdys,
                "nstdx": nstdxs, "nstdy": nstdys,
                "last_dir": last_dirs,
                "last_price": last_prices,
                "km_groupid": km_groupids
            }, 
        index=index)

        if len(df) == 0:
            return []

        df = df.sort_values(by=["cnt", "lose_rate"], ascending=False)

        granularity = self.granularity
        min_profit = self.min_profit
        orders = []
        n = 1
        for r in df.itertuples():
            dg = data_getter.getDataGetter(r.codename, granularity)
            (now, _, _, _, _, price, v) = dg.getPrice(epoch)
            if v < self.min_volume:
                continue
            
            profit = r.y - price
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

            tp = 0
            if side == SIDE_BUY:
                tp = max(r.y, (price-r.last_price)+price)
            if side == SIDE_SELL:
                tp = min(r.y, price-(r.last_price-price))
            sl = price - (tp-price)

            if r.lose_rate >= 1- self.max_lose_rate:
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
                    desc="km_group=%s lose_rate=%2f" % (r.km_groupid, r.lose_rate)) 

            orders.append(order)

            n += 1
            if n > self.max_trades_a_day:
                break
        return orders
        
        
    def onSignal(self, epoch, event):
        pass