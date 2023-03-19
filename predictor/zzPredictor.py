
from predictor import Predictor
import analyzer.zz.zz_analyzer as zz
from ticker.zigzag import Zigzag
import lib
import lib.tradelib as tradelib
from consts import *

import pandas as pd
from datetime import datetime

class ZzPredictor(Predictor):
    def __init__(self, config={}):
        self.initAttrFromArgs(config, "granularity")
        self.initAttrFromArgs(config, "min_feed_year_rate", 0.5)
        self.initAttrFromArgs(config, "min_feed_score", 0.7)
        self.initAttrFromArgs(config, "min_km_count", 100)
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "zz_middle_size", 2)
        self.unitsecs = tradelib.getUnitSecs(self.granularity)
        
        self.config = config
        
        a = zz.ZzAnalyzer(config=config)
        a.loadKmModel()
        self.analyzer = a
        self.codenames = a.codenames

    def getTickers(self, epoch, buffNbars=0):
        startep = epoch - self.n_points * self.zz_size * 3 * self.unitsecs
        granularity = self.analyzer.granularity
        zz_size = self.analyzer.zz_size
        tickers = {}
        for codename in self.analyzer.codenames:
            config = {
                "codename": codename,
                "granularity": self.granularity,
                "startep": startep,
                "endep": epoch,
                "size": self.zz_size,
                "middle_size": self.zz_middle_size
            }
            tickers[codename] = Zigzag(config)
        return tickers

    def search(self, tickers, epoch):
        n_points = self.analyzer.n_points
        min_km_count = self.min_km_count
        min_feed_score = self.min_feed_score
        predictNext = self.analyzer.predictNext
        index = []
        ys = []
        item_cnts = []
        scores = []
        last_prices = []
        last_eps = []
        km_ids = []
        codenames = self.analyzer.codenames
        for codename in codenames:
            z = tickers[codename]
            z.tick(epoch)
            if z.updated:
                (ep, dt , dirs, prices, vols) = z.getData(n=n_points-1, 
                        zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE, do_update_flag_change=False)
                if len(ep) < n_points-1:
                    continue

                # middle_peak_ep, middle_peak_price, last_ep, last_price
                (last_middle_peak_ep, last_middle_peak_price, last_ep, last_price) = z.getLastMiddlePeak()
                (km_id, score, y, item_cnt) = predictNext(ep + [last_middle_peak_ep, last_ep], 
                    prices + [last_middle_peak_price, last_price])

                #if score1 < self.min_feed_score or score2 < self.min_feed_score:
                if score < min_feed_score:
                    continue

                if item_cnt < min_km_count:
                    continue

                index.append(codename)
                #xs.append(x)
                ys.append(y)
                item_cnts.append(item_cnt)
                scores.append(score)
                last_prices.append(prices[-1])
                last_eps.append(ep[-1])
                km_ids.append(km_id)

        if len(index) == 0:
            return []

        df = pd.DataFrame({
                "codename": index,
                #"x": xs, 
                "y": ys, 
                "item_cnt": item_cnts,
                "score": scores,
                "last_price": last_prices,
                "last_ep": last_eps,
                "km_id": km_ids
            }, 
        index=index)

        return df

    def plot(self, df):
        km_ids = df.km_id.tolist()
        km_setid = self.km_setid
        self.analyzer.plotKmGroups(km_setid, km_ids)