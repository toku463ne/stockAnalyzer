
from predictor import Predictor
import analyzer.zz_analyzer as zz
from ticker.zigzag import Zigzag
import lib
import lib.tradelib as tradelib
from consts import *

import pandas as pd
from datetime import datetime

class ZzPredictor(Predictor):
    def __init__(self, config={}):
        self.initAttrFromArgs(config, "granularity")
        self.initAttrFromArgs(config, "km_setid")
        self.initAttrFromArgs(config, "min_cnt_a_year", 10)
        self.initAttrFromArgs(config, "deflect_rate", 0.65)
        self.initAttrFromArgs(config, "min_feed_year_rate", 0.5)
        self.initAttrFromArgs(config, "min_feed_score", 0.6)
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.unitsecs = tradelib.getUnitSecs(self.granularity)
        
        
        a = zz.ZzAnalyzer(config=config, use_master=True)
        a.loadKmModel(self.km_setid)
        self.n_feed_years = a.getNFeededYears()

        kms = a.getDeflectedKmGroups(deflect_rate=self.deflect_rate, 
            min_cnt_a_year=self.min_cnt_a_year)
        kms = kms[kms["total"] >= self.n_feed_years*self.min_feed_year_rate]
        kms = kms[(kms["score1"] > self.min_feed_score) | (kms["score2"] > self.min_feed_score)]
        self.deflectedKms = kms
        self.analyzer = a
        self.codenames = a.codenames

    def getTickers(self, epoch):
        startep = epoch - self.n_points * self.zz_size * 3 * self.unitsecs
        granularity = self.analyzer.granularity
        zz_size = self.analyzer.zz_size
        tickers = {}
        for codename in self.analyzer.codenames:
            tickers[codename] = Zigzag(codename, granularity, 
                    startep, endep=epoch, size=zz_size, load_db=True)
        return tickers

    def search(self, tickers, epoch):
        deflectedKms = self.deflectedKms
        n_points = self.analyzer.n_points
        predictNext = self.analyzer.predictNext
        index = []
        xs = []
        ys = []
        scores1 = []
        scores2 = []
        last_dirs = []
        last_prices = []
        km_ids = []
        codenames = self.analyzer.codenames
        for codename in codenames:
            z = tickers[codename]
            z.tick(epoch)
            if z.updated:
                (ep, dt , dirs, prices) = z.getData(n=n_points-1, 
                        zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE)
                if len(ep) < n_points-1:
                    continue
                (x, y, km_id) = predictNext(ep, prices)

                if km_id not in deflectedKms.index:
                    continue

                row = deflectedKms[deflectedKms.index==km_id]
                score1 = row["score1"].values[0]
                score2 = row["score2"].values[0]

                index.append(codename)
                xs.append(x)
                ys.append(y)
                scores1.append(score1)
                scores2.append(score2)
                last_dirs.append(dirs[-1])
                last_prices.append(prices[-1])
                km_ids.append(km_id)

        if len(index) == 0:
            return []

        df = pd.DataFrame({
                "codename": index,
                "x": xs, "y": ys, 
                "score1": scores1,
                "score2": scores2,
                "last_dir": last_dirs,
                "last_price": last_prices,
                "km_id": km_ids
            }, 
        index=index)

        return df