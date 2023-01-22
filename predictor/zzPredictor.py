
from predictor import Predictor
import analyzer.zz_analyzer as zz
from ticker.zigzag import Zigzag
import lib
import lib.tradelib as tradelib
from consts import *

from datetime import datetime

class ZzPredictor(Predictor):
    def __init__(self, config={}):
        self.initAttrFromArgs(config, "predict_period", PREDICT_PERIOD)
        self.initAttrFromArgs(config, "km_setid")
        self.initAttrFromArgs(config, "min_cnt_a_year", 10)
        self.initAttrFromArgs(config, "deflect_rate", 0.65)

        zz = zz.ZzAnalyzer(config=config, use_master=True)
        zz.loadKmModel(self.km_setid)
        self.deflectedKms = zz.getDeflectedKmGroups(deflect_rate=self.deflect_rate, 
            min_cnt_a_year=min_cnt_a_year)
        self.zz = zz

    def getTickers(self, epoch):
        startep = epoch - self.predict_period
        for codename in self.zz.codenames:
            tickers[codename] = Zigzag(codename, zz.granularity, 
                    startep, endep=epoch, size=zz.zz_size, load_db=True)
        return tickers

    def search(self, tickers, epoch):
        deflectedKms = self.deflectedKms
        for codename in self.zz.codenames:
            z = tickers[codename]
            z.tick(epoch)
            if z.updated:
                (ep, dt , dirs, prices) = z.getData(n=n_points-1, zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE)
                if len(ep) < n_points-1:
                    continue
                (x, y, km_groupid) = self.zz.predictNext(ep, prices)

                if km_groupid not in deflectedKms.index:
                    continue

                row = deflectedKms[deflectedKms.index==km_groupid]
                score = row["score"].values[0]
                score1 = row["score1"].values[0]
                score2 = row["score2"].values[0]

                if x+epoch < min_epoch:
                    continue

                index.append(codename)
                xs.append(x)
                ys.append(y)
                scores.append(score)
                scores1.append(score1)
                scores2.append(score2)
                last_dirs.append(dirs[-1])
                last_prices.append(prices[-1])
                km_groupids.append(km_groupid)

        if len(index) == 0:
            return []

        df = pd.DataFrame({
                "codename": index,
                "x": xs, "y": ys, 
                "score": scores,
                "score1": scores1,
                "score2": scores2,
                "last_dir": last_dirs,
                "last_price": last_prices,
                "km_groupid": km_groupids
            }, 
        index=index)

        return df