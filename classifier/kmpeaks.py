
from classifier.kmeans import KmClassifier
import pandas as pd
import numpy as np
import scipy
import pickle
from consts import *

class KmPeaksClassifier(KmClassifier):
    def __init__(self, config={}):
        super(KmPeaksClassifier, self).__init__(config)
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "deflect_rate", 0.65)
        self.initAttrFromArgs(config, "score_period", "year") # month, day, hour
        self.type_name = CLF_TYPE_KM_PEAK

    def getNextK(self, k, score, max_score, k_limit, k_step_size=1):
        if score >= self.enough_score:
            return 0, True
        if k+k_step_size > k_limit:
            return 0, True
        return k+k_step_size, False

    def getClassId(self, v):
        def get_b(d):
            b = "0"
            if abs(d) >= 0.01:
                if d > 0:
                    b = "1"
                else:
                    b = "2"
            return b

        if v is None:
            return ""

        n1 = self.n_points-2
        n2 = self.n_points-3
        n3 = self.n_points-4
        g = [0]*(n1 + n2 + n3)
        for j in range(n1):
            d = v[j*2+3]-v[j*2+1]
            g[j] = get_b(d)
        starti = n1
        for j in range(n2):
            d = v[j*2+5]-v[j*2+1]
            g[starti+j] = get_b(d)
        starti = n1+n2
        for j in range(n3):
            d = v[j*2+7]-v[j*2+1]
            g[starti+j] = get_b(d)
        
        gid = "".join(g)
        return "pek" + gid


    def calcScore(self, df, k):
        if len(df) < self.min_km_size:
            return (0, [0]*k, [0]*k, [0]*k)
        max_score = 0
        scores = []
        expects = []
        cnts = []
        for i in range(k):
            if k > 1:
                subdf = df.loc[df["km_val"] == i]
            else:
                subdf = df
            n = len(subdf)
            if n < self.min_km_size:
                scores.append(0)
                expects.append(0)
                cnts.append(len(subdf))
                continue
            (score, expected) = self._calcScoreOfKmVal(subdf)
            scores.append(score)
            expects.append(expected)
            cnts.append(len(subdf))
            if score > max_score:
                max_score = score
        return max_score, scores, expects, cnts


    def _calcScoreOfKmVal(self, df):
        score_period = self.score_period
        if score_period == "year":
            df.insert(0, "period", df["dt"].dt.year, True)
        elif score_period == "month":
            df.insert(0, "period", df["dt"].dt.month, True)
        elif score_period == "day":
            df.insert(0, "period", df["dt"].dt.day, True)
        else:
            df["period"] = df["dt"].dt

        udf = df[df["result"]>0.1][["period", "result"]].rename(columns={"result": "up"})
        ddf = df[df["result"]<-0.1][["period", "result"]].rename(columns={"result": "down"})
        
        dfp = df[["period", "result"]].groupby("period").count().rename(columns={"result": "total"})
        udfp = udf.groupby("period").count()
        ddfp = ddf.groupby("period").count()

        adfp = pd.merge(dfp, udfp, on=["period"], how="outer").replace(np.nan, 0)
        adfp = pd.merge(adfp, ddfp, on=["period"], how="outer").replace(np.nan, 0)
        total = len(adfp)
        ucnt = adfp[adfp["up"]/adfp["total"] > 0.5]["total"].count()
        dcnt = adfp[adfp["down"]/adfp["total"] > 0.5]["total"].count()

        score1 = max(ucnt, dcnt)/total
        score2 = max(udf["up"].count(),ddf["down"].count())/(df["result"].count())
        
        expected = 0
        if ucnt > dcnt:
            expected = scipy.stats.trim_mean(udf["up"],0.3)
        elif ucnt < dcnt:
            expected = scipy.stats.trim_mean(ddf["down"],0.3)

        score = score1 * score2
        return score, expected
        

    def calcTrendSum(self, term="year", min_cnt=10):
        deflect_rate = self.deflect_rate
        return df


    