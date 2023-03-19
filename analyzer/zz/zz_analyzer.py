import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from env import *
from consts import *
import lib.naming as naming
from analyzer import Analyzer
from analyzer.zz.code_manager import CodeManager
from analyzer.zz.peaks_manager import PeaksManager
from db.mysql import MySqlDB
import db.mydf as mydf


class ZzAnalyzer(Analyzer):
    def __init__(self, config):
        self.initAttrFromArgs(config, "granularity", "D")
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "zz_middle_size", 2)
        self.initAttrFromArgs(config, "codenames", [])
        self.initAttrFromArgs(config, "n_candles", 3)
        self.initAttrFromArgs(config, "obsyear", 0)
        self.initAttrFromArgs(config, "use_master", False)
        self.initAttrFromArgs(config, "recreate", False)
        self.initAttrFromArgs(config, "clf_name", "")
        
        self.redb = MySqlDB(is_master=self.use_master)
        self.updb = MySqlDB(is_master=self.use_master)
        self.df = mydf.MyDf(is_master=self.use_master)

        self.codes = CodeManager(config)
        self.peaks = PeaksManager(config)

        if self.clf_name != "":
            self.clf_id = self.peaks.kmpeaks.getClfId(self.clf_name)
        else:
            self.clf_id = -1
        


    def getCodes(self, obsyear):
        return self.codes.getCodes(obsyear)

    def _setCodesFromDb(self, obsyear):
        self.codenames = self.getCodes(obsyear)
        self.peaks.codenames = self.codenames

    def registerCodes(self, obsyear):
        self.codes.register(obsyear)
        self._setCodesFromDb(obsyear)

    
    def registerPeaks(self, startep, endep, obsyear=0, with_prediction=False):
        if obsyear == 0:
            obsyear = lib.epoch2dt(endep).year
        if len(self.codenames) == 0:
            self._setCodesFromDb(obsyear)
        self.peaks.register(startep, endep, with_prediction=with_prediction)


    def execKmeans(self, startep, endep):
        obsyear = lib.epoch2dt(endep).year
        if len(self.codenames) == 0:
            self._setCodesFromDb()
        self.peaks.execKmeans(startep, endep)

    def loadKmModel(self):
        self.peaks.kmpeaks.loadKmModel()

    def predict(self, v):
        km_id, score, expected, cnt = self.peaks.predict(v)
        return km_id, score, expected, cnt

    def predictNext(self, ep, prices):
        return self.peaks.predictNext(ep, prices)

    def testKmeans(self, obsyear):
        if len(self.codenames) == 0:
            self._setCodesFromDb(obsyear)
        self.peaks.testKm()


def strStEd2ep(start, end):
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"
    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)
    return startep, endep

def feed(config, startep, endep, stage="all"):
    obsyear = lib.epoch2dt(endep).year
    a = ZzAnalyzer(config)
    if stage == "init" or stage == "all":
        a.registerCodes(obsyear)

    if stage == "register" or stage == "all":
        a.registerPeaks(startep, endep)

    if stage == "kmeans" or stage == "all":
        a.execKmeans(startep, endep)

    if stage == "kmtest" or stage == "all":
        a.testKmeans(obsyear)

def predict(config, startep, endep):
    a = ZzAnalyzer(config)
    obsyear = config["obsyear"]
    log("Registering items with prediction")
    a.loadKmModel()
    a.registerPeaks(startep, endep, obsyear=obsyear, with_prediction=True)
    log("end")

if __name__ == "__main__":
    import sys
    start = "2011-01-01"
    end = "2020-12-31"
    stage = "kmtest"

    (startep, endep) = strStEd2ep(start, end)
    config = {
        "name": "backtest",
        "classifier": "kmpeak_backtest",
        "obsyear": 2020,
        "granularity": "D",
        "data_dir": "app/stockAnalyzer/1",
        "type_name": CLF_TYPE_KM_PEAK,
        "km_granularity": "Y",
        "codenames": [],
        "valid_after_epoch": endep,
        "min_km_size": 100,
        "recreate": False
    }

    #feed(config, startep, endep, stage)

    start = "2021-01-04"
    end = "2022-12-31"
    (startep, endep) = strStEd2ep(start, end)
    predict(config, startep, endep)


# register code 6576.T
