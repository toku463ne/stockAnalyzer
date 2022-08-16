import time
from data_getter import DataGetter
import lib.tradelib as tradelib
import yfinance as yf
import env

from datetime import datetime

class YFinanceGetter(DataGetter):
    def __init__(self, codename, granularity):
        self.name = "yfinance_%s_%s" % (codename, granularity)
        self.codename = codename
        self.granularity = granularity
        self.unitsecs = tradelib.getUnitSecs(granularity)
        self.data_get_interval = env.conf["data_download_interval"]

        if self.granularity == "":
            pass

        self.ticker = yf.Ticker(self.codename)
        self.interval = self._convGranularity2Interval(self.granularity)

    def _convGranularity2Interval(self, granularity):
        t = granularity[:1]
        i = granularity[1:]

        if i == "":
            i = "1"
        
        # interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        interval = ""
        if t.upper() == "M":
            interval = "%s%s" % (i, "m")
        elif t.upper() == "H": 
            interval = "%s%s" % (i, "h")
        elif t.upper() == "D":
            interval = "%s%s" % (i, "d")
        elif t.upper() == "W": 
            interval = "%s%s" % (i, "wk")
        if interval == "": raise Exception("Not proper granularity type")

        return interval


    def getPrices(self, startep, endep, waitDownload=True):
        if waitDownload:
            time.sleep(self.data_get_interval)
        start = datetime.fromtimestamp(startep)
        end = datetime.fromtimestamp(endep)
        df = self.ticker.history(interval=self.interval, start=start, end=end)
        df["EP"] =  [x.timestamp() for x in df.index] 
        df["DT"] =  df.index
        df = df.rename(columns={"Open": "O", "High": "H", "Low": "L", 
                                "Close": "C", 
                                "Volume": "V"})[['EP', 'DT', 'O', 'H', 'L', 'C', 'V']]
        return df