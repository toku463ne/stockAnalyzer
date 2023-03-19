import __init__
import unittest
import analyzer.zz.zz_analyzer as zz
from predictor.zzPredictor import ZzPredictor
import lib

from datetime import datetime


start = "2019-01-01"
end = "2020-12-31"
(startep, endep) = zz.strStEd2ep(start, end)
config = {
    "name": "analtest",
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": [],
    "startep": startep,
    "endep": endep,
    "use_master": False,
    "valid_after_epoch": endep,
    "recreate": False
}

class TestPredictor(unittest.TestCase):

    def test_search(self):
        config["clf_name"] = "backtest"
        #config["codenames"] = ["4587.T", "1712.T", "2134.T"]
        config["codenames"] = ["4587.T"]
        

    
        p = ZzPredictor(config=config)

        epoch = lib.dt2epoch(datetime(2021, 7, 13))
        tickers = p.getTickers(epoch)
        df = p.search(tickers, epoch)
        print(df)

if __name__ == "__main__":
    unittest.main()

