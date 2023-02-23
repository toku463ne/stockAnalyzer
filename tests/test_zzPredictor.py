import __init__
import unittest
from predictor.zzPredictor import ZzPredictor
import lib

from datetime import datetime

config = {
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": []
}

class TestPredictor(unittest.TestCase):
    """
{
    "codename": "4587.T",
    "granularity": "D",
    "start_date": "2021-04-01",
    "end_date": "2021-08-30",
    "indicators": {
        "zigzag5": {
            "size": 5,
            "type": "zigzag"
        }
    }
}
    """
    def test_search(self):
        config["km_setid"] = "km20100101000020201231000096964fd8ed"
        #config["codenames"] = ["4587.T", "1712.T", "2134.T"]
        config["codenames"] = ["4587.T"]
    
        p = ZzPredictor(config=config)

        epoch = lib.dt2epoch(datetime(2021, 7, 13))
        tickers = p.getTickers(epoch)
        df = p.search(tickers, epoch)
        print(df)

if __name__ == "__main__":
    unittest.main()

