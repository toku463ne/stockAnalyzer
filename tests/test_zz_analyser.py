import __init__
import unittest
from datetime import datetime
from db.mysql import MySqlDB
import pandas as pd

import analyzer.zz.zz_analyzer as zz
import lib
from consts import *

start = "2019-01-01"
end = "2020-12-31"

config = {
    "name": "analtest",
    "classifier": "kmtest",
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": [],
    "use_master": False,
    "type_name": CLF_TYPE_KM_PEAK,
    "min_km_size": 5,
    "recreate": True
}

class TestZzAnalyzer(unittest.TestCase):
    def test_feed(self):
        db = MySqlDB()
        stage = "all"
        
        config["codenames"] = ["1301.T", "1928.T"]
        (startep, endep) = zz.strStEd2ep("2018-01-04", "2021-12-31")    
        config["valid_after_epoch"] = endep
        zz.feed(config, startep, endep, stage=stage)

        (startep, endep) = zz.strStEd2ep("2021-12-31", "2022-12-31")
        config["recreate"] = False    
        zz.predict(config, startep, endep)


if __name__ == "__main__":
    unittest.main()
