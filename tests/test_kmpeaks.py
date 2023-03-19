import __init__
import unittest
from datetime import datetime
from db.mysql import MySqlDB
import pandas as pd

import analyzer.zz_analyzer as zz
import lib

def strStEd2ep(start, end):
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"
    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)
    return startep, endep

start = "2019-01-01"
end = "2020-12-31"
(startep, endep) = strStEd2ep(start, end)

config = {
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": [],
    "startep": startep,
    "endep": endep,
    "use_master": False
}


class TestKmPeaks(unittest.TestCase):
    def test_kmeans(self):
        from classifier.kmpeaks import KmPeaksClassifier
        a = KmPeaksClassifier(config)
        



if __name__ == "__main__":
    unittest.main()
