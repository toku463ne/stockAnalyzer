import __init__
import unittest
import analyzer.zz_analyzer as zz
from datetime import datetime
from db.mysql import MySqlDB

"""
{
    "codename": "1928.T",
    "granularity": "D",
    "start_date": "2022-08-01",
    "end_date": "2022-12-01",
    "indicators": {
        "zigzag5": {
            "size": 5,
            "type": "zigzag"
        }
    }
}
"""

config = {
    "granularity": "D",
    "n_points": 5,
    "zz_size": 5,
    "km_avg_size": 5,
    "data_dir": "app/stockAnalyzer/test",
    "km_granularity": "Y",
    "codenames": []
}

class TestZzAnalyzer(unittest.TestCase):
    def test_x6_less_than_1_case(self):
        config = {
            "granularity": "D",
            "n_points": 5,
            "zz_size": 5,
            "km_avg_size": 5,
            "data_dir": "app/stockAnalyzer/test",
            "km_granularity": "Y",
            "codenames": ["1928.T"]
        }
        (startep, endep) = zz.strStEd2ep("2022-01-04", "2022-06-08")
        a = zz.ZzAnalyzer(startep, endep, config=config, use_master=False)

if __name__ == "__main__":
    unittest.main()
