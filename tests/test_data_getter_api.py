import __init__
import unittest
import pyapi.data_getter_api as api
from datetime import datetime
import lib
import json

class TestDataGetterApi(unittest.TestCase):
    def test_get_data(self):
        dgapi = api.DataGetterApi()
        
        start = "2021-11-01T00:00:00"
        end = "2021-12-01T00:00:00"
        data = dgapi.get_price_data({"codename": "MSFT", 
            "granularity": "D", "start": start, "end": end})

        self.assertEqual(lib.str2dt(data[0]["date"]).day, 1)
        self.assertEqual(lib.str2dt(data[-2]["date"]).day, 30)
        
        # test if it is json serializable
        json.dumps(data)

if __name__ == "__main__":
    unittest.main()
