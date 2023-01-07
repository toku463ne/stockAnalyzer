import __init__
import unittest
import pyapi.data_getter_api as api
from datetime import datetime
import lib
import json

class TestDataGetterApi(unittest.TestCase):
    def test_get_data(self):
        dgapi = api.DataGetterApi()
        
        start = "2021-11-01T00:00:00+0900"
        end = "2021-11-30T00:00:00+0900"
        datetime_format = '%Y-%m-%dT%H:%M:%S%z'
        data = dgapi.get_price_data({"codename": "^N225", 
            "granularity": "D", "start": start, "end": end, 
            "waitDownload": False, "datetime_format": datetime_format})
        self.assertEqual(lib.str2dt(data[0]["date"]).day, 1)
        self.assertEqual(lib.str2dt(data[-1]["date"]).day, 30)
        
        # test if it is json serializable
        json.dumps(data)

if __name__ == "__main__":
    unittest.main()
