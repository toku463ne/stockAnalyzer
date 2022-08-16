import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import json
import falcon
import data_getter
import lib
import pandas as pd
import env

class DataGetterApi(object):
    def get_price_data(self, kwargs):
        instrument = kwargs["instrument"]
        granularity = kwargs["granularity"]
        start = kwargs["start"]
        end = kwargs["end"]
        dg = data_getter.getDataGetter(instrument, granularity)
        (ep, dt, o, h, l, c, v) = dg.getPrices(lib.str2epoch(start), lib.str2epoch(end))
        data = []
        for i in range(len(ep)):
            item = {}
            item["date"] = lib.dt2str(pd.to_datetime(dt[i]))
            item["value"] = str(c[i])
            item["open"] = str(o[i])
            item["low"] = str(l[i])
            item["high"] = str(h[i])
            item["volume"] = str(v[i])
            data.append(item)
        return data


    def on_post(self, req, resp, **kwargs):
        args = req.media
        print(args)
        data = self.get_price_data(args)
        resp.body = json.dumps(data)

    def on_get(self, req, resp):
        args = req.params
        print(args)
        data = self.get_price_data(args)
        resp.body = json.dumps(data)

# curl -i -X POST -d "instrument=MSFT&granularity=D&start=2021-11-01T00:00:00&end=2021-12-01T00:00:00" http://localhost:9000/data
# curl "http://localhost:9000/prices.json?instrument=MSFT&granularity=D&start=2021-11-01T00:00:00&end=2021-12-01T00:00:00"
def start_app():
    app = falcon.API()
    app.add_route("/prices.json", DataGetterApi())
    from wsgiref import simple_server
    httpd = simple_server.make_server(env.conf["pyapi"]["host"], 
    env.conf["pyapi"]["port"], app)
    httpd.serve_forever()

if __name__ == "__main__":
    start_app()