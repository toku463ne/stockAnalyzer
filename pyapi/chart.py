import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import env
import lib
import data_getter
import json
import pyapi.chart_ele as ele

class Chart(object):
    def get_data(self,data):
        instrument = data["instrument"]
        granularity = data["granularity"]
        start = data["start_date"]
        end = data["end_date"]
        if len(start) == 10:
            start = start + "T00:00:00"
            end = end + "T00:00:00"
        dg = data_getter.getDataGetter(instrument, granularity)
        (ep, dt, o, h, l, c, v) = dg.getPrices(lib.str2epoch(start), lib.str2epoch(end))
        ep = ep.tolist()
        o = o.tolist()
        h = h.tolist()
        l = l.tolist()
        c = c.tolist()
        v = v.tolist()
        
        ohlcv = []
        for i in range(len(ep)):
            item = {}
            item["Date"] = ep[i]*1000
            item["Close"] = c[i]
            item["Open"] = o[i]
            item["Low"] = l[i]
            item["High"] = h[i]
            item["Volume"] = v[i]
            ohlcv.append(item)
        
        data["ohlcv"] = ohlcv
        
        if not "indicators" in data.keys():
            return json.dumps(data).replace("\r", "").replace("\n", "")

        for indicator_name in data["indicators"].keys():
            indicator = data["indicators"][indicator_name]
            ind_type = indicator["type"]
            if ind_type == "sma":
                values = ele.get_sma_chart_values(indicator_name, ep, c, int(indicator["span"]))
            if ind_type == "zigzag":
                values = ele.get_zigzag_chart_values(indicator_name, ep, 
                    dt, h, l, int(indicator["size"]))


            indicator["values"] = values    
            data["indicators"][indicator_name] = indicator

        #print(data)

        return json.dumps(data).replace("\r", "").replace("\n", "")


    def on_post(self, req, resp, **kwargs):
        args = req.media
        chartparams = json.loads(args["chartparams"])
        print(chartparams)
        data = self.get_data(chartparams)

        html = ""
        with open("%s/pyapi/templates/am_ohlcv.html" % env.BASE_DIR, "r") as f:
            html = f.read()
        #with open("%s/charts/samples/candlechart.html" % env.BASE_DIR, "r") as f:
        #    html = f.read()

        resp.content_type = 'text/html'
        html = html.replace("#AM_OHLCV_DATA#", data)
        html = html.replace("#AM_OHLCV_INSTRUMENT#", chartparams["instrument"])
        resp.body = html
