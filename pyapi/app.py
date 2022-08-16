import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pyapi.index
import pyapi.chart
import falcon
import env

app = falcon.API()
app.add_route("/index", pyapi.index.Index())
app.add_route("/chart", pyapi.chart.Chart())
from wsgiref import simple_server
httpd = simple_server.make_server(env.conf["pyapi"]["host"], 
                                    env.conf["pyapi"]["port"], app)
httpd.serve_forever()