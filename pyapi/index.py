import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import env

class Index(object):
    def on_get(self, req, resp):
        default_text = ""
        with open("%s/default_chart.json" % (env.BASE_DIR), "r") as f:
            default_text = f.read()
        html = ""
        with open("%s/pyapi/templates/index.html" % env.BASE_DIR, "r") as f:
            html = f.read()
        
        resp.content_type = 'text/html'
        html = html.replace("#CHART_DEFAULT_JSON#", default_text)
        #html = html.replace("#CHART_ENDPOINT#", self.action)
        resp.body = html
