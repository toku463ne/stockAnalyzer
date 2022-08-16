import json
import falcon

class AppResource(object):

    def on_get(self, req, resp):

        msg = {
            "message": "Welcome to the Falcon %s" % req.query_string
        }
        resp.body = json.dumps(msg)

    # curl -i -X POST -d 'a=1' http://localhost:8000
    def on_post(self, req, resp, **kwargs):
        result = req.media
        print("media is %s" % result)
        # do your job
        resp.body = json.dumps(result)


app = falcon.API()
app.add_route("/", AppResource())


if __name__ == "__main__":
    from wsgiref import simple_server
    httpd = simple_server.make_server("127.0.0.1", 8000, app)
    httpd.serve_forever()
