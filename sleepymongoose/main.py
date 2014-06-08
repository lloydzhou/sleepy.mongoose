from handlers import MongoHandler
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)
define("xorigin", default='*', help="xorigin", type=basestring)
define("mongos", default='localhost', help="mongos", type=basestring)

class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, mh, xorigin):
        self.mh = mh
        self.xorigin=xorigin
    def prependJSONPCallback(self, str):
        jsonp_output = '%s(' % self.jsonp_callback + str + ')'
        self.write( jsonp_output )


class MainHandler(BaseHandler):
    def prepare(self):
        self.set_header('Content-type', 'application/json')

    def get(self, db, collection, cmd):
        name = self.get_argument('name', None)
        func = getattr(self.mh, cmd, None)
        if callable(func):
            func(self.request.arguments, self.write, name = name, db = db, collection = collection)

    def post(self, db, collection, cmd):
        name = self.get_argument('name', None)
        func = getattr(self.mh, cmd, None)
        if callable(func):
            func(self.request.arguments, self.write, name = name, db = db, collection = collection)

def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/([a-z0-9_]+)/([a-z0-9_]+)/([a-z_]+)", MainHandler, dict(mh=MongoHandler(options.mongos.split(',')), xorigin=options.xorigin)),
    ])
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()

