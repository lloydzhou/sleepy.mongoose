from handlers import MongorestHandler
import tornado.httpserver
import tornado.ioloop
from tornado.options import define, options
from pymongo import Connection
from pymongo.errors import ConnectionFailure, ConfigurationError


define("port", default=8888, help="run on the given port", type=int)
define("xorigin", default='*', help="xorigin", type=basestring)
define("mongos", default='localhost', help="mongos", type=basestring)


class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/([a-z0-9_]+)/([a-z0-9_]+)", MongorestHandler, dict(xorigin=options.xorigin))
        ]
        settings = dict()
        tornado.web.Application.__init__(self, handlers, **settings)

        # Have one global connection to the blog DB across all handlers
        self.connections = {}
        self.cursors = {}
        self._cursor_id = 0

        for host in options.mongos:
            if len(options.mongos) == 1:
                name = "default"
            else:
                name = host.replace(".", "")
                name = name.replace(":", "")

            self._get_connection(name = name, uri=host)

    def _get_connection(self, name = None, uri='mongodb://localhost:27017'):
        if name == None:
            name = "default"

        if name in self.connections:
            return self.connections[name]

        try:
            connection = Connection(uri, network_timeout = 2)
        except (ConnectionFailure, ConfigurationError):
            return None

        self.connections[name] = connection
        return connection


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()

