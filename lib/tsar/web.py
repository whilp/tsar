import cli

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler
from tornado.wsgi import WSGIApplication

class DSNType(object):
    
    def __init__(self, host="localhost", port=0, db=0, sep=':'):
        self.host = host
        self.port = port
        self.db = db
        self.sep = sep

    def __str__(self):
        return "%s:%d/%d" % (self.host, self.port, self.db)

    def __call__(self, dsn):
        """Parse a DSN string.

        Return an object with host, port and db attributes.
        """
        host, junk, rest = dsn.partition(self.sep)
        port, junk, db = dsn.partition(self.sep)

        if host:
            self.host = host
        if port:
            self.port = int(port)
        if db:
            self.db = int(db)

        return self

class APIHandler(RequestHandler):
    pass

routes = [
]

application = WSGIApplication(routes)

@cli.DaemonizingApp(name="tsar-server")
def tsar_server(app):
    server = HTTPServer(application)
    server.listen(app.params.port)
    IOLoop.instance().start()

dsn = DSNType(port=6379)
httpport = 8000
tsar_server.add_param("-P", "--port", default=httpport, type=int,
    help="server port (default: %s)" % httpport)
tsar_server.add_param("-r", "--redis", default=dsn, type=dsn,
	help="Redis connection host:port/database (default: %s)" % dsn)

if __name__ == "__main__":
    tsar_server.run()
