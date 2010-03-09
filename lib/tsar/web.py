import logging

import cli

from redis import Redis
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
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

    def __init__(self, application, request, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)
        super(APIHandler, self).__init__(self, application, request, **kwargs)

        self.redis = Redis(
            host=application.settings["redis.host"],
            port=application.settings["redis.port"],
            db=application.settings["redis.db"])

    def get_argument(self, name, type=str, **kwargs):
        arg = super(APIHandler, self).get_argument(name, **kwargs)
        try:
            arg = type(arg)
        except:
            raise HTTPError(404, "bad argument %s" % name)

        return arg

class ObservationsHandler(APIHandler):

    def post(self):
        """Create a new observation."""
        time = self.get_argument("time", type=int)
        value = self.get_argument("value", type=int)
        subject = self.get_argument("subject")
        attribute = self.get_argument("attribute")

        self.log.debug("Recording %s's %s (%d) at %d",
            subject, attribute, value, time)
        self.record(time, subject, attribute, value)

    def record(self, time, subject, attribute, value):
        """Record an observation."""

        # In Redis, we save each observation as in a sorted set with the
        # time of the observation as its score. This allows us to easily
        # pull observations in a range from history.

        # Since value is not likely to be unique across the observation
        # period, we make a unique member by prepending the time of
        # observation to the value. Consumers must reverse this process
        # to get at the actual data (ie, value.split(':')).

        key = "observations!%s!%s" % (subject, attribute)
        uniqueval = "%d:%d" % (time, value)
        self.redis.zadd(key, uniqueval, time)

routes = [
    (r"/observations", ObservationsHandler),
]
application = WSGIApplication(routes)

@cli.DaemonizingApp(name="tsar-server")
def tsar_server(app):
    settings = {
        "redis.port": app.params.redis.port,
        "redis.host": app.params.redis.host,
        "redis.db": app.params.redis.db,
    }
    application = Application(routes, **settings)

    app.log.debug("Listening on port %d" % app.params.port)

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
