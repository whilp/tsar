"""\
%PROJECT%
"""

__project__ = "tsar"
__version__ = "0.1"
__package__ = "tsar"
__description__ = "time series archival and retrieval"
__author__ = "Will Maier"
__author_email__ = "wcmaier@hep.wisc.edu"
__url__ = "http://code.hep.wisc.edu/tsar"

# See http://pypi.python.org/pypi?%3Aaction=list_classifiers.
__classifiers__ = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Science/Research",
    "Intended Audience :: System Administrators",
    "License :: OSI Approved :: BSD License",
    "Natural Language :: English",
    "Programming Language :: Python :: 2.5",
    "Programming Language :: Python :: 2.6",
    "Topic :: Database :: Front-Ends",
    "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    "Topic :: System :: Monitoring",
    ] 
__keywords__ = "time series monitor"

__requires__ = [
    "pyCLI>=1.0",
    "tornado>=0.2",
]

# The following is modeled after the ISC license.
__copyright__ = """\
Copyright (c) 2010 Will Maier <wcmaier@hep.wisc.edu>

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""

__todo__ = """\
"""

import csv
import logging
import string

import cli

from redis import Redis
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application, HTTPError, RequestHandler
from tornado.wsgi import WSGIApplication

class DictReader(csv.DictReader):
    """Produce keys that are strings even if input is unicode.

    This hack makes it possible to pass the resulting dictionaries as
    kwargs to functions.
    """
    keytype = str
    
    def next(self):
        d = csv.DictReader.next(self)
        newd = {}
        for k in d:
            newd[self.keytype(k)] = d[k]

        return newd

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
    fieldchars = [x for x in string.letters if x not in "!/"]
    fieldlen = 32

    def __init__(self, application, request, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)
        super(APIHandler, self).__init__(application, request, **kwargs)

        self.redis = Redis(
            host=application.settings["redis.host"],
            port=application.settings["redis.port"],
            db=application.settings["redis.db"])

    @staticmethod
    def db_string(handler, field):
        if len(field) > handler.fieldlen:
            raise TypeError("field too long")

        badchars = [x for x in field if x not in handler.fieldchars]
        if badchars:
            raise TypeError("field contains reserved characters")

        return field

    @staticmethod
    def db_int(handler, field):
        return int(field)

class ObservationsHandler(APIHandler):
        
    fields = {
        "time": APIHandler.db_int,
        "value": APIHandler.db_int,
        "subject": APIHandler.db_string,
        "attribute": APIHandler.db_string,
    }

    def post(self):
        """Create a new observation."""
        contenttype = self.request.headers.get("Content-Type", None)
        observations = []
        if contenttype.startswith(u"multipart/form-data"):
            # Bulk update.
            _file = self.request.files.get("observations", [{}])[0]
            body = (l for l in _file.get("body", "").splitlines())
            filetype = _file.get("content_type", None)
            if filetype == u"text/csv":
                observations = DictReader(body)
        elif contenttype.startswith(u"application/x-www-form-urlencoded"):
            # Single update.
            observations = [dict((k, self.get_argument(k)) for k in self.fields)]

        created = False
        for observation in observations:
            self.record(**observation)

            created = True

        if created:
            self.set_status(201)

    def record(self, **kwargs):
        """Record an observation."""
        missing_fields = [k for k in self.fields if k not in kwargs]
        if missing_fields:
            raise HTTPError(400, "missing fields: %s" % ", ".join(missing_fields))

        extra_fields = [k for k in kwargs if k not in self.fields]
        if extra_fields:
            raise HTTPError(400, "extra fields: %s" % ", ".join(extra_fields))

        for k in kwargs:
            try:
                kwargs[k] = self.fields[k](self, kwargs[k])
            except TypeError, e:
                raise HTTPError(400, "%s: %s" % (e.args[0], k))

        # In Redis, we save each observation as in a sorted set with the
        # time of the observation as its score. This allows us to easily
        # pull observations in a range from history.

        # Since value is not likely to be unique across the observation
        # period, we make a unique member by prepending the time of
        # observation to the value. Consumers must reverse this process
        # to get at the actual data (ie, value.split(':')).

        key = "observations!%(subject)s!%(attribute)s" % kwargs
        uniqueval = "%(time)d:%(value)d" % kwargs
        self.redis.zadd(key, uniqueval, kwargs["time"])
        self.log.debug("Recording %(subject)s's %(attribute)s "
            "(%(value)d) at %(time)d", kwargs)

routes = [
    (r"/observations", ObservationsHandler),
]
application = WSGIApplication(routes)

@cli.DaemonizingApp
def tsar(app):
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
tsar.add_param("-P", "--port", default=httpport, type=int,
    help="server port (default: %s)" % httpport)
tsar.add_param("-r", "--redis", default=dsn, type=dsn,
    help="Redis connection host:port/database (default: %s)" % dsn)

if __name__ == "__main__":
    tsar.run()
