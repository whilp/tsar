"""\
tsar is a simple HTTP service built on the `Tornado`_ web framework
that performs efficient archival and retrieval of time-series
data. It uses the `Redis`_ key-value store to provide fast access
to stored records. New records can be sent via HTTP POST singly
(application/x-www-form-urlencoded) or in bulk (text/csv via
multipart/form-data). Query results requested via HTTP GET are returned
as application/json.

.. _Tornado:    http://www.tornadoweb.org/
.. _Redis:      http://www.redis-db.com/
"""

__project__ = "tsar"
__version__ = "0.1"
__pkg__ = "tsar"
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
    "redis",
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

from calendar import timegm
from datetime import datetime, timedelta
from time import gmtime

import cli

from redis import Redis
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application, HTTPError, RequestHandler, StaticFileHandler
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
    fieldchars = [x for x in string.letters + string.punctuation if x not in "!/"]
    fieldlen = 128

    def __init__(self, application, request, **kwargs):
        super(APIHandler, self).__init__(application, request, **kwargs)

        self.redis = Redis(
            host=application.settings["redis.host"],
            port=application.settings["redis.port"],
            db=application.settings["redis.db"])

    @classmethod
    def db_string(handler, field):
        if len(field) > handler.fieldlen:
            raise TypeError("field too long")

        badchars = [x for x in field if x not in handler.fieldchars]
        if badchars:
            raise TypeError("field contains reserved characters")

        return field

    @classmethod
    def db_int(handler, field):
        return int(field)

    @classmethod
    def db_reltime(handler, field, now=None):
        field = int(field)
        if field < 0:
            if now is None:
                now = datetime.now()
            field = now - timedelta(seconds=field)
        else:
            field = datetime(*gmtime(field)[:6])

        return timegm(field.timetuple())

    def validate(self, fields, **kwargs):
        for k in kwargs:
            try:
                kwargs[k] = fields[k](kwargs[k])
            except TypeError, e:
                raise HTTPError(400, "%s: %s" % (e.args[0], k))

        return kwargs

    def encodeval(self, time, value, sep=':'):
        return "%s%s%s" % (time, sep, value)

    def decodeval(self, value, sep=':'):
        time, junk, value = value.partition(sep)
        return int(time), int(value)

class ObservationsHandler(APIHandler):

    def get(self):
        fields = {
            "start": self.db_reltime,
            "stop": self.db_reltime,
            "subject": self.db_string,
            "attribute": self.db_string,
        }
        kwargs = dict((k, self.get_argument(k)) for k in fields)
        kwargs = self.validate(fields, **kwargs)

        if kwargs["start"] > kwargs["stop"]:
            raise HTTPError(400, "start must be less than stop")

        key = "observations!%(subject)s!%(attribute)s" % kwargs
        results = self.redis.zrange(key, kwargs["start"], kwargs["stop"])

        # Since the members of the sorted timeseries set have the
        # timestamp prepended, we don't need to request scores as well.
        # Instead, we simply decode the members themselves.
        results = [self.decodeval(x) for x in results]

        # JavaScript expects millisecond precision.
        jsresults = [(t * 1000, v) for t, v in results]
        kwargs["len"] = len(results)
        logging.debug("Serving %(len)d results for %(subject)s's "
            "%(attribute)s from %(start)d to %(stop)d", kwargs)

        self.write({"results": jsresults})

    def post(self):
        """Create a new observation."""
        fields = {
            "time": self.db_int,
            "value": self.db_int,
            "subject": self.db_string,
            "attribute": self.db_string,
        }

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
            observations = [dict((k, self.get_argument(k)) for k in fields)]

        created = False
        for observation in observations:
            self.record(fields, **observation)

            created = True

        if created:
            self.set_status(201)

    def record(self, fields, **kwargs):
        """Record an observation."""
        missing_fields = [k for k in fields if k not in kwargs]
        if missing_fields:
            raise HTTPError(400, "missing fields: %s" % ", ".join(missing_fields))

        extra_fields = [k for k in kwargs if k not in fields]
        if extra_fields:
            raise HTTPError(400, "extra fields: %s" % ", ".join(extra_fields))

        kwargs = self.validate(fields, **kwargs)

        # In Redis, we save each observation as in a sorted set with the
        # time of the observation as its score. This allows us to easily
        # pull observations in a range from history.

        # Since value is not likely to be unique across the observation
        # period, we make a unique member by prepending the time of
        # observation to the value. Consumers must reverse this process
        # to get at the actual data (ie, value.split(':')).

        key = "observations!%(subject)s!%(attribute)s" % kwargs
        uniqueval = self.encodeval(kwargs["time"], kwargs["value"])
        self.redis.zadd(key, uniqueval, kwargs["time"])
        logging.debug("Recording %(subject)s's %(attribute)s "
            "(%(value)d) at %(time)d", kwargs)

class InterfaceHandler(StaticFileHandler):

    def __init__(self, application, request):
        root = application.settings["interface.root"]
        super(InterfaceHandler, self).__init__(application, request, root)

    def get(self, path, include_body=True):
        try:
            return super(InterfaceHandler, self).get(path, include_body)
        except HTTPError, e:
            # Append the html suffix if we can't find the file.
            if e.status_code != 404:
                raise
            return super(InterfaceHandler, self).get(path + ".html", include_body)

routes = [
    (r"/observations", ObservationsHandler),
    (r"/(.*)", InterfaceHandler),
]
application = WSGIApplication(routes)

msgfmt = "%(asctime)s\t%(message)s"
datefmt = "%Y.%m.%dT%H:%M:%S-%Z"
@cli.DaemonizingApp(message_format=msgfmt, date_format=datefmt)
def tsar(app):
    settings = {
        "redis.port": app.params.redis.port,
        "redis.host": app.params.redis.host,
        "redis.db": app.params.redis.db,
        "interface.root": app.params.interface,
    }
    application = Application(routes, **settings)

    if app.params.daemonize:
        app.log.debug("Daemonizing")
        app.daemonize()

    app.log.debug("Listening on port %d" % app.params.port)
    server = HTTPServer(application)
    server.listen(app.params.port)
    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        return 0

dsn = DSNType(port=6379)
httpport = 8000
interface = "/var/www/htdocs/"
tsar.add_param("-P", "--port", default=httpport, type=int,
    help="server port (default: %s)" % httpport)
tsar.add_param("-r", "--redis", default=dsn, type=dsn,
    help="Redis connection host:port/database (default: %s)" % dsn)
tsar.add_param("-i", "--interface", default=interface,
    help="directory from which to serve the web interface files "
        "(default: %s)" % interface)

if __name__ == "__main__":
    tsar.run()
