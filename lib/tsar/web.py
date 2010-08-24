from __future__ import with_statement

import csv
import logging
import os
import socket

try:
    import mimetypes
except ImportError:
    mimetypes = None

from functools import partial
from urllib2 import quote, unquote

from neat import neat

from . import errors, model
from .commands import DBMixin, DaemonizingSubCommand
from .commands import (
    Application, CommandLineMixin, DaemonizingMixin, LoggingMixin)
from .util import Decorator, adiff, derive, differentiators, json, parsedsn, trim

__all__ = ["Records", "service"]

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "records": v + ".records.v2",
}

class AllRecords(neat.Resource):
    prefix = "/records"
    media = {
        mediatypes["records"] + "+json": "json",
        "application/json": "json",
        mediatypes["records"] + "+csv": "csv",
        "text/csv": "csv",
    }
    extensions = {
        ".json": "application/json",
        ".csv": "text/csv",
    }
    params = {
        "method": "_method",
        "accept": "_accept",
        "content-type": "_content",
    }
    filters = {
        "skipnull": lambda r: (x for x in r if x[1] is not None),
        "derive": derive,
    }
    for name, ds in differentiators.items():
        for n, fxn in ds.items():
            filters["%s-%d" % (name, n)] = partial(fxn, points=n, fxns=ds)

    def decodeid(self, id):
        return tuple(unquote(id.encode("utf8")).split('/'))

    def encodeid(self, subject, attribute=None, cf=None):
        if attribute is None and cf is None:
            records = subject
            attribute = records.attribute
            cf = records.cf
            subject = records.subject
        return quote(u'/'.join((subject, attribute, cf)))

    def extend(self, records, data):
        try:
            records.extend(data)
        except ValueError:
            raise errors.HTTPBadRequest("Bad data")
        except errors.RecordError, e:
            raise errors.HTTPConflict(e.args[0])

    def post(self):
        content = getattr(self.req, "content", None)
        if not content:
            raise errors.HTTPBadRequest("No data")

        for key, data in content.items():
            try:
                subject, attribute, cf = key
                records = model.Records(subject, attribute, cf, 
                    exception=errors.HTTPBadRequest)
            except (TypeError, ValueError):
                raise errors.HTTPBadRequest("Invalid resource id")
            self.extend(records, data)

        raise errors.HTTPNoContent("Records created")

    def _get(self, **base):
        filters = [f for f in self.req.GET.pop("filters", "").split(',') if f]
        queries = []
        query = base.copy()
        for k, v in self.req.params.items():
            if k in ("callback",):
                continue
            elif k == "subject":
                if query:
                    queries.append(query)
                    query = base.copy()
                query = {k: v}
            elif not query:
                raise errors.HTTPBadRequest("%r parameter must follow subject" % k)
            else:
                query[k] = v
        if query:
            queries.append(query)

        data = {}
        for query in queries:
            subject, attribute, cf = \
                [query.pop(x, None) for x in "subject attribute cf".split()]
            records = model.Records(subject, attribute, cf, 
                exception=errors.HTTPBadRequest)
            records.types.now = query.pop("now", None)
            result = records.query(**query)
            if filters:
                try:
                    result = self.filter(result, filters)
                except KeyError, e:
                    raise errors.HTTPBadRequest("Invalid filter: %r" % e.args[0])
            data[self.encodeid(records)] = list(result)

        return data

    def filter(self, result, filters):
        for filter in (self.filters[f] for f in filters):
            result = filter(result)

        return result

    def get_json(self):
        data = self._get()

        self.response.status_int = 200
        self.response.body = json.dumps(data)
        callback = self.req.params.get("callback", None)
        if callback is not None:
            self.response.body = "%s(%s)" % (callback, self.response.body)

    def handle_json(self):
        if not self.req.body:
            return
        try:
            data = json.loads(self.req.body)
        except ValueError:
            raise errors.HTTPBadRequest("Could not parse JSON")

        for k in data.keys():
            key = self.decodeid(k)
            data[key] = data.pop(k)

        return data

    def handle_csv(self):
        data = {}
        reader = csv.reader(iter(self.req.body.splitlines()))
        for row in reader:
            if not row:
                continue
            if row == ["subject", "attribute", "cf", "timestamp", "value"]:
                continue
            try:
                subject, attribute, cf, timestamp, value = row
            except ValueError:
                raise errors.HTTPBadRequest("Too few fields in CSV: %r" % 
                    ','.join(row))
            key = (subject, attribute, cf)
            data.setdefault(key, [])
            data[key].append((timestamp, value))

        return data

class Records(AllRecords):
    prefix = "/records/"

    def _get(self):
        path = self.req.path_info.lstrip('/')
        keys = "subject attribute cf".split()
        try:
            query = dict(zip(keys, self.decodeid(path)))
        except ValueError:
            raise errors.HTTPNotFound
        return super(Records, self)._get(**query)

    def get_json(self):
        data = self._get()

        self.response.status_int = 200
        self.response.body = json.dumps(data)
        callback = self.req.params.get("callback", None)
        if callback is not None:
            self.response.body = "%s(%s)" % (callback, self.response.body)

    def get_csv(self):
        writer = csv.writer(self.response.body_file)
        writer.writerow("subject attribute cf timestamp value".split())
        for k, v in self._get().items():
            s, a, c = self.decodeid(k)
            for t, v in v:
                writer.writerow((s, a, c, t, v))

        self.response.status_int = 200

class Ping(neat.Resource):
    prefix = "/_ping"

    def get(self):
        self.response.body = "OK"
        self.response.status_int = 200
        self.response.content_type = "text/plain"

class Static(neat.Resource):
    prefix = "/"
    public = ""
    index = "index.html"

    def get(self):
        prefix = self.prefix.rstrip("/") + "/"
        fname = self.req.path
        if fname.endswith("/"):
            fname += self.index
        fname = trim(fname, self.prefix)
        fullpath = os.path.abspath(os.path.join(self.public, fname))
        if not fullpath.startswith(self.public):
            raise errors.HTTPNotFound()

        content, body = self.read(fullpath)
        self.read(fullpath)
        self.response.content_type = content
        self.response.body = body or ""

    def read(self, fname, tryindex=True):
        content = "text/plain"
        if mimetypes is not None:
            guess = mimetypes.guess_type(fname)
            if guess[0] is not None:
                content = guess[0]

        body = None
        try:
            with open(fname, 'r') as f:
                body = f.read()
        except (OSError, IOError), e:
            if isinstance(e, IOError):
                if e.errno == 13:
                    raise errors.HTTPForbidden()
                elif e.errno == 21:
                    return self.read(fname + "/" + self.index, tryindex=False)
            raise errors.HTTPNotFound()

        return content, body

class Dispatch(neat.Dispatch):
    backend = ""

    @neat.wsgify
    def __call__(self, req):
        response = super(Dispatch, self).__call__(req)
        if self.backend:
            response.headers["X-Tsar-Backend"] = self.backend
        return response

def Server(host, port, backend="", public=False, **kwargs):
    from .ext import wsgiserver

    resources = [
        AllRecords(),
        Records(),
        Ping(),
    ]
    if public is not False:
        static = Static()
        static.public = public
        resources.append(static)
    service = Dispatch(*resources)
    service.backend = socket.gethostname()
    if backend:
        service.backend = "%s/%s" % (service.backend, backend)
    server = wsgiserver.CherryPyWSGIServer((host, port), service, **kwargs)
    return server

class Serve(DBMixin, DaemonizingSubCommand):
    
    def main(self):
        host, _, port = self.params.server.partition(':')
        if not port:
            port = 8000

        public = self.params.public
        if public is False or not os.path.isdir(public):
            public = False
        else:
            public = os.path.abspath(public)

        self.log.info("Starting server at http://%s:%s/", host, port)
        server = Server(host, int(port),
            public=public,
            backend=self.params.backend,
            numthreads=int(self.params.nthreads),
            request_queue_size=int(self.params.requests),
            timeout=int(self.params.timeout))
        if self.params.daemonize:
            self.daemonize()
        try:
            server.start()
        except KeyboardInterrupt:
            server.stop()

    def pre_run(self):
        DaemonizingSubCommand.pre_run(self)
        DBMixin.pre_run(self)

    def setup(self):
        Application.setup(self)
        CommandLineMixin.setup(self)
        LoggingMixin.setup(self)
        self.argparser = self.parent.subparsers.add_parser("serve", 
            help="start the tsar web service")
        DaemonizingMixin.setup(self)
        DBMixin.setup(self)

        default_server = "0.0.0.0:8000"
        default_threads = 10
        default_requests = 5
        default_timeout = 10
        default_backend = ""
        self.add_param("-n", "--nthreads", default=default_threads,
            help="minimum size of server thread pool (default: %s)" % default_threads)
        self.add_param("-r", "--requests", default=default_requests,
            help="size of the server request queue (default: %s)" % default_requests)
        self.add_param("-t", "--timeout", default=default_timeout,
            help="timeout (seconds) for accepted connections (default: %s)" % default_timeout)
        self.add_param("-b", "--backend", default=default_backend,
            help="set backend tag (default: %s)" % default_backend)
        self.add_param("-P", "--public", default=False,
            help="specify public directory for static files (default: no static files)")
        self.add_param("server", nargs="?",
            help="<host>:<port> (default: %s)" % default_server, default=default_server)
