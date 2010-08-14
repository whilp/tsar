import csv
import logging

from urllib2 import quote, unquote

from neat import neat

from . import errors, model
from .commands import DBMixin, DaemonizingSubCommand
from .commands import (
    Application, CommandLineMixin, DaemonizingMixin, LoggingMixin)
from .util import Decorator, json, parsedsn

__all__ = ["Records", "service"]

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "records": v + ".records.v2",
}

class getmethod(Decorator):
    
    def call(self, func, args, kwargs):
        log = logger(self)
        instance = args[0]
        req = instance.req

        path = req.path_info.lstrip('/')
        try:
            rid = instance.decodeid(path)
        except ValueError:
            raise errors.HTTPNotFound

        try:
            records = model.Records(*rid, exception=errors.HTTPBadRequest)
        except TypeError:
            raise errors.HTTPBadRequest("Invalid resource id")

        start, stop, now, interval = 0, -1, None, None
        if req.content is not None:
            now = req.content.get("now", None)
            start = req.content.get("start", 0)
            stop = req.content.get("stop", -1)
            interval = req.content.get("interval", None)
        records.types.now = now

        log.debug("Handling GET, start=%s, stop=%s, now=%s", 
            start, stop, records.types.now)

        return func(instance, records, start, stop, interval)

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
        content = self.req.content
        if not content:
            raise errors.HTTPBadRequest("No data")

        for key, data in content.items():
            try:
                records = model.Records(*key, exception=errors.HTTPBadRequest)
            except TypeError:
                raise errors.HTTPBadRequest("Invalid resource id")
            self.extend(records, data)

        raise errors.HTTPNoContent("Records created")

    def _get(self):
        queries = []
        query = {}
        for k, v in self.req.params.items():
            if k == "subject":
                if query:
                    queries.append(query)
                    query = {}
                query = {k: v}
            elif not query:
                raise errors.HTTPBadRequest("%r parameter must follow subject", k)
            else:
                query[k] = v
        if query:
            queries.append(query)

        data = {}
        for query in queries:
            rid = [query.pop(x, None) for x in "subject attribute cf".split()]
            records = model.Records(*rid, exception=errors.HTTPBadRequest)
            records.types.now = query.pop("now", None)
            result = records.query(**query)
            data[self.encodeid(records)] = list(result)

        return data

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

    @getmethod
    def get_json(self, records, start, stop, interval):
        data = {
            self.encodeid(records): list(records.query(start, stop, interval)),
        }

        self.response.status_int = 200
        self.response.body = json.dumps(data)
        callback = self.req.params.get("callback", None)
        if callback is not None:
            self.response.body = "%s(%s)" % (callback, self.response.body)

    @getmethod
    def get_csv(self, records, start, stop, interval):
        writer = csv.writer(self.response.body_file)
        writer.writerow("subject attribute cf timestamp value".split())
        for t, v in records.query(start, stop, interval):
            writer.writerow((records.subject, records.attribute, records.cf, t, v))

        self.response.status_int = 200

class Ping(neat.Resource):
    prefix = "/_ping"

    def get(self):
        self.response.body = "OK"
        self.response.status_int = 200
        self.response.content_type = "text/plain"

def Server(host, port, **kwargs):
    from .ext import wsgiserver

    service = neat.Dispatch(
        AllRecords(),
        Records(),
        Ping())
    server = wsgiserver.CherryPyWSGIServer((host, port), service, **kwargs)
    return server

class Serve(DBMixin, DaemonizingSubCommand):
    
    def main(self):
        host, _, port = self.params.server.partition(':')
        if not port:
            port = 8000

        self.log.info("Starting server at http://%s:%s/", host, port)
        server = Server(host, int(port),
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
        self.add_param("-n", "--nthreads", default=default_threads,
            help="minimum size of server thread pool (default: %s)" % default_threads)
        self.add_param("-r", "--requests", default=default_requests,
            help="size of the server request queue (default: %s)" % default_requests)
        self.add_param("-t", "--timeout", default=default_timeout,
            help="timeout (seconds) for accepted connections (default: %s)" % default_timeout)
        self.add_param("server", nargs="?",
            help="<host>:<port> (default: %s)" % default_server, default=default_server)
