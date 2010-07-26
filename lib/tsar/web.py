import csv
import logging

from urllib2 import quote, unquote

from neat import neat

from . import errors, model
from .util import Decorator, json

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
        records.types.now = req.content.get("now", None)
        start = req.content.get("start", 0)
        stop = req.content.get("stop", -1)

        log.debug("Handling GET, start=%s, stop=%s, now=%s", 
            start, stop, records.types.now)

        return func(instance, records, start, stop)

class AllRecords(neat.Resource):
    prefix = "/records"
    media = {
        mediatypes["records"] + "+json": "json",
        "application/json": "json",
        mediatypes["records"] + "+csv": "csv",
        "text/csv": "csv",
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

    def handle_json(self):
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
    def get_json(self, records, start, stop):
        data = {
            self.encodeid(records): list(records.query(start, stop)),
        }

        self.response.status_int = 200
        self.response.body = json.dumps(data)
        callback = self.req.params.get("callback", None)
        if callback is not None:
            self.response.body = "%s(%s)" % (callback, self.response.body)

    @getmethod
    def get_csv(self, records, start, stop):
        writer = csv.writer(self.response.body_file)
        writer.writerow("subject attribute cf timestamp value".split())
        for t, v in records.query(start, stop):
            writer.writerow((records.subject, records.attribute, records.cf, t, v))

        self.response.status_int = 200

def Server(host, port, **dsn):
    from .ext import wsgiserver

    model.db = model.connect(**dsn)
    service = neat.Dispatch(
        AllRecords(),
        Records())
    server = wsgiserver.CherryPyWSGIServer((host, port), service)
    return server
