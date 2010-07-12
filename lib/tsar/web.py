import csv
import logging

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
    "records": v + ".records.v1",
}

class method(Decorator):
    
    def before(self, instance):
        try:
            subject, attribute, cf = \
                instance.req.path_info.lstrip('/').split('/', 3)[1:]
        except ValueError:
            raise errors.HTTPNotFound
        return [model.Records(subject, attribute, cf, 
            exception=errors.HTTPBadRequest)]

    def after(self, instance, value):
        return value

    def call(self, func, args, kwargs):
        instance = args[0]

        args = self.before(instance)
        value = func(instance, *args)
        value = self.after(instance, value)
        return value

class getmethod(method):
    
    def before(self, instance):
        log = logger(self)
        req = instance.req
        args = super(getmethod, self).before(instance)
        records = args[0]
        records.types.now = req.content.get("now", None)
        start = req.content.get("start", 0)
        stop = req.content.get("stop", -1)
        log.debug("Handling GET, start=%s, stop=%s, now=%s", 
            start, stop, records.types.now)
        args.extend([start, stop])
        return args

class Records(neat.Resource):
    prefix = "/records/"
    media = {
        mediatypes["records"] + "+json": "json",
        "application/json": "json",
        mediatypes["records"] + "+csv": "json",
        "text/csv": "csv",
    }
        
    # HTTP methods.
    @method
    def post(self, records):
        try:
            records.extend(self.req.content["data"])
        except errors.RecordError, e:
            raise errors.HTTPConflict(e.args[0])
        self.response.status_int = 204 # No Content

    @getmethod
    def get_json(self, records, start, stop):
        body = {}
        body["data"] = list(records.query(start, stop))
        body["start"] = start
        body["stop"] = stop

        self.response.status_int = 200
        self.response.body = json.dumps(body)

    @getmethod
    def get_csv(self, records, start, stop):
        # XXX: We don't use a csv.writer here because it doesn't work well with
        # unicode output across interpreter versions.
        self.response.body_file.write("timestamp,value\n")
        for t, v in records.query(start, stop):
            self.response.body_file.write("%s,%s\n" % (t, v))
        self.response.status_int = 200

    # HTTP helpers.
    def handle_json(self):
        return json.loads(self.req.body)

    def handle_csv(self):
        body = {}
        reader = csv.reader(iter(self.req.body.splitlines()))
        data = [reader.next()]
        if data[0] == ["timestamp", "value"]:
            data = []
        body["data"] = list(reader)
        return body

def Server(host, port, **dsn):
    from .ext import wsgiserver

    model.db = model.connect(**dsn)
    service = neat.Dispatch(
        Records())
    server = wsgiserver.CherryPyWSGIServer((host, port), service)
    return server
