import logging

import neat

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

class Records(neat.Resource):
    prefix = "/records/"
    media = {
        mediatypes["records"] + "+json": "json",
        "application/json": "json",
    }

    class method(Decorator):

        def call(self, func, args, kwargs):
            instance = args[0]
            subject, attribute, cf = \
                instance.req.path_info.lstrip('/').split('/', 3)[1:]
            records = model.Records(subject, attribute, cf, exception=errors.HTTPBadRequest)
            return func(instance, records)
        
    # HTTP methods.
    @method
    def post(self, records):
        try:
            records.extend(self.req.content["data"])
        except errors.RecordError, e:
            raise errors.HTTPConflict(e.args[0])
        self.response.status_int = 204 # No Content

    @method
    def get_json(self, records):
        body = {}
        start = self.req.content.get("start", 0)
        stop = self.req.content.get("stop", -1)
        records.types.now = self.req.content.get("now", None)
        body["data"] = list(records.query(start, stop))
        body["start"] = records.types.Time(start) 
        body["stop"] = records.types.Time(stop) 

        self.response.status_int = 200
        self.response.body = json.dumps(body)

    # HTTP helpers.
    def handle_json(self):
        return json.loads(self.req.body)

service = neat.Dispatch(
    Records())
