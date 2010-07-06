import logging

from neat import Resource, errors

from . import model
from .util import Decorator, json

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "records": v + ".records.v1",
}

model.db = model.connect()
model.Types.validator.exception = errors.HTTPBadRequest

class Records(Resource):
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
            records = model.Records(subject, attribute, cf)
            return func(instance, records)
        
    # HTTP methods.
    @method
    def post(self, records):
        records.extend(self.req.content["data"])
        self.response.status_int = 204 # No Content

    # HTTP helpers.
    def handle_json(self):
        return json.loads(self.req.body)
