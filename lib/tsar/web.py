import logging

from neat import Resource

from . import model
from .util import json

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "records": v + ".records.v1",
}

model.db = model.connect()

class Records(Resource):
    prefix = "/records/"
    media = {
        mediatypes["records"] + "+json": "json",
        "application/json": "json",
    }

    # Model interface.
    def record(self, subject, attribute, cf, data):
        records = model.Records(subject, attribute, cf)
        records.extend(data)

    def parseuri(self, uri):
        """Parse a URI.

        Returns a tuple (subject, attribute, cf).
        """
        return uri.lstrip('/').split('/', 3)[1:]

    # HTTP methods.
    def post(self):
        subject, attribute, cf = self.parseuri(self.req.path_info)
        self.record(subject, attribute, cf, self.req.content["data"])
        self.response.status_int = 204 # No Content

    # HTTP helpers.
    def handle_json(self):
        return json.loads(self.req.body)
