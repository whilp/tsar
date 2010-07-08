import logging
import time

from neat import Resource, Dispatch
from webob.exc import HTTPBadRequest, HTTPNotFound

from .model import Records, Types
from .util import json

__all__ = ["Records", "validate"]

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "record": v + ".record.v1",
}

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

validate = Types().validate
validate.exception = HTTPBadRequest

class Records(Resource):
    prefix = "/record"
    media = {
        mediatypes["record"] + "+form": "form",
        "application/x-www-form-urlencoded": "form",
        mediatypes["record"] + "+json": "json",
        "application/json": "json",
    }

    # Base methods.
	def create(subject="Key", attribute="Key", cf="Key"):
		if cf not in Records.cfs:
		
		
    @validate(subject="Key", attribute="Key", stamp="Time", value="Number")
    def create(self, subject, attribute, stamp, value):
        intervals = self.tokey("records", subject, attribute, "intervals")
        pipe = self.db.pipeline()
        pipe.zadd(self.tokey("records", subject, attribute),
            self.tovalue(stamp, value), stamp)
        pipe.delete(intervals)
        for i in self.intervals:
            pipe.lpush(intervals, i.interval)
        pipe.sadd(self.tokey("queues", "records", "raw"),
            self.tokey(subject, attribute))
        pipe.execute()

    @validate(subject="Key", attribute="Key", start="Time", stop="Time", cf="Key")
    def list(self, subject, attribute, start=0, stop=time.time(), cf="average"):
        if start > stop:
            raise HTTPBadRequest("start %d is more recent than stop %d" % (
                start, stop))

        # Step through the list of intervals from largest to smallest, stopping
        # when we start seeing fewer results. This should give us the dataset
        # with the highest precision at the cost of a few extra roundtrips to
        # the server.
        interval = None
        data = []
        key = self.tokey("records", subject, attribute)
        for ival in self.intervals:
            k = self.tokey(key, ival.interval, cf)
            d = self.db.zrangebyscore(k, start, stop)
            if len(d) < len(data):
                break
            data = d
            interval = ival

        data = [self.fromvalue(d) for d in data]
        results = {
            "records": data,
            "cf": cf,
            "interval": interval.interval,
            "samples": interval.samples,
            "subject": subject,
            "attribute": attribute,
        }
        return results

    # Media type handlers.
    def handle_form(self):
        return self.req.params

    def handle_json(self):
        return dict((str(k), v) for k, v in json.loads(self.req.body).items())

    # HTTP handlers.
    def post(self):
        self.create(**self.req.content)
        self.response.status_int = 201

    def get_json(self):
        result = self.list(**self.req.content)
        return json.dumps(result)
