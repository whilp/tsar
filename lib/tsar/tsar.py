import logging
import string
import time

from collections import namedtuple
from csv import DictReader
from functools import update_wrapper
from itertools import chain
from string import digits, letters, punctuation

from redis import Redis
from neat import Resource, Dispatch, validate
from webob.exc import HTTPBadRequest, HTTPNotFound

from .util import json

compose = lambda f, g: update_wrapper(lambda *a, **k: g(f(*a, **k)), f)

__all__ = ["Records", "validate"]

# Media types.
v = "application/vnd.tsar";
mediatypes = {
    "record": v + ".record.v1",
}

Interval = namedtuple("Interval", "interval samples")

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

class validate(validate):
    keychars = [x for x in digits + letters + punctuation if x not in "!/"]
    keylen = 128
    numbertypes = (int, float, long)
    precision = 2
    
    def Key(self, value):
        value = str(value)
        if len(value) > self.keylen:
            raise TypeError("value too long: %s" % repr(value))

        badchars = [x for x in value if x not in self.keychars]
        if badchars:
            raise TypeError("value contains reserved characters: %s" % repr(value))

        return value

    def Time(self, value, now=None):
        value = int(self.Number(value))
        if value < 0:
            if now is None: # pragma: nocover
                now = time.time()
            now = self.Time(now)
            value += now

        return value

    def Number(self, value):
        if isinstance(value, self.numbertypes):
            return value
        if '.' in value:
            return round(float(value), self.precision)
        try:
            return int(value)
        except ValueError, e:
            raise TypeError(e.args[0])

validate.exception = HTTPBadRequest

class RedisResource(Resource):
    delim = '!'
    
    def __init__(self, connection={}):
        self.db = Redis(**connection)

    def fromkey(self, key):
        return key.split(self.delim)

    def tokey(self, *chunks):
        chunks = chain(*[self.fromkey(str(c)) for c in chunks])
        return self.delim.join(validate().Key(c) for c in chunks)

class Records(RedisResource):
    prefix = "/record"
    media = {
        mediatypes["record"] + "+form": "form",
        "application/x-www-form-urlencoded": "form",
        mediatypes["record"] + "+json": "json",
        "application/json": "json",
    }
    cf = ["minimum", "average", "maximum", "last"]
    intervals = [Interval(*x) for x in [
        # interval  samples
        (60,        720),   # one minute, max 12 hours
        (3600,      672),   # one hour, max 28 days
        (86400,     730),   # one day,  max 2 years
        (604800,    480),   # one week, max 10 years
    ]]

    @validate(stamp="Time", value="Number")
    def tovalue(self, stamp, value):
        return self.tokey(stamp, value)

    def fromvalue(self, value):
        try:
            stamp, value = self.fromkey(value)
        except ValueError, e:
            raise TypeError(e.args[0])
        v = validate()
        return v.Time(stamp), v.Number(value)

    def handle_form(self):
        return self.req.params

    def handle_json(self):
        return dict((str(k), v) for k, v in json.loads(self.req.body).items())

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

    def post(self):
        self.create(**self.req.content)
        self.response.status_int = 201

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

    def get_json(self):
        result = self.list(**self.req.content)
        return json.dumps(result)
