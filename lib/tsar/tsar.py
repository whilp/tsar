import logging
import string
import time

from csv import DictReader
from functools import update_wrapper
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

def logger(base, cls): # pragma: nocover
    return logging.getLogger("%s.%s" % (base, cls.__class__.__name__))

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

    def __init__(self, *args, **kwargs): # pragma: nocover
        super(RedisResource, self).__init__(*args, **kwargs)
        self.log = logger("tsar", self)
    
    def __init__(self, connection={}):
        self.db = Redis(**connection)

    def tokey(self, *chunks):
        return self.delim.join(validate().Key(c) for c in chunks)

    def fromkey(self, key):
        return key.split(self.delim)

class Records(RedisResource):
    prefix = "/record"
    media = {
        mediatypes["record"] + "+form": "form",
        "application/x-www-form-urlencoded": "form",
        mediatypes["record"] + "+json": "json",
        "application/json": "json",
    }

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

    @validate(subject="Key", attribute="Key", stamp="Time", value="Number")
    def create(self, subject, attribute, stamp, value):
        self.db.zadd(self.tokey(subject, attribute), self.tovalue(stamp, value), stamp)

    def post_form(self):
        self.create(**self.req.params)
        self.response.status_int = 201

    def post_json(self):
        params = dict((str(k), v) for k, v in json.loads(self.req.body).items())
        self.create(**params)
        self.response.status_int = 201
