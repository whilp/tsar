import logging
import string
import time

from csv import DictReader
from functools import update_wrapper
from string import digits, letters, punctuation

from redis import Redis
from neat import Resource, Dispatch, validate
from webob.exc import HTTPBadRequest, HTTPNotFound

compose = lambda f, g: update_wrapper(lambda *a, **k: g(f(*a, **k)), f)

__all__ = ["Records", "validate"]

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
            raise TypeError("key too long")

        badchars = [x for x in value if x not in self.keychars]
        if badchars:
            raise TypeError("key contains reserved characters")

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
        return int(value)

# Keep a validate instance around to access its methods.
_validate = validate()

class RedisResource(Resource):
    delim = '!'

    def __init__(self, *args, **kwargs): # pragma: nocover
        super(RedisResource, self).__init__(*args, **kwargs)
        self.log = logger("tsar", self)
    
    def __init__(self, connection={}):
        self.db = Redis(**connection)

    def tokey(self, *chunks):
        return self.delim.join(chunks)

    def fromkey(self, key):
        return key.split(self.delim)

class Records(RedisResource):
    prefix = "/record"
    media = {
        "application/x-www-form-urlencoded": "form",
    }

    @validate(stamp="Key", value="Key")
    def tovalue(self, stamp, value):
        return self.key(stamp, value)

    def fromvalue(self, value):
        Number = _validate.Number
        return [Number(x) for x in self.fromkey(value)]

    @validate(subject="Key", attribute="Key", stamp="Time", value="Number")
    def create(self, subject, attribute, stamp, value):
        self.db.zadd(self.key(subject, attribute), self.value(stamp, value), stamp)

    def post_form(self):
        self.create(**self.req.params)
        self.response.status_int = 201
