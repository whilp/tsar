import logging
import string
import time

from csv import DictReader
from functools import update_wrapper

from redis import Redis
from neat import Resource, Service
from webob.exc import HTTPBadRequest, HTTPNotFound

compose = lambda f, g: update_wrapper(lambda *a, **k: g(f(*a, **k)), f)

__all__ = ["DBResource", "Record", "service"]

class DBResource(Resource):
    fieldchars = [x for x in string.digits + string.letters + string.punctuation if x not in "!/"]
    fieldlen = 128

    def __init__(self, collection="", mimetypes={}):
        super(DBResource, self).__init__(collection, mimetypes)

        self.redis = Redis()

    def db_key(self, field):
        if len(field) > self.fieldlen:
            raise TypeError("field too long")

        badchars = [x for x in field if x not in self.fieldchars]
        if badchars:
            raise TypeError("field contains reserved characters")

        return field

    def db_int(self, field):
        if isinstance(field, (int, float, long)):
            return field
        if '.' in field:
            return round(float(field), 2)
        return int(field)

    def db_reltime(self, field, now=None):
        field = int(self.db_int(field))
        if field < 0:
            if now is None: # pragma: nocover
                now = time.time()
            now = self.db_reltime(now)
            field = now + field

        return field

    @staticmethod
    def validate(params, **fields):
        _params = {}
        NoDefault = object()
        for field, validator in fields.items():
            default = NoDefault

            # The validator must be a callable or a two-tuple.
            if not callable(validator):
                validator, default = validator

            param = params.get(field, default)
            if param is NoDefault:
                raise HTTPBadRequest("Missing parameter: %s" % field)
            try:
                param = validator(param)
            except (TypeError, ValueError), e:
                raise HTTPBadRequest("Bad parameter: %s" % field)
            _params[field] = param

        return _params

    def encodeval(self, time, value, sep=':'):
        return "%s%s%s" % (time, sep, value)

    def decodeval(self, value, sep=':'):
        time, junk, value = value.partition(sep)
        return self.db_int(time), self.db_int(value)

class Record(DBResource):
    mimetypes = {
        "application/json": "json",
        "text/csv": "csv",
        "application/x-www-form-urlencoded": "form",
    }
    
    @staticmethod
    def sample(sample, size, f=None):
        if f is None:
            f = lambda x: x

        samplesize = len(sample)
        if size <= 0 or samplesize < size:
            return [f(x) for x in sample]

        difference = samplesize - size
        keep = lambda x: x % (samplesize/difference)

        return [f(sample[x]) for x in xrange(samplesize) if keep(x)]

    def list(self, req):
        params = self.validate(req.params,
            subject=(self.db_key, "*"),
            attribute=(self.db_key, "*"),
            start=(self.db_reltime, 0),
            stop=(self.db_reltime, time.time()),
            sample=(self.db_int, 0)
        )

        if params["start"] > params["stop"]:
            raise HTTPBadRequest("Bad parameter: start must be less than stop")

        results = {}
        sa = []
        keys = self.redis.keys("observations!%(subject)s!%(attribute)s" % params)
        for key in keys:
            _, subject, attribute = key.split('!')
            sa.append((subject, attribute))
            results.setdefault(subject, {})
            results[subject][attribute] = \
                self.redis.zrangebyscore(key, params["start"], params["stop"])

        for s, a in sa:
            # Build the value processor. Since the members of the sorted
            # timeseries set have the timestamp prepended, we don't
            # need to request scores as well. Instead, we simply decode
            # the members themselves. Additionally, JavaScript expects
            # millisecond precision.
            jsprecision = lambda (t, v): (t * 1000, v)
            processor = compose(self.decodeval, jsprecision)

            # Downsample (if necessary) and apply the value processor
            # built above.
            results[s][a] = self.sample(results[s][a], sample, processor)

            params["len"] = len(results[s][a])
            params["subject"] = s
            params["attribute"] = a
            logging.debug("Serving %(len)d results for %(subject)s's "
                "%(attribute)s from %(start)d to %(stop)d", params)

        return {"results": results}

    def list_json(self, req):
        params = self.validate(req.params, callback=(self.db_key, None))
        result = self.list(req)

        req.response.content_type = "application/javascript"
        req.response.body = json.dumps(result)
        if params["callback"] is not None:
            req.response.body = "%s(%s)" % (params["callback"], req.response.body)

        return req.response

    def create(self, time=None, value=None, subject=None, attribute=None, **kwargs):
        """Record an observation."""
        params = self.validate(
            dict(time=time, value=value, subject=subject, attribute=attribute),
            time=self.db_int,
            value=self.db_int,
            subject=self.db_key,
            attribute=self.db_key,
        )

        # In Redis, we save each observation as in a sorted set with the
        # time of the observation as its score. This allows us to easily
        # pull observations in a range from history.

        # Since value is not likely to be unique across the observation
        # period, we make a unique member by prepending the time of
        # observation to the value. Consumers must reverse this process
        # to get at the actual data (ie, value.split(':')).

        key = "observations!%(subject)s!%(attribute)s" % params
        uniqueval = self.encodeval(params["time"], params["value"])
        self.redis.zadd(key, uniqueval, params["time"])
        logging.debug("Recording %(subject)s's %(attribute)s "
            "(%(value)d) at %(time)d", params)

    def create_form(self, req):
        self.create(**req.params)
        req.response.status_int = 201
        
    def create_csv(self, req):
        records = DictReader(req.body_file)

        created = False
        for record in records:
            created = True
            self.create(**record)

        if created:
            req.response.status_int = 201

service = Service(
    Record("records"),
)

if __name__ == "__main__":
    from wsgiref.simple_server import make_server
    server = make_server('', 8000, service)
    server.serve_forever()
