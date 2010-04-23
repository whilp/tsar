import csv
import logging
import string
import time

from calendar import timegm
from datetime import datetime, timedelta
from functools import update_wrapper
from time import gmtime

from redis import Redis
from neat import Resource, Service
from webob.exc import HTTPNotFound

compose = lambda f, g: update_wrapper(lambda *a, **k: g(f(*a, **k)), f)

class DictReader(csv.DictReader):
    """Produce keys that are strings even if input is unicode.

    This hack makes it possible to pass the resulting dictionaries as
    kwargs to functions.
    """
    keytype = str
    
    def next(self):
        d = csv.DictReader.next(self)
        newd = {}
        for k in d:
            newd[self.keytype(k)] = d[k]

        return newd

class DBResource(Resource):
    fieldchars = [x for x in string.digits + string.letters + string.punctuation if x not in "!/"]
    fieldlen = 128

    def __init__(self, collection="", mimetypes={}):application, request, **kwargs):
        super(DBResource, self).__init__(collection, mimetypes)

        self.redis = Redis()

    @classmethod
    def db_string(handler, field):
        if len(field) > handler.fieldlen:
            raise TypeError("field too long")

        badchars = [x for x in field if x not in handler.fieldchars]
        if badchars:
            raise TypeError("field contains reserved characters")

        return field

    @classmethod
    def db_int(handler, field):
        if '.' in field:
            return round(float(field), 2)
        return int(field)

    @classmethod
    def db_reltime(handler, field, now=None):
        field = int(field)
        if field < 0:
            if now is None:
                now = datetime.now()
            field = now + timedelta(seconds=field)
        else:
            field = datetime(*gmtime(field)[:6])

        return timegm(field.timetuple())

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
            except TypeError, e:
                raise HTTPBadRequest("Bad parameter: %s" % field)

        return _params

    @staticmethod
    def encodeval(time, value, sep=':'):
        return "%s%s%s" % (time, sep, value)

    @staticmethod
    def decodeval(value, sep=':'):
        time, junk, value = value.partition(sep)
        return self.db_int(time), self.db_int(value)

class Record(DBResource):
    
    @staticmethod
    def sample(self, sample, size, f=None):
        if f is None:
            f = lambda x: x

        samplesize = len(sample)
        if size <= 0 or samplesize < size:
            return [f(x) for x in sample]

        difference = samplesize - size
        if samplesize/size > 1:
            keep = lambda x: (x % samplesize)/difference
        else:
            keep = lambda x: x % (samplesize/difference)

        return [f(sample[x]) for x in xrange(samplesize) if keep(x)]

    def list(self, req):
        params = self.validate(req.params,
            subject=(self.db_string, "*"),
            attribute=(self.db_string, "*"),
            start=(self.db_reltime, 0),
            stop=(self.db_reltime, time.time())
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
        params = self.validate(req.params, callback=(self.db_string, None))
        result = self.list(req)

        req.response.content_type = "application/javascript"
        req.response.body = json.dumps(result)
        if params["callback"] is not None:
            req.response.body = "%s(%s)" % (params["callback"], req.response.body)

        return req.response

    def post(self):
        """Create a new observation."""
        fields = {
            "time": self.db_int,
            "value": self.db_int,
            "subject": self.db_string,
            "attribute": self.db_string,
        }

        contenttype = self.request.headers.get("Content-Type", None)
        observations = []
        if contenttype.startswith(u"multipart/form-data"):
            # Bulk update.
            _file = self.request.files.get("observations", [{}])[0]
            body = (l for l in _file.get("body", "").splitlines())
            filetype = _file.get("content_type", None)
            if filetype == u"text/csv":
                observations = DictReader(body)
        elif contenttype.startswith(u"application/x-www-form-urlencoded"):
            # Single update.
            observations = [dict((k, self.get_argument(k)) for k in fields)]

        created = False
        for observation in observations:
            self.record(fields, **observation)

            created = True

        if created:
            self.set_status(201)

    def create(self, time, value, subject, attribute):
        """Record an observation."""
        params = self.validate(
            dict(time=time, value=value, subject=subject, attribute=attribute),
            time=self.db_int,
            value=self.db_int,
            subject=self.db_string,
            attribute=self.db_string,
        }

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

class InterfaceHandler(StaticFileHandler):

    def __init__(self, application, request):
        root = application.settings["interface.root"]
        super(InterfaceHandler, self).__init__(application, request, root)

    def get(self, path, include_body=True):
        try:
            return super(InterfaceHandler, self).get(path, include_body)
        except HTTPError, e:
            # Append the html suffix if we can't find the file.
            if e.status_code != 404:
                raise
            return super(InterfaceHandler, self).get(path + ".html", include_body)

routes = [
    (r"/observations", ObservationsHandler),
    (r"/(.*)", InterfaceHandler),
]
application = WSGIApplication(routes)

