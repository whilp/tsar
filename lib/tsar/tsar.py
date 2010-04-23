import csv
import logging
import string

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

    def validate(self, fields, **kwargs):
        params = {}
        for field, validator in fields.items():
            try:
                params[field] = validator(kwargs[field])
            except KeyError, e:
                raise HTTPBadRequest("Missing parameter: %s" % field)
            except TypeError, e:
                raise HTTPBadRequest("%s: %s" % (e.args[0], field))

        return params

    def encodeval(self, time, value, sep=':'):
        return "%s%s%s" % (time, sep, value)

    def decodeval(self, value, sep=':'):
        time, junk, value = value.partition(sep)
        return self.db_int(time), self.db_int(value)

class ObservationsHandler(APIHandler):

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

    def get(self):
        fields = {
            "start": self.db_reltime,
            "stop": self.db_reltime,
            "subject": self.db_string,
            "attribute": self.db_string,
        }
        kwargs = dict((k, self.get_argument(k)) for k in fields)
        kwargs = self.validate(fields, **kwargs)

        # Options.
        try:
            sample = int(self.get_argument("sample", 0))
        except TypeError:
            raise HTTPError(400, "bad value for 'sample'")

        if kwargs["start"] > kwargs["stop"]:
            raise HTTPError(400, "start must be less than stop")

        results = {}
        sa = []
        keys = self.redis.keys("observations!%(subject)s!%(attribute)s" % kwargs)
        for key in keys:
            _, subject, attribute = key.split('!')
            sa.append((subject, attribute))
            results.setdefault(subject, {})
            results[subject][attribute] = self.redis.zrangebyscore(key, kwargs["start"], kwargs["stop"])

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

            kwargs["len"] = len(results[s][a])
            kwargs["subject"] = s
            kwargs["attribute"] = a
            logging.debug("Serving %(len)d results for %(subject)s's "
                "%(attribute)s from %(start)d to %(stop)d", kwargs)

        # Output JSON or JSONP (if the callback argument is present).
        self.set_header("Content-Type", "text/javascript; charset=UTF-8")
        callback = self.get_argument("callback", None)
        out = json_encode({"results": results})
        if callback is not None:
            out = "%s(%s)" % (callback, out)

        self.write(out)

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

    def record(self, fields, **kwargs):
        """Record an observation."""
        missing_fields = [k for k in fields if k not in kwargs]
        if missing_fields:
            raise HTTPError(400, "missing fields: %s" % ", ".join(missing_fields))

        extra_fields = [k for k in kwargs if k not in fields]
        if extra_fields:
            raise HTTPError(400, "extra fields: %s" % ", ".join(extra_fields))

        kwargs = self.validate(fields, **kwargs)

        # In Redis, we save each observation as in a sorted set with the
        # time of the observation as its score. This allows us to easily
        # pull observations in a range from history.

        # Since value is not likely to be unique across the observation
        # period, we make a unique member by prepending the time of
        # observation to the value. Consumers must reverse this process
        # to get at the actual data (ie, value.split(':')).

        key = "observations!%(subject)s!%(attribute)s" % kwargs
        uniqueval = self.encodeval(kwargs["time"], kwargs["value"])
        self.redis.zadd(key, uniqueval, kwargs["time"])
        logging.debug("Recording %(subject)s's %(attribute)s "
            "(%(value)d) at %(time)d", kwargs)

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

