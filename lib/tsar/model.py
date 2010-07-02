from itertools import chain
from string import digits, letters, punctuation

from neat import validate

from . import errors
from .util import Decorator

def nearest(value, interval):
    """Round *value* to the nearest value evenly divisible by *interval*."""
    distance = value % interval
    if distance > (interval/2):
        distance -= interval
    return value - distance

def consolidate(data, interval, cfunc, missing=None):
    """Consolidate a data in a timeseries.

    *data* is an iterable consisting of two-tuples (timestamp, value), where
    timestamp is a Unix timestamp and value is an integer or float. The
    timestamps in *data* should be increasing. *interval* is an integer that
    determins the size of the bins. *cfunc* is a callable that takes two
    adjacent values as its arguments and returns a new, consolidated value. If
    there are any gaps in the consolidated data set, they will be filled with
    *missing* values.

    Yields an iterable of new (timestamp, value) two-tuples.
    """
    lasttime, lastval = None, None
    first = True
    for timestamp, value in data:
        timestamp = nearest(timestamp, interval)

        if lasttime is None:
            lasttime = timestamp
        if timestamp < lasttime:
            raise errors.RecordError(
                "Series out of order: %d more recent than %d", lasttime, timestamp)

        # Fill in any missing values.
        while (timestamp - lasttime) > interval:
            lasttime += interval
            yield (lasttime, missing)

        if lastval is not None:
            value = cfunc(lastval, value)
        if first or timestamp != lasttime:
            yield (timestamp, value)
            first = False
            lasttime = timestamp

class Types(validate):
    keylen = 128
    keydelim = '!'
    keychars = [x for x in digits + letters + punctuation \
        if x not in (keydelim, '/')]
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

    def Value(self, value):
        if value is not None:
            value = self.Number(value)

        return value

class DBObject(object):
    types = Types()

    def __init__(self, db):
        super(DBObject, self).__init__()
        self.db = db

    def fromkey(self, key):
        return key.split(self.types.keydelim)

    def tokey(self, *chunks):
        return self.types.keydelim.join(self.types.Key(c) for c in chunks)

    class Lock(object):

        def __call__(self, db, key, expire):
            self.db = db
            self.key = key
            self.expire = expire
            return self
        
        def __enter__(self):
            self.db.setex(self.key, "", self.expire)

        def __exit__(self, *args):
            self.db.delete(self.key)

    lock = Lock()

class Records(DBObject):
    """A series of records."""

    namespace = "records"
    """Root namespace in the data store.

    All class:`Records` instances will be stored somewhere under the root
    namespace.
    """
    intervals = [
        # interval  samples     unit
        (60,        720),     # minute, max 12 hours
        (3600,      672),     # hour, max 28 days
        (86400,     730),     # day, max 2 years (unbounded)
        #(604800,   480),     # week, max 10 years
    ]
    """Consolidation intervals and sample limits.

    These intervals determine the size of the bins into which new data is
    consolidated. The samples determine the number of bins kept for each
    interval.
    """
    cfs = {
        "average": None, # XXX
        "minimum": min,
        "maximum": max,
        "last": lambda x, y: y,
    }
    """Supported consolidation functions."""
    
    def __init__(self, db, subject, attribute, cf="last"):
        super(Records, self).__init__(db)

        self.subject = subject
        self.attribute = attribute
        self.cf = cf

    def subkey(self, *chunks):
        """Return a key within this :class:`Record`'s :attr:`namespace`."""
        return self.tokey(self.namespace, \
            self.subject, self.attribute, self.cf, *chunks)

    def query(self, start, stop):
        """Select a range of data from the series.

        The range spans from *start* to *stop*, inclusive. If all of the
        requested range could be selected from multiple intervals, data from the
        smallest interval (and the highest resolution) will be chosen.

        Returns an iterator.
        """
        for interval, samples in self.intervals:
            ikey = self.subkey(interval)
            lkey = self.subkey(interval, "last")

            last = self.db.get(lkey)
            if last is None:
                raise StopIteration

            llen = self.db.llen(ikey)
            first = last - (llen * interval)
            istart, istop = nearest(start, interval), nearest(stop, interval)
            if not (first <= ifirst and last >= istop):
                continue

            data = self.db.lrange(ikey, (last - istop)/interval, (last - istart)/interval)
            timestamp = istop
            while data:
                yield (timestamp, data.pop())
                timestamp -= interval

    def record(self, pipeline, data):
        """Add new data to the series.

        *data* is an iterable consisting of two-tuples (timestamp, value).
        *timestamp* is a Unix-style timestamp; *value* is a float or an integer.
        The new data will be consolidated and added to each of the intervals
        supported by the series. If any of the intervals now exceeds its sample
        limit, old data will be expired.

        All of the updates are queued in the *pipeline*. The caller is
        responsible for locking the relevant keys in the database, creating the
        pipeline and, finally, executing it. This approach allows for atomic
        updates of both single and multiple values.
        """
        if not data:
            return

        lasti = len(self.intervals) - 1
        cfunc = self.cfs[self.cf]
        dirty = {}
        lkeys = [self.subkey(i, "last") for i, s in self.intervals]
        for i, last in enumerate(self.db.mget(lkeys)):
            interval, samples = self.intervals[i]
            ikey = self.subkey(interval)
            lkey = lkeys[i]

            lasttime, lastval = None, None
            if last is not None:
                lasttime, lastval = last.split()
                lasttime = self.types.Time(lasttime)
                lastval = self.types.Value(lastval)

            if lasttime is not None:
                data = chain([(lasttime, lastval)], data)
            idata = consolidate(data, interval, cfunc)

            # Only remove the possibly redundant first entry if we're actually
            # going to write new data.
            needspop = True
            for timestamp, value in idata:
                timestamp = self.types.Time(timestamp)
                value = self.types.Value(value)
                if needspop:
                    pipeline.lpop(ikey)
                    needspop = False
                pipeline.lpush(ikey, value)

            if not needspop:
                dirty.setdefault("last", [])
                dirty["last"].append((lkey, ' '.join(str(x) for x in (timestamp, value))))
                # Don't trim the last interval, letting it grow.
                if i < lasti:
                    pipeline.ltrim(ikey, 0, samples)
        last = dirty.get("last", [])
        if last:
            pipeline.mset(dict(last))

    def extend(self, iterable):
        """Atomically extend the series with new values from *iterable*.

        Each value is a two-tuple consisting of (timestamp, value) that willbe
        passed to :meth:`record`.
        """
        with self.lock(self.db, self.subkey("lock"), 60):
            pipe = self.db.pipeline(transaction=True)
            self.record(pipe, iterable)
            pipe.execute()

    def append(self, value):
        """Atomically append a new *value* to the series.

        *value* is a two-tuple consisting of (timestamp, value) that will be
        passed to :meth:`record`.
        """
        self.extend([value])
