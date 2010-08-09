import logging
import operator
import time

from calendar import timegm
from datetime import datetime
from itertools import chain, dropwhile
from string import digits, letters, punctuation

from neat.util import validate, validator

from . import errors
from .util import nearest

__all__ = ["Records", "connect", "db"]

db = None

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

def connect(**kwargs):
    import redis
    return redis.Redis(**kwargs)

def cma(last, next, i):
    """Calculate the moving average of two items in a larger series.

    *last* and *next* are adjacent items in a series at indices *i* - 1 and *i*,
    respectively.
    """
    if i == 0:
        i += 1
    return (i, last + float(next - last)/(i + 1))

class Types(validate):
    keylen = 128
    keydelim = '!'
    keychars = [x for x in digits + letters + punctuation \
        if x not in (keydelim, '/')]
    numbertypes = (int, float, long)
    precision = 2
    now = None
    
    @validator
    def Key(self, value):
        value = str(value)
        if len(value) > self.keylen:
            raise TypeError("value too long: %s" % repr(value))

        badchars = [x for x in value if x not in self.keychars]
        if badchars:
            raise TypeError("value contains reserved characters: %s" % repr(value))

        return value

    @validator
    def Time(self, value):
        timetuple = getattr(value, "timetuple", None)
        if callable(timetuple):
            value = timegm(timetuple())
        else:
            value = int(self.Number(value))
        if value < 0:
            now = self.now
            if now is None: # pragma: nocover
                now = int(time.time())
            value += self.Time(now)

        return value

    Time.todatetime = lambda value: datetime(*time.gmtime(value)[:6])

    @validator
    def Number(self, value):
        if isinstance(value, self.numbertypes):
            return value
        if '.' in value:
            return round(float(value), self.precision)
        try:
            return int(value)
        except ValueError, e:
            raise TypeError(e.args[0])

    @validator
    def Value(self, value):
        # The backend stores nil values as "None" strings; convert them here to
        # None.
        if value == "None":
            value = None
        if value is not None:
            value = self.Number(value)

        return value

class Records(object):
    """A series of records."""

    namespace = "records"
    """Root namespace in the data store.

    All class:`Records` instances will be stored somewhere under the root
    namespace.
    """
    intervals = [
        # interval  samples     unit
        (60,        1440),    # minute, max 24 hours
        (3600,      1344),    # hour, max 56 days (two months)
        (86400,     1460),    # day, max 2 years (actually unbounded)
    ]
    """Consolidation intervals and sample limits.

    These intervals determine the size of the bins into which new data is
    consolidated. The samples determine the number of bins kept for each
    interval.
    """
    cfs = {
        "ave": cma,
        "min": lambda x, y, i: (i, min(x, y)),
        "max": lambda x, y, i: (i, max(x, y)),
        "first": lambda x, y, i: (i, x),
        "last": lambda x, y, i: (i, y),
        "add": lambda x, y, i: (i, operator.add(x, y)),
        "sub": lambda x, y, i: (i, operator.sub(x, y)),
    }
    """Supported consolidation functions."""
    
    def __init__(self, subject, attribute, cf, exception=None, excs=None):
        super(Records, self).__init__()

        global db
        self.db = db
        self.subject = subject
        self.attribute = attribute
        if cf not in self.cfs:
            raise errors.RecordError("Invalid cf: %s" % self.cf)
        self.cf = cf
        self.types = Types()
        self.types.exception = exception
        self.types.excs = excs

    def __eq__(self, other):
        if not isinstance(other, Records):
            return False

        return self.subkey("__eq__") == other.subkey("__eq__")

    @classmethod
    def all(self):
        """Return an iterable with all records known to the database."""
        global db
        for record in db.smembers(self.namespace):
            yield Records(*record.split())

    def delete(self):
        """Remove the instance from the database."""
        with self.lock(self.db, self.subkey("lock"), 60):
            pipe = self.db.pipeline(transaction=True)
            pipe.delete(*self.keys())
            pipe.srem(self.namespace, "%s %s %s" % (
                    self.subject, self.attribute, self.cf))
            pipe.execute()

    def rename(self, new):
        """Rename the series in the database."""
        keys = list(self.keys())
        keymap = zip(keys, new.keys())
        if len(keymap) != len(keys):
            raise errors.RecordError("Key mismatch in new Records instance")

        with self.lock(self.db, self.subkey("lock"), 60):
            check = True
            pipe = self.db.pipeline(transaction=True)
            for src, dst in keymap:
                if check and self.db.exists(dst):
                    raise errors.RecordError("New Records instance already exists")
                elif check:
                    check = False
                pipe.renamenx(src, dst)
            pipe.execute()

    def keys(self):
        """Return an iterable of keys used by this instance."""
        for i, samples in self.intervals:
            yield self.subkey(i)
            yield self.subkey(i, "last")

    def subkey(self, *chunks):
        """Return a key within this :class:`Record`'s :attr:`namespace`."""
        return self.tokey(self.namespace, \
            self.subject, self.attribute, self.cf, *chunks)

    def consolidate(self, data, interval, cfunc, i=0, missing=None):
        """Consolidate a data in a time series.

        *data* is an iterable consisting of two-tuples (timestamp, value), where
        timestamp is a Unix time stamp and value is an integer or float. The
        time stamps in *data* should be increasing. *interval* is an integer that
        determines the size of the bins. *cfunc* is a callable that takes two
        adjacent values as its arguments and returns a new, consolidated value. If
        there are any gaps in the consolidated data set, they will be filled with
        *missing* values.

        Yields an iterable of new (timestamp, value) two-tuples.
        """
        log = logger(self)
        lasttime = None
        lastval = 0
        for timestamp, value in data:
            timestamp = nearest(timestamp, interval)
            if lasttime is not None and lasttime > timestamp:
                raise errors.RecordError(
                    "Series out of order: %d more recent than %d" % (lasttime, timestamp))

            # Fill in any missing values.
            while lasttime is not None and ((timestamp - lasttime) > interval):
                yield (lasttime, lastval, i)
                lasttime += interval; lastval = missing; i = 0

            if timestamp != lasttime:
                # We've entered a new interval, so dump whatever we were working
                # on and start over.
                if lasttime is not None:
                    yield (lasttime, lastval, i)
                lasttime = timestamp
                lastval = value
                i = 0
            else:
                # This record belongs in the current bin, so consolidate it.
                i, lastval = cfunc(lastval, value, i)
                i += 1

        # We've reached the end of the series. If we're in the middle of
        # something, yield it.
        if lasttime is not None:
            yield (lasttime, lastval, i)

    def fromkey(self, key):
        return key.split(self.types.keydelim)

    def tokey(self, *chunks):
        return self.types.keydelim.join(self.types.Key(c) for c in chunks)

    def fromlast(self, last):
        timestamp, value, points = last.split()
        return self.types.Time(timestamp), self.types.Value(value), int(points)

    def tolast(self, timestamp, value, points):
        return "%d %r %d" % (timestamp, value, points)

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

    def query(self, start=0, stop=-1, interval=None, **kwargs):
        """Select a range of data from the series.

        The range spans from Unix time stamps *start* to *stop*, inclusive. If
        all of the requested range could be selected from multiple intervals,
        data from the smallest interval (and the highest resolution) will be
        chosen.

        Returns an iterator.
        """
        log = logger(self)
        empty = (x for x in [])
        start, stop = self.types.Time(start), self.types.Time(stop)

        # Shortcut: if we know the desired interval, we don't need to do all that
        # guessing (see below).
        if interval is not None:
            interval = int(interval)
            samples = None
            for i, s in self.intervals:
                if i == interval:
                    interval, samples = i, s
                    break
            if samples is None:
                return empty

            return self.select(start, stop, interval)

        # Choose the most appropriate consolidation interval to answer the
        # query.
        nintervals = len(self.intervals) - 1
        lkeys = [self.subkey(i, "last") for i, s in self.intervals]
        ikey = None
        log.debug("MGET %r", lkeys)
        for i, last in enumerate(self.db.mget(lkeys)):
            interval, samples = self.intervals[i]
            istart, istop = nearest(start, interval), nearest(stop, interval)
            lkey = lkeys[i]

            # Bail if we don't have any data yet.
            if last is None:
                return empty

            # Choose the first interval that might encompass the requested
            # range. If we're on the last interval, just use that (it's the best
            # we'll be able to do).
            lasttime = self.fromlast(last)[0]
            earliest = lasttime - (interval * samples)

            inrange = (start > (earliest - interval)) and (stop < (lasttime + interval))
            if inrange or (i >= nintervals):
                ikey = self.subkey(interval)
                break

        # Bail if we didn't find a suitable interval.
        if ikey is None:
            return empty

        return self.select(start, stop, interval, lasttime)

    def select(self, start, stop, interval, lasttime=None):
        log = logger(self)
        start, stop = self.types.Time(start), self.types.Time(stop)
        istart, istop = nearest(start, interval), nearest(stop, interval)
        ikey = self.subkey(interval)

        if lasttime is None:
            last = self.db.get(self.subkey(interval, "last"))
            lasttime = self.fromlast(last)[0]

        # Convert start and stop to indexes on the series. Since the series is
        # stored from most recent to oldest in the database, we flip the order
        # here.
        if istop > lasttime:
            istop = lasttime
        first, last = (lasttime - istop)/interval, (lasttime - istart)/interval

        # The data here runs from most to least recent, so we need to yield it
        # in reverse order.
        log.debug("LRANGE %s %r %r", ikey, first, last)
        data = self.db.lrange(ikey, first, last)
        dlen = len(data)
        timestamp = istop - ((dlen - 1)* interval)
        for i in xrange(dlen):
            value = self.types.Value(data[-(i + 1)])
            yield (timestamp, value)
            timestamp += interval

    def record(self, pipeline, data):
        """Add new data to the series.

        *data* is an iterable consisting of two-tuples (timestamp, value).
        *timestamp* is a Unix-style time stamp; *value* is a float or an integer.
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
        log = logger(self)

        nintervals = len(self.intervals) - 1
        cfunc = self.cfs[self.cf]
        dirty = {}
        lkeys = [self.subkey(i, "last") for i, s in self.intervals]
        log.debug("MGET %r", lkeys)
        for i, last in enumerate(self.db.mget(lkeys)):
            interval, samples = self.intervals[i]
            ikey = self.subkey(interval)
            lkey = lkeys[i]

            lasttime, lastval, lasti = None, None, 0
            if last is not None:
                lasttime, lastval, lasti = last.split()
                lasti = self.types.Number(lasti)

            idata = ((self.types.Time(t), self.types.Value(v)) for t, v in data)
            if lasttime is not None:
                notrecent = lambda x: x[0] <= lasttime
                lasttime = self.types.Time(lasttime)
                lastval = self.types.Value(lastval)
                idata = dropwhile(notrecent, idata)
                idata = chain([(lasttime, lastval)], idata)
            idata = self.consolidate(idata, interval, cfunc, lasti)

            # Only remove the possibly redundant first entry if we're actually
            # going to write new data.
            needspop = True
            for timestamp, value, points in idata:
                if needspop:
                    log.debug("LPOP %s", ikey)
                    pipeline.lpop(ikey)
                    needspop = False
                log.debug("LPUSH %s %r", ikey, value)
                pipeline.lpush(ikey, value)

            if not needspop:
                dirty.setdefault("last", [])
                dirty["last"].append((lkey, self.tolast(timestamp, value, points)))
                # Don't trim the last interval, letting it grow.
                if i < nintervals:
                    log.debug("LTRIM %s %r %r", ikey, 0, samples)
                    pipeline.ltrim(ikey, 0, samples)
        last = dirty.get("last", [])
        if last:
            last = dict(last)
            log.debug("MSET %r", last)
            pipeline.mset(last)
            pipeline.sadd(self.namespace, "%s %s %s" % (
                self.subject, self.attribute, self.cf))

    def extend(self, iterable):
        """Atomically extend the series with new values from *iterable*.

        Each value is a two-tuple consisting of (timestamp, value) that will be
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
