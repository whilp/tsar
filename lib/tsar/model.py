import tsar.errors

from tsar.lib.util import Decorator

delimiter = '!'

def fromkey(key, delimiter=delimiter):
    return key.split(delimiter)

def tokey(*chunks, delimiter=delimiter):
    return delimiter.join(str(c) for c in chunks)

def nearest(value, interval):
    """Round *value* to the nearest value evenly divisible by *interval*."""
    distance = value % interval
    if distance > (interval/2):
        distance -= interval
    return value - distance

class Records(object):
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
    
    def __init__(self, subject, attribute):
        global db

        self.subject = subject
        self.attribute = attribute
        self.db = db

    class lock(object):
        def __call__(self, db, key, expire):
            self.db = db
            self.key = key
            self.expire = expire
            return self
        
        def __enter__(self):
            self.db.setex(self.key, "", self.expire)

        def __exit__(self, *args):
            self.db.del(key)

    def subkey(self, *chunks):
        """Return a key within this :class:`Record`'s :attr:`namespace`."""
        return tokey(self.namespace, self.subject, self.attribute, *chunks)

    def record(self, pipeline, timestamp, value):
        """Add a new record to the series.

        The record consists of a Unix-style *timestamp* and a *value*. The new
        data will be consolidated and added to each of the intervals supported
        by the series. If any of the intervals now exceeds its sample limit, old
        data will be expired.

        All of the updates are queued in the *pipeline*. The caller is
        responsible for locking the relevant keys in the database, creating the
        pipeline and, finally, executing it. This approach allows for atomic
        updates of both single and multiple values.
        """
        for interval, samples in self.intervals:
            timestamp = nearest(timestamp, interval)
            for cf, cfunc in self.cfs.items():
                ikey = self.subkey(interval, cf)
                lkey = self.subkey(ikey, "last")

                last = self.db.get(lkey)
                if last is not None and timestamp < last:
                    raise tsar.errors.RecordError(
                        "New record is older than the last update")

                lastval = self.db.lindex(ikey, 0)
                if last == timestamp and lastval is not None:
                    value = cfunc(lastval, value)

                while (timestamp - last) > interval:
                    last += interval
                    pipe.lpush(ikey, None)
                pipe.set(lkey, timestamp)
                pipe.lpush(ikey, value)
                pipe.ltrim(ikey, 0, samples)

    def query(self, start, stop, cf="average"):
        """Select a range of data from the series.

        The range spans from *start* to *stop*, inclusive; *cf* is the name
        of the function used to consolidate the data. If all of the requested
        range could be selected from multiple intervals, data from the smallest
        interval (and the highest resolution) will be chosen.
        """
        raise NotImplementedError
