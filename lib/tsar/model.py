delimiter = '!'

def fromkey(key, delimiter=delimiter):
    return key.split(delimiter)

def tokey(*chunks, delimiter=delimiter):
    return delimiter.join(str(c) for c in chunks)

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
    cfs = ["average", "minimum", "maximum", "last"]
    """Supported consolidation functions."""
    
    def __init__(self, subject, attribute):
        global db

        self.subject = subject
        self.attribute = attribute
        self.db = db

    def subkey(self, *chunks):
        """Return a key within this :class:`Record`'s :attr:`namespace`."""
        return tokey(self.namespace, self.subject, self.attribute, *chunks)

    def record(self, timestamp, value):
        """Add a new record to the series.

        The record consists of a Unix-style *timestamp* and a *value*. The new
        data will be consolidated and added to each of the intervals supported
        by the series. If any of the intervals now exceeds its sample limit, old
        data will be expired.
        """
        for interval, samples in self.intervals:
            ikey = self.subkey(interval)
			
        key = tokey(self.ns, self.intervals[0])

    def query(self, start, stop, cf="average"):
        """Select a range of data from the series.

        The range spans from *start* to *stop*, inclusive; *cf* is the name
        of the function used to consolidate the data. If all of the requested
        range could be selected from multiple intervals, data from the smallest
        interval (and the highest resolution) will be chosen.
        """
        raise NotImplementedError
