def consolidate(records, interval, cf=None):
    """Consolidate *records* according to *interval*.

    *records* should be an iterable composed of two-tuples like:
        
        (timestamp, value)

    *timestamp* is an integer representing seconds since the Unix Epoch; *value*
    is an integer or float. *interval* is the desired time between records (in
    seconds). *cf* is a callable taking a sequence of values to be consolidated
    into a single value. *records* should be sorted from smallest (earliest)
    *timestamp* to the largest (most recent). Yields interpolated (timestamp,
    value) tuples.
    """
    bin = None
    values = []
    if cf is None:
        cf = lambda values: values[-1]
    for stamp, value in records:
        remainder = stamp % interval
        rest = interval - remainder
        if abs(rest) > remainder:
            remainder = rest
        nearest = stamp - remainder
        if nearest != bin:
            yield bin, cf(values)
            bin = nearest
            values = []
        else:
            values.append(value)
