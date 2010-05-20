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
        if remainder > (interval/2):
            remainder = -(interval - remainder)
        nearest = stamp - remainder

        if bin is None:
            bin = nearest
        elif bin != nearest:
            yield bin, cf(values)
            bin = nearest
            values = []
        values.append(value)

    if values:
        yield bin, cf(values)
