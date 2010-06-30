delimiter = '!'

def fromkey(key, delimiter=delimiter):
    return key.split(delimiter)

def tokey(*chunks, delimiter=delimiter):
    return delimiter.join(str(c) for c in chunks)

class Records(object):
    ns = "records"
    intervals = [
        # interval  samples     unit
        (60,        720),     # minute, max 12 hours
        (3600,      672),     # hour, max 28 days
        (86400,     730),     # day, max 2 years (unbounded)
        #(604800,   480),     # week, max 10 years
    ]
    cf = "average"
    
    def __init__(self, subject, attribute):
        global db

        self.subject = subject
        self.attribute = attribute
        self.db = db

    @property
    def namespace(self):
        return tokey(self.ns, self.subject, self.attribute)

    def record(self, timestamp, value):
        raise NotImplementedError

    def query(self, start, stop, cf=None):
        raise NotImplementedError
