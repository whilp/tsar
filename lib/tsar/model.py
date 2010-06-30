delimiter = '!'

def fromkey(key, delimiter=delimiter):
    return key.split(delimiter)

def tokey(*chunks, delimiter=delimiter):
    return delimiter.join(chunks)

class Records(object):
    namespace = "records"
    intervals = [
        # interval  unit    samples
        60,       # minute  720 (12 hours)
        3600,     # hour    672 (28 days)
        86400,    # day     730 (2 years)
        #604800,  # week    480 (10 years) # technically unbounded
    ]
    cf = "average"
    
    def __init__(self, subject, attribute):
        global db

        self.subject = subject
        self.attribute = attribute
        self.db = db

    def record(self, timestamp, value):
        raise NotImplementedError

    def query(self, start, stop, cf=None):
        raise NotImplementedError
