from csv import DictReader

__all__ = ["TypedCSVReader"]

class TypedCSVReader(DictReader):

    def __init__(self, f, fieldtypes={}, keytype=str, **kwargs):
        DictReader.__init__(self, f, **kwargs)
        self.keytype = str
        self.fieldtypes = fieldtypes
    
    def next(self):
        d = DictReader.next(self)
        newd = {}
        for k in d:
            coerce = self.fieldtypes.get(k, str)
            newd[self.keytype(k)] = coerce(d[k])

        return newd
