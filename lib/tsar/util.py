import functools
import logging
import operator
import sys

try:
    import json
except ImportError: # pragma: nocover
    import simplejson as json

try:
    from functools import wraps
except ImportError:
    def update_wrapper(wrapper, wrapped):
        for attr in "module name doc".split():
            attr = "__%s__" % attr
            setattr(wrapper, attr, getattr(wrapped, attr))
        for attr in "dict".split():
            attr = "__%s__" % attr
            getattr(wrapper, attr).update(getattr(wrapped, attr, {}))

        return wrapper

    def wraps(wrapped):
        return partial(update_wrapper, wrapped=wrapped)

class Decorator(object):

    def __new__(cls, func=None, **kwargs):
        obj = super(Decorator, cls).__new__(cls)

        if func is not None:
            obj.__init__(**kwargs)
            obj = obj.wrap(func)

        return obj

    def __call__(self, *args, **kwargs):
        func = self.func
        if func is None:
            func = args[0]
            args = args[1:]

        return self.wrap(func, args, kwargs)

    def wrap(self, func, args=(), kwargs={}):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, args, kwargs)
            return func(*args, **kwargs)

        return wrapper

    def call(self, func, args, kwargs):
        return func(*args, **kwargs)

class log(Decorator):

    def __init__(self, func=None, level=logging.NOTSET,
        format=logging.BASIC_FORMAT, stream=sys.stderr, date=None):
        self.func = func
        self.level = level
        self.format = format
        self.stream = stream
        self.date = date

    def call(self, func, args, kwargs):
        funcname = func.func_name

        self.stream.write("\n>>> begin log for %s\n" % funcname)
        logMultiprocessing = logging.logMultiprocessing
        logging.logMultiprocessing = 0

        level = logging.root.level
        logging.root.setLevel(self.level)

        handlers = logging.root.handlers
        formatter = logging.Formatter(self.format, self.date)
        handler = logging.StreamHandler(self.stream)
        handler.setFormatter(formatter)
        logging.root.handlers = [handler]

        try:
            result = func(*args, **kwargs)
        finally:
            logging.root.setLevel(level)
            logging.logMultiprocessing = logMultiprocessing
            logging.root.handlers = handlers
            self.stream.write(">>> end log for %s\n" % funcname)

        return result

def parsedsn(dsnstring, **defaults):
    "<driver>://<username>:<password>@<host>:<port>/<database>"
    dsn = dict((x, None) for x in "driver username password host port database".split())
    dsn.update(defaults)
    dsn["driver"], _, rest = dsnstring.partition("://")
    if '@' in rest:
        user, _, rest = rest.partition('@')
        if ':' in user:
            user, _, dsn["password"] = user.partition(':')
        dsn["user"] = user
    if '/' in rest:
        host, _, dsn["database"] = rest.partition('/')
    if ':' in host:
        host, _, dsn["port"] = host.partition(':')
    dsn["host"] = host
    if dsn["port"] is not None:
        dsn["port"] = int(dsn["port"])

    return dsn

def nearest(value, interval):
    """Round *value* to the nearest value evenly divisible by *interval*."""
    distance = value % interval
    if distance > (interval/2):
        distance -= interval
    return value - distance

def trim(s, subs, reverse=False):
    find = s.find
    if reverse:
        find = s.rfind
    i = find(subs)

    if i < 0:
        start, end = 0, None
    elif reverse:
        start, end = 0, i
    else:
        start, end = len(subs), None

    return s[start:end]

rtrim = functools.partial(trim, reverse=True)

# Various filters for numerical differentiation.

# N-point stencils (see eg http://www.holoborodko.com/pavel/?page_id=239)
stencils = {
    3: lambda y, h: (y[1] - y[-1])/(2 * h),
    5: lambda y, h: (y[-2] - (8 * y[-1]) + (8 * y[1]) - y[2])/(12 * h),
    7: lambda y, h: (-y[-3] + (9 * y[-2]) - (45 * y[-1]) + \
        (45 * y[1]) - (9 * y[2]) + y[3])/(60 * h),
    9: lambda y, h: ((3 * y[-4]) - (32 * y[-3]) + (168 * y[-2]) - (672 * y[-1]) + \
        (672 * y[1]) - (168 * y[2]) + (32 * y[3]) - (3 * y[4]))/(840 * h),
}

# See: http://www.holoborodko.com/pavel/?page_id=242
lanczos2 = {
    5: lambda y, h: (y[1] - y[-1] + (2 * (y[2] - y[-2])))/(10 * h),
    7: lambda y, h: (y[1] - y[-1] + (2 * (y[2] - y[-2])) + \
        (3 * (y[3] - y[-3])))/(28 * h),
    9: lambda y, h: (y[1] - y[-1] + (2 * (y[2] - y[-2])) + \
        (3 * (y[3] - y[-3])) + (4 * (y[4] - y[-4])))/(60 * h),
    11: lambda y, h: (y[1] - y[-1] + (2 * (y[2] - y[-2])) + \
        (3 * (y[3] - y[-3])) + (4 * (y[4] - y[-4])) + \
            (5 * (y[5] - y[-5])))/(110 * h),
}
lanczos4 = {
    7: lambda y, h: ((58 * (y[1] - y[-1])) + (67 * (y[2] - y[-2])) - \
        (22 * (y[3] - y[-3])))/(252 * h),
    9: lambda y, h: ((126 * (y[1] - y[-1])) + (193 * (y[2] - y[-2])) + \
        (142 * (y[3] - y[-3])) - (86 * (y[4] - y[-4])))/(1188 * h),
    11: lambda y, h: ((296 * (y[1] - y[-1])) + (503 * (y[2] - y[-2])) + \
        (532 * (y[3] - y[-3])) + (294 * (y[4] - y[-4])) - \
            (300 * (y[5] - y[-5])))/(5148 * h),
}

# See: http://www.holoborodko.com/pavel/?page_id=245
snrd2 = {
    5: lambda y, h: ((2 * (y[1] - y[-1])) + y[2] - y[-2])/(8 * h),
    7: lambda y, h: ((5 * (y[1] - y[-1])) + (4 * (y[2] - y[-2])) + \
        y[3] - y[-3])/(32 * h),
    9: lambda y, h: ((14 * (y[1] - y[-1])) + (14 * (y[2] - y[-2])) + \
        (6 * (y[3] - y[-3])) + y[4] - y[-4])/(128 * h),
    11: lambda y, h: ((42 * (y[1] - y[-1])) + (48 * (y[2] - y[-2])) + \
        (27 * (y[3] - y[-3])) + (8 * (y[4] - y[-4])) + y[5] - y[-5])/(512 * h),
}
snrd4 = {
    7: lambda y, h: ((39 * (y[1] - y[-1])) + (12 * y[2] - y[-2]) - \
        (5 * (y[3] - y[-3])))/(96 * h),
    9: lambda y, h: ((27 * (y[1] - y[-1])) + (16 * (y[2] - y[-2])) - \
        (y[3] - y[-3]) - (2 * (y[4] - y[-4])))/(96 * h),
    11: lambda y, h: ((322 * (y[1] - y[-1])) + (256 * (y[2] - y[-2])) + \
        (39 * (y[3] - y[-3])) - (32 * (y[4] - y[-4])) - (11 * (y[5] - y[-5])))/(1536 * h)
}


def derive(seq, points=5):
    h = None
    ys = range(points)
    xs = list(ys)

    for i, coord in enumerate(seq):
        loc = i % points
        xs[loc], ys[loc] = coord

        if i == 1:
            h = abs(operator.sub(*xs[:2]))

        if i >= points:
            center = (i - (points/2)) % points
            x = xs[center]
            y = ys[center:] + ys[:center]
            try:
                yield (x, (-y[2] + (8 * y[1]) - (8 * y[-1]) + y[-2])/(12 * h))
            except TypeError:
                yield (x, None)
