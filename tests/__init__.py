import logging
import sys

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from webob import Request, Response

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

class BaseTest(unittest.TestCase):
    pass

class AppTest(BaseTest):
    application = None
    
    def app(self, *args, **kwargs):
        req = Request.blank(*args, **kwargs)

        return req.get_response(self.application)

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
