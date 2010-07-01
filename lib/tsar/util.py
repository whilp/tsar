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

