import functools
import operator

from io import StringIO

insert = lambda l, i, o: l[0:i] + [o] + l[i:]
incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)
median = lambda x: sorted(x)[len(x)/2]

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
