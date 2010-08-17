import functools
import operator

from io import StringIO

def replace(seq, i, other, width=None):
    if not isinstance(other, list):
        other = [other]
    if width is None:
        width = len(other)
    return seq[0:i] + other + seq[i + width:]
insert = functools.partial(replace, width=0)

incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)
appendkey = lambda d, k, v: operator.setitem(d, k, d.setdefault(k, []) + [v])
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
