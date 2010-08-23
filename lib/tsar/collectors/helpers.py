import functools
import operator

from io import StringIO

from ..util import rtrim, trim

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
pct = lambda x, y: y != 0 and (100.0 * x)/y or 0.0

def intorfloat(string):
    try:
        return int(string)
    except ValueError:
        return float(string)
