import math
import time

from datetime import datetime
from functools import partial

import redis

from tsar.model import Records, consolidate, nearest

db = redis.Redis(db=1)

records = Records(db, "foo", "bar")

now = 1278007023
now = 1278007837

def loop(f, count=100, before=None, after=None):
    mintime = -1
    i = 0
    while i < count:
        i += 1
        if before is not None: before()
        start = time.time()
        f()
        stop = time.time()
        if after is not None: after()
        runtime = stop - start
        if mintime < 0 or runtime < mintime:
            mintime = runtime
    return mintime

cleanloop = partial(loop, before=db.flushdb, after=db.flushdb)

interval = 300
stop = now + (500 * interval)
data = [(x, abs(int(100 * math.cos(x)))) for x in \
    range(now, stop + (3 * interval), 3 * interval)]


ttod = records.types.Time.todatetime
print "Generating data from %s to %s" % (ttod(now), ttod(stop))

#print "append: %g" % cleanloop(lambda : records.append((now, 12)))
#print "extend: %g" % cleanloop(lambda : records.extend(data), count=10)

print ttod(data[0][0]), ttod(data[-1][0])

db.flushdb()
#records.append((datetime.now(), 10))
records.extend(data)
#print "start monitor"; time.sleep(5)
q1, q2 =  stop - (400 * interval), stop
print "Querying from %s to %s" % (ttod(q1), ttod(q2))
result = [(str(ttod(x)), y) for x,y  in list(records.query(q1, q2))]
print len(result), result[0], result[-1]
