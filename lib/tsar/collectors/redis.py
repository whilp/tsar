#!/usr/bin/env python

from __future__ import absolute_import

import sys

import os

from redis import Redis

from . import helpers
from ..util import parsedsn

@helpers.Collector
def redis(app):
    dsn = parsedsn(app.params.dsn)
    del(dsn["username"])
    del(dsn["driver"])
    dsn["db"] = dsn.pop("database")
    db = Redis(**dsn)

    statkeys = "size resident share".split()

    t = app.now
    info = db.info()
    pid = info["process_id"]

    data = []
    subject = app.hostname
    data.append((subject, "redis_used_memory", t, info["used_memory"]))
    data.append((subject, "redis_keys", t, info.get("db0", {}).get("keys", 0)))

    t = app.now
    statcols = "size resident share text lib data dt".split()
    with open("/proc/%d/statm" % pid) as stat:
        statvals = stat.read()

    statdata = dict(zip(statcols, statvals.split()))
    for key in statkeys:
        data.append((subject, "redis_stat_%s" % key, t, statdata[key]))

    app.submit(data)

default_dsn = "redis://localhost:6379/0"
redis.add_param("-D", "--dsn", default=default_dsn,
    help="<driver>://<username>:<password>@<host>:<port>/<database> (default: %s)" % default_dsn)

if __name__ == "__main__":
    redis.run()
