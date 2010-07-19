#!/usr/bin/env python

import os
import socket
import sys
import time

import cli
import redis

from tsar.client import Tsar

now = lambda: int(time.time())

def prepare(data, cfs=["last"]):
    for record in data:
        for cf in cfs:
            yield record[:2] + (cf,) + record[2:]

@cli.LoggingApp
def collect_redis(app):
    db = redis.Redis()

    name = socket.gethostname() 
    statkeys = "size resident share".split()
    data = []

    t = now()
    info = db.info()
    pid = info["process_id"]
    data.append((name, "redis_used_memory", t, info["used_memory"]))


    t = now()
    statcols = "size resident share text lib data dt".split()
    with open("/proc/%d/statm" % pid) as stat:
        statvals = stat.read()

    statdata = dict(zip(statcols, statvals.split()))
    for key in statkeys:
        data.append((name, "redis_stat_%s" % key, t, statdata[key]))

    data = prepare(data, "min max ave".split())
    tsar = Tsar("http://g13n01.hep.wisc.edu:8080/records")
    tsar.bulk(data)

if __name__ == "__main__":
    collect_redis.run()
