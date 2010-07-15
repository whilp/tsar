#!/usr/bin/env python

import os
import socket
import sys
import subprocess
import time

import cli
import tsar

now = lambda: int(time.time())

def prepare(data, cfs=["last"]):
    for record in data:
        for cf in cfs:
            yield record[:2] + (cf,) + record[2:]


class Command(subprocess.Popen):
    defaults = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }
    
    def __init__(self, *args, **kwargs):
        _kwargs = self.defaults.copy()
        _kwargs.update(kwargs)
        super(Command, self).__init__(*args, **kwargs)

@cli.LoggingApp
def collect_redis(app):
    rediscmd = "/cms/sw/redis/bin/redis-cli info".split()
    redis = Command(rediscmd)

    name = socket.gethostname() 
    prefix = "redis_"
    data = []
    # key name: converter
    rediskeys = "used_memory".split()
    statkeys = "size resident share".split()

    pid = None
    t = now
    for line in redis.stdout:
        key, value = line.partition(':')
        converter = rediskeys.get(key, None)
        if key is "process_id":
            pid = int(value)
        elif key in rediskeys:
            data.append((name, prefix + key, t, value))

    if pid is not None:
        t = int(time.time())
        statcols = "size resident share text lib data dt".split()
        with open("/proc/%d/statm" % pid) as stat:
            statvals = stat.read()

        statdata = dict(zip(statcols, statvals.split()))
        for key in statkeys:
            data.append((name, "%sstat_%s" % key, t, value))

    data = prepare(data, "min max".split())
    tsar.bulk(data)

if __name__ == "__main__":
    collect_redis.run()
