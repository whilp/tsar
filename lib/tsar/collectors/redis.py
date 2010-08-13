#!/usr/bin/env python

from __future__ import absolute_import

import sys

import os

import redis

from . import helpers
from .commands import Collector
from ..commands import DBMixin
from ..util import parsedsn

class Redis(DBMixin, Collector):

    def main(self):
        statkeys = "size resident share".split()

        t = self.now
        info = self.db.info()
        pid = info["process_id"]

        data = []
        subject = self.hostname
        data.append((subject, "redis_used_memory", t, info["used_memory"]))
        data.append((subject, "redis_keys", t, info.get("db0", {}).get("keys", 0)))

        t = self.now
        statcols = "size resident share text lib data dt".split()
        with open("/proc/%d/statm" % pid) as stat:
            statvals = stat.read()

        statdata = dict(zip(statcols, statvals.split()))
        for key in statkeys:
            data.append((subject, "redis_stat_%s" % key, t, statdata[key]))

        self.submit(data)

    def pre_run(self):
        Collector.pre_run(self)
        DBMixin.pre_run(self)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("redis", 
            help="redis database server")
        DBMixin.setup(self)

if __name__ == "__main__":
    redis.run()
