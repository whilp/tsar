import re
import time

import cli

from itertools import chain

from . import model
from .commands import DBMixin, SubCommand
from .util import Decorator, nearest

def intorfloat(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return float(value)

def dtos(delta):
    units = [
        ("day", 24 * 60 * 60),
        ("hour", 60 * 60),
        ("minute", 60),
        ("second", 1),
    ]

    delta = int(delta)
    lasti = len(units)
    for i, unit in enumerate(units):
        identifier, n = unit
        count = delta / n
        if i < lasti and count == 0:
            continue
        count = nearest(delta, n)/n
        return "%d %s%s" % (count, identifier, count != 1 and 's' or '')

def lastkeys(db):
    split = lambda x: tuple(x.split())
    records = db.smembers("records")
    lkeys = ["records!%s!%s!%s!60!last" % split(x) for x in records]
    last = zip(records, [v.split() for v in db.mget(lkeys)])
    return [(k, [intorfloat(x) for x in v]) for k, v in last]


class Last(DBMixin, SubCommand):
    name = "last"

    @staticmethod
    def main(self):
        now = time.time()
        pattern = re.compile(self.params.pattern)
        last = lastkeys(self.db)
        last = [(k, v) for k, v in last if pattern.match(k)]
        last.sort(key=lambda x:x[1][0], reverse=self.params.reverse)

        headers = ("#AGE", "VALUE", "RECORD")
        format = "%-12s %-12s %s\n"
        self.stdout.write(format % headers)
        for key, val in last:
            lasttime, lastval, i = val
            self.stdout.write(format % (dtos(now - lasttime), "%g" % lastval, key))

    def setup(self):
        SubCommand.setup(self)
        self.argparser = self.parent.subparsers.add_parser("last", 
            help="list database keys from oldest to newest")
        DBMixin.setup(self)

        self.add_param("-r", "--reverse", default=False, action="store_true",
            help="reverse sort")
        self.add_param("pattern", nargs="?", default=".*", 
            help="regular expression to match subkeys against")

class Clean(DBMixin, SubCommand):
    name = "clean"
    
    @staticmethod
    def main(self):
        pattern = re.compile(self.params.pattern[0])
        for record in model.Records.all():
            key = record.subkey("")
            if pattern.match(key):
                self.stdout.write("%s*\n" % key)
                if not self.params.dryrun:
                    record.delete()

    def setup(self):
        SubCommand.setup(self)
        self.argparser = self.parent.subparsers.add_parser("clean", 
            help="remove keys")
        DBMixin.setup(self)

        self.add_param("-n", "--dryrun", default=False, action="store_true",
            help="don't actually remove records")
        self.add_param("pattern", nargs=1, help="regular expression to match subkeys against")
