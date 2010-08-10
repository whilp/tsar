import re
import time

import cli

from itertools import chain

from . import model
from .commands import SubCommand
from .util import nearest, parsedsn

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

@cli.LoggingApp
def manage(app):
    dsn = parsedsn(app.params.dsn)
    del(dsn["username"])
    del(dsn["driver"])
    dsn["db"] = dsn.pop("database")
    model.db = model.connect(**dsn)
    cmd = app.commands[app.params.command]
    cmd.db = model.db
    cmd.params = app.params
    cmd.run()

default_dsn = "redis://localhost:6379/0"
manage.add_param("-D", "--dsn", default=default_dsn,
    help="<driver>://<username>:<password>@<host>:<port>/<database> (default: %s)" % default_dsn)
class Last(SubCommand):
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
        super(Last, self).setup()
        self.argparser = self.parent.subparsers.add_parser("last", 
            help="list database keys from oldest to newest")
        self.add_param("-r", "--reverse", default=False, action="store_true",
            help="reverse sort")
        self.add_param("pattern", nargs="?", default=".*", 
            help="regular expression to match subkeys against")

class Clean(SubCommand):
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
        super(Clean, self).setup()
        self.argparser = self.parent.subparsers.add_parser("clean", 
            help="remove keys")
        self.add_param("-n", "--dryrun", default=False, action="store_true",
            help="don't actually remove records")
        self.add_param("pattern", nargs=1, help="regular expression to match subkeys against")

if __name__ == "__main__":
    manage.run()
