import re
import time

import cli

from itertools import chain

from . import model
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

class SubApp(cli.LoggingApp):
    
    def pre_run(self):
        pass

@SubApp
def last(app):
    now = time.time()
    last = lastkeys(app.db)
    last.sort(key=lambda x:x[1][0])

    app.stdout.write("===> Found %d records\n" % len(last))
    headers = ("AGE", "VALUE", "RECORD")
    format = "%-8s %-12s %s\n"
    app.stdout.write(format % headers)
    for key, val in last:
        lasttime, lastval, i = val
        app.stdout.write(format % (dtos(now - lasttime), "%g" % lastval, key))

@SubApp
def clean(app):
    pattern = re.compile(app.params.pattern[0])
    for record in model.Records.all():
        key = record.subkey("")
        if pattern.match(key):
            app.stdout.write("%s*\n" % key)
            if not app.params.dryrun:
                record.delete()

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

manage.commands = {
    "last": last,
    "clean": clean,
}

default_dsn = "redis://localhost:6379/0"
manage.add_param("-D", "--dsn", default=default_dsn,
    help="<driver>://<username>:<password>@<host>:<port>/<database> (default: %s)" % default_dsn)

subparsers = manage.argparser.add_subparsers(dest="command")
last.argparser = subparsers.add_parser("last", help="list database keys")
clean.argparser = subparsers.add_parser("clean", help="remove keys")
clean.add_param("-n", "--dryrun", default=False, action="store_true",
    help="don't actually remove records")
clean.add_param("pattern", nargs=1, help="regular expression to match subkeys against")

if __name__ == "__main__":
    manage.run()
