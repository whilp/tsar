#!/usr/bin/env python

import cli
import os
import sys

from bz2 import BZ2File
from calendar import timegm
from csv import writer
from time import strptime

FIELDS="subject attribute time value".split()
OPENERS = {
    ".bz2": BZ2File,
}

def smartopen(fname, *args, **kwargs):
    root, ext = os.path.splitext(fname)
    opener = OPENERS.get(ext, file)
    return opener(fname, *args, **kwargs)

def report(reporter, fname, subject=None, attribute=None, out=sys.stdout, fields=FIELDS):
    csvwriter = writer(out)
    csvwriter.writerow(fields)

    csvwriter.writerow(reporter(fname, subject, attribute, fields))

def zeroreps(fname):
    attribute = "unreplicated_files"
    f = smartopen(fname, 'r')
    root, _ext = os.path.splitext(fname)
    junk, _junk, time = root.partition('-')

    time = timegm(strptime(time, "%Y%m%d-%H:%M"))
    value = len([x for x in f if x.startswith('/')])
    return (attribute, time, value)

def timefromlog(logline):
    try:
        time, junk, rest = logline.partition('.')
        return timegm(strptime(time, "%m/%d/%y %H:%M:%S"))
    except ValueError:
        pass

def runtime(fname):
    attribute = "runtime_seconds"
    f = smartopen(fname, 'r')
    start = timefromlog(f.readline())
    end = None

    increment = 32
    chunk = ''
    while end is None:
        increment *= 2
        f.seek(-increment, 2)
        chunk = f.read()
        lines = reversed(chunk.splitlines())
        for line in lines:
            end = timefromlog(line)
            if end is not None:
                break

    value = end - start

    return (attribute, start, value)

@cli.LoggingApp
def tsarbulk(app):
    csvwriter = writer(app.stdout)
    if app.params.fieldnames:
        csvwriter.writerow(FIELDS)

    subject = "dcache_pfm"
    for fname in app.params.fnames:
        app.stderr.write("Analyzing %s\n" % fname)
        processor = None
        if fname.startswith("pfm_logfile"):
            processor = runtime
        elif fname.startswith("zero_replica_files"):
            processor = zeroreps
        else:
            app.log.debug("No processor found for file '%s'", fname)
        attribute, time, value = processor(fname)
        row = [locals()[x] for x in FIELDS]
        csvwriter.writerow(row)
    
    app.stdout.flush()
        
tsarbulk.add_param("-f", "--fieldnames", default=False,
    help="print field names")
tsarbulk.add_param("fnames", nargs="+")

if __name__ == "__main__":
    tsarbulk.run()
