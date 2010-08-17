import grp
import logging
import os
import pwd
import sys
import time

from functools import partial

from . import helpers
from .commands import Collector

def fields(string, keys, delim=None, types=str):
    """Split *string* and zip its fields with *keys*, yielding two-tuples.

    *delim* is used to split the string. *types* may be a converter callable or
    a dictionary mapping keys to converter callables. A converter should take a
    value as its only argument and return the converted value.
    """
    convert = types
    if isinstance(types, dict):
        convert = lambda v: types.get(k, lambda x: x)(v)

    for i, v in enumerate(string.split(delim)):
        k = keys[i]
        yield k, convert(v)

# Handlers.
file_nr = partial(fields, keys="allocated free max".split(), types=int)
inode_state = partial(fields,
    keys="nr_inodes nr_free_inodes preshrink "
        "dummy1 dummy2 dummy3 dummy4".split(), types=int)

def diskstats(string):
    keys = "major minor reads-completed reads-merged " \
        "sectors-read ms-reading writes-completed writes-merged " \
        "sectors-written ms-writing current-ios ms-ios weighted-ms-ios".split()

    for line in string.splitlines():
        values = line.split()
        dev = values.pop(2)
        for i, v in enumerate(values):
            key = '.'.join((dev, keys[i]))
            yield key, int(v)

def loadavg(string):
    results = {}
    keys = "1min 5min 15min entities lastpid".split()
    types=dict((x, float) for x in keys if x != "entities")
    types["lastpid"] = int
    result = dict(fields(string, keys=keys, types=types))

    executing, entities = result["entities"].split('/')
    result["executing"] = int(executing)
    result["entities"] = int(entities)

    return result.items()

def meminfo(string):
    delim = ':'
    for line in string.splitlines():
        k, v = [x.strip() for x in line.split()[:2]]
        k = k.lower().rstrip(':')
        v = int(v)
        yield k, v

def netdev(string):
    keys = []
    rx = "bytes packets errs drop fifo".split()
    tx = rx + "colls carrier compressed".split()
    rx.extend("frame compressed multicast".split())
    keys.extend("rx-" + x for x in rx)
    keys.extend("tx-" + x for x in tx)

    # Skip the first two lines, which are headers.
    for line in string.splitlines()[2:]:
        dev, rest = line.split(':', 1)
        dev = dev.strip()
        for i, v in enumerate(rest.split()):
            key = '.'.join((dev, keys[i]))
            yield key, int(v)

def slabinfo(string):
    # '_' keys are useless; skip 'em.
    keys = "active_objs num_objs objsize objperslab pagesperslab _ _ limit " \
        "batchcount sharedfactor _ _ active_slabs num_slabs sharedavail".split()

    # Skip header.
    for line in string.splitlines()[2:]:
        values = line.split()
        name = values.pop(0)
        for i, v in enumerate(values):
            key = keys[i]
            if key == "_": continue
            key = '.'.join((name, key))
            yield key, int(v)

def stat(string):
    for line in string.splitlines():
        values = line.split()
        keys = "user nice system idle _ iowait irq softirq".split()
        name = values.pop(0)
        if name.startswith("cpu"):
            for i, v in enumerate(values):
                key = keys[i]
                if key == "_": continue
                yield '.'.join((name, key)), int(v)
        elif name == "intr":
            pass
        else:
            yield name, int(v)

def swaps(string):
    keys = "filename type size used priority".split()
    types = {"filename": str, "type": str, "size": int, "used": int, "priority": int}
    # Skip header.
    for line in string.splitlines()[1:]:
        results = dict(fields(line, keys=keys, types=types))
        filename = results.pop("filename")
        for k, v in results.items():
            yield '.'.join((filename, k)), v

def vmstat(string):
    for line in string.splitlines():
        k, v = line.split()
        yield k, int(v)

class Proc(Collector):
    fds = {}
    state = {}
    interval = 180
    handlers = {
        "/proc/sys/fs/inode-state": inode_state,
        "/proc/sys/fs/file-nr": file_nr,
        "/proc/diskstats": diskstats,
        "/proc/interrupts": None, # TODO
        "/proc/loadavg": loadavg,
        "/proc/meminfo": meminfo,
        "/proc/net/dev": netdev,
        "/proc/slabinfo": slabinfo,
        "/proc/stat": stat,
        "/proc/swaps": swaps,
        "/proc/sys/kernel/pty/nr": lambda x: [("number", int(x))],
        "/proc/vmstat": vmstat,
    }

    def open(self, fname):
        fd = self.fds.get(fname, None)
        if fd is None:
            self.log.debug("Opening %s", fname)
            fd = open(fname, 'r')
            self.fds[fname] = fd

        return fd

    def read(self, fname):
        f = self.open(fname)
        f.seek(0)
        return f.read()

    def dispatch(self, fname):
        handler = self.handlers.get(fname, None)
        if not callable(handler):
            self.log.debug("No handler for %s", fname)
            return

        self.log.debug("Handling data from %s", fname)
        for k, v in handler(self.read(fname)):
            key = '!'.join((fname, k))
            last = self.state.get(key, None)
            if last != v:
                self.state[key] = v
                self.log.info("%s %s %s", fname, k, v)

    def cycle(self):
        for fname in self.fds:
            self.dispatch(fname)
        
    def loop(self):
        while True:
            self.log.debug("Beginning monitoring cycle")
            self.cycle()

            self.log.debug("Monitoring cycle complete; sleeping %d seconds",
                self.interval)
            time.sleep(self.interval)

    def start(self):
        pass

    def stop(self):
        pass

    def restart(self):
        self.stop()
        self.start()

    actions = {
        "start": start,
        "stop": stop,
        "restart": restart,
    }

    def main(self):
		action = self.actions[self.params.action[0]]
        action()

        # Open pidfile so we can write to it after we daemonize.
        for fname in self.handlers:
            self.open(fname)

        if self.params.daemonize:
            self.daemonize()

            try:
                self.loop()
            except (KeyboardInterrupt, SystemExit):
                reutrn 0
        else:
            return self.cycle()

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("proc", 
            help="Linux proc(5) monitor")

        self.add_param("action", default="start", choices=self.actions,
            help="action to take")
