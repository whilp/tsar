import functools
import operator
import os
import socket
import subprocess
import time

import cli

from itertools import chain

from tsar import errors
from tsar.client import Tsar

insert = lambda l, i, o: l[0:i] + [o] + l[i:]
incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)
median = lambda x: sorted(x)[len(x)/2]

def trim(s, subs, reverse=False):
    find = s.find
    if reverse:
        find = s.rfind
    i = find(subs)

    if i < 0:
        start, end = 0, None
    elif reverse:
        start, end = 0, i
    else:
        start, end = len(subs), None

    return s[start:end]

rtrim = functools.partial(trim, reverse=True)

def runcmd(cmd, **kwargs):
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)

class Collector(cli.LoggingApp):
    service = "http://tsar.hep.wisc.edu/records"
    cfs = {
        "min": min,
        "max": max,
        "ave": median,
    }

    def __init__(self, main=None, timeout=30, killpg=True, **kwargs):
        self.timeout = timeout
        self.killpg = killpg
        super(Collector, self).__init__(main, **kwargs)

    @property
    def name(self):
        return "tsar-collect-%s" % super(Collector, self).name

    def setup(self):
        super(Collector, self).setup()
        self.add_param("-S", "--service", default=self.service,
            help="service URL (default: %s)" % self.service)
        self.add_param("-t", "--timeout", default=self.timeout,
            help="timeout (default: %s seconds)" % self.timeout)

    def pre_run(self):
        super(Collector, self).pre_run()
        self.tsar = Tsar(self.params.service)

    def run(self):
        self.pre_run()

        # Set a signal in case main() takes too long.
        self.params.timeout = int(self.params.timeout)
        timeout = self.params.timeout > 0

        cleanup = None
        if self.killpg:
            import atexit
            import signal
            cleanup = atexit.register(lambda: os.killpg(os.getpgid(os.getpid()), signal.SIGKILL))

        oldhandler = None
        if timeout:
            import signal
            def handle_timeout(signum, frame):
                raise errors.TimeoutError("main() exceeded timeout %s", self.params.timeout)
            oldhandler = signal.signal(signal.SIGALRM, handle_timeout)
            signal.alarm(self.params.timeout)

        try:
            returned = self.main(self)
        finally:
            if oldhandler:
                signal.signal(signal.SIGALRM, oldhandler)

        if timeout:
            signal.alarm(0)

        if self.killpg:
            atexit._exithandlers.remove((cleanup, (), {}))

        return self.post_run(returned)

    def prepare(self, data, cfs=None):
        if cfs is None:
            cfs = self.cfs

        expand = lambda r: (insert(r, 2, cf) for cf in cfs)
        records = chain(*[len(r) == 5 and (r,) or expand(list(r)) for r in data])

        for record in records:
            value = record[4]
            if not isinstance(value, (int, float)):
                record[4] = cfs[record[2]](value)

            yield record

    def submit(self, data, cfs=None):
        return self.tsar.bulk(self.prepare(data, cfs=cfs))

    now = property(lambda s: int(time.time()))
    hostname = property(lambda s: socket.gethostname())
