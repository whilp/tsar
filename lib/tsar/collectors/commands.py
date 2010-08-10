import os
import socket
import time

import cli

from itertools import chain
from operator import itemgetter

from tsar import errors
from tsar.commands import ClientMixin, Command, SubCommand
from tsar.collectors.helpers import insert, median

class Collect(ClientMixin, Command):
    collectors = {}
    service = "http://tsar.hep.wisc.edu/records"

    def __init__(self, main=None, timeout=30, killpg=True, parent=None, **kwargs):
        self.timeout = timeout
        self.killpg = killpg
        self.parent = parent
        super(Collect, self).__init__(main, **kwargs)

    def setup(self):
        Command.setup(self)
        self.argparser = self.parent.subparsers.add_parser("collect", 
            help="collect and submit data for a tsar service")
        ClientMixin.setup(self)

        self.add_param("-t", "--timeout", default=self.timeout,
            help="timeout (default: %s seconds)" % self.timeout)

        self.subparsers = self.argparser.add_subparsers(dest="collector")
        from tsar.collectors.afs_server import Afs_Server
        self.collectors = {
            "afs-server": Afs_Server,
        }
        for k, v in sorted(self.collectors.items(), key=itemgetter(0)):
            collector = v(parent=self)
            collector.setup()
            self.collectors[k] = collector

    @staticmethod
    def main(self):
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

        collector = self.collectors[self.params.collector]
        collector.params = self.params
        try:
            collector.run()
        finally:
            if oldhandler:
                signal.signal(signal.SIGALRM, oldhandler)

        if timeout:
            signal.alarm(0)

        if self.killpg:
            atexit._exithandlers.remove((cleanup, (), {}))

class Collector(SubCommand):
    cfs = {
        "min": min,
        "max": max,
        "ave": median,
    }

    def pre_run(self):
        self.client = self.parent.client

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
        return self.client.bulk(self.prepare(data, cfs=cfs))

    now = property(lambda s: int(time.time()))
    hostname = property(lambda s: socket.gethostname())
