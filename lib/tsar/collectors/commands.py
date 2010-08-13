import os
import socket
import subprocess
import time

from itertools import chain
from operator import itemgetter

from cli.app import Abort

from tsar import errors
from tsar.commands import ClientMixin, Command, SubCommand
from tsar.collectors.helpers import insert, median, replace

class Collect(ClientMixin, Command):
    collectors = {}
    service = "http://tsar.hep.wisc.edu/records"

    def __init__(self, main=None, timeout=600, killpg=True, parent=None, **kwargs):
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
        self.add_param("-n", "--dryrun", default=False, action="store_true",
            help="print collected data instead of submitting it (default: submit)")
        self.add_param("-g", "--groups", nargs="*", 
            help="list of aggregate groups to report to (default: none)")

        self.subparsers = self.argparser.add_subparsers(dest="collector")

        from tsar.collectors.afs_server import AFSServer
        from tsar.collectors.condor_queue import CondorQueue
        from tsar.collectors.dcache_latency import DcacheLatency
        from tsar.collectors.dcache_pnfsmanager import PnfsManager
        from tsar.collectors.dcache_transfers import DcacheTransfers
        from tsar.collectors.hddtemp import Hddtemp
        from tsar.collectors.redis import Redis
        from tsar.collectors.sar import Sar
        self.collectors = {
            "afs-server": AFSServer,
            "condor-queue": CondorQueue,
            "dcache-latency": DcacheLatency,
            "dcache-pnfsmanager": PnfsManager,
            "dcache-transfers": DcacheTransfers,
            "hddtemp": Hddtemp,
            "redis": Redis,
            "sar": Sar,
        }
        for k, v in sorted(self.collectors.items(), key=itemgetter(0)):
            collector = v(parent=self)
            self.collectors[k] = collector

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
        collector.exit_after_main = False
        try:
            returned = collector.run()
        finally:
            if oldhandler:
                signal.signal(signal.SIGALRM, oldhandler)

        if timeout:
            signal.alarm(0)

        if self.killpg:
            atexit._exithandlers.remove((cleanup, (), {}))

        return returned

class Collector(SubCommand):
    cfs = {
        "min": min,
        "max": max,
        "ave": median,
    }

    def pre_run(self):
        SubCommand.pre_run(self)
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
        processed = list(self.prepare(data, cfs=cfs))
        groupdata = []
        for group in (self.params.groups or []):
            groupdata.extend(replace(r, 0, group) for r in data)
        processed.extend(groupdata)
        nrecords = len(processed)
        self.log.debug("Collected %d records", nrecords)
        for record in processed:
            self.log.debug(' '.join(repr(r) for r in record))
            
        if not self.params.dryrun:
            self.log.debug("Submitting %d records", nrecords)
            return self.client.bulk(processed)

    def runcmd(self, cmd, expect=0, abort=True, **kwargs):
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        stdout, stderr = process.communicate()
        if expect is not False and process.returncode != expect:
            if not isinstance(cmd, basestring):
                cmd = ' '.join(cmd)
            self.log.warn("Command %r returned %d", cmd, process.returncode)
            self.stdout.write(stdout)
            self.stderr.write(stderr)
            if abort:
                raise Abort(process.returncode)

        return process, stdout, stderr

    now = property(lambda s: int(time.time()))
    hostname = property(lambda s: socket.gethostname())
