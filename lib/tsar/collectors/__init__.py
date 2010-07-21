import socket
import time

import cli

from tsar import errors
from tsar.client import Tsar

class Collector(cli.LoggingApp):
    service = "http://tsar.hep.wisc.edu/records"

    def __init__(self, main=None, timeout=30, name=None, **kwargs):
        self.timeout = timeout
        super(Collector, self).__init__(self, main, **kwargs)

    @property
    def name(self):
        name = self._name
        if name is None:
            name = getattr(self.main, 'func_name', self.main.__name__)
        return "tsar-collect-%s" % name

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

        return self.post_run(returned)

    now = property(lambda s: int(time.time()))
    hostname = property(lambda s: socket.gethostname())

    def prepare(self, data, cfs=["min", "max", "ave"]):
        for record in data:
            for cf in cfs:
                yield record[:2] + (cf,) + record[2:]
