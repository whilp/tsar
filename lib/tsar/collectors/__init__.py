import socket
import time

import cli

from tsar.client import Tsar

class Collector(cli.LoggingApp):
    service = "http://tsar.hep.wisc.edu/records"

    def __init__(self, *args, **kwargs):
        super(Collector, self).__init__(*args, **kwargs)
        self._name = "tsar-collector-%s" % self._name

    def setup(self):
        super(Collector, self).setup()
        self.add_param("-S", "--service", default=self.service,
            help="service URL (default: %s)" % self.service)

    def pre_run(self):
        super(Collector, self).pre_run()
        self.tsar = Tsar(self.params.service)

    now = property(lambda s: int(time.time()))
    hostname = property(lambda s: socket.gethostname())

    def prepare(self, data, cfs=["min", "max", "ave"]):
        for record in data:
            for cf in cfs:
                yield record[:2] + (cf,) + record[2:]
