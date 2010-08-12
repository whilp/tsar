#!/usr/bin/env python

from .commands import Collector
from . import commands, helpers

class AFSServer(Collector):

    @staticmethod
    def main(self):
        server = self.params.server[0]
        cmd = ["/usr/sbin/rxdebug", server]

        t = self.now
        process = self.runcmd(cmd)
        stdout, stderr = process.communicate()

        rxdata = {}
        for line in stdout.splitlines():
            if line.startswith("Connection from"):
                helpers.incrkey(rxdata, "afs_connections")
            elif "waiting_for_process" in line:
                helpers.incrkey(rxdata, "blocked_afs_connections")

        data = []
        subject = server
        data.append((subject, "afs_connections", t, rxdata.get("afs_connections", 0)))
        data.append((subject, "blocked_afs_connections", t, 
            rxdata.get("blocked_afs_connections", 0)))

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("afs-server", 
            help="AFS server")
        self.add_param("server", nargs=1, help="AFS server name")
