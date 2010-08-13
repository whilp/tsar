#!/usr/bin/env python

from .commands import Collector

class Hddtemp(Collector):

    def main(self):
        cmd = self.params.hddtempcmd.split()
        cmd.extend(self.params.disks)
        t = self.now
        process, stdout, stderr = self.runcmd(cmd, abort=False)
        temps = [int(l.strip()) for l in stdout.splitlines()]

        self.submit([(self.hostname, "disk_temp_c", t, temps)])

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("hddtemp", 
            help="hddtemp(8) disk monitor")
        default_hddtemp = "/usr/sbin/hddtemp -u C -n"
        self.add_param("-c", "--hddtempcmd", default=default_hddtemp,
            help="hddtemp command (default: %r)" % default_hddtemp)
        self.add_param("disks", nargs="+", 
            help="disks to monitor")
