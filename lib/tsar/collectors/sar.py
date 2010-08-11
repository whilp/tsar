#!/usr/bin/env python

from itertools import chain, dropwhile
from operator import itemgetter

from . import helpers
from .commands import Collector

def parsesadf(output, fieldmap={}):
    keys = "subject interval timestamp device field value".split()
    nkeys = len(keys)
    for line in output:
        line = line.strip()
        if not line:
            continue
        data = dict(zip(keys, line.split(None, nkeys - 1)))
        data["attribute"] = fieldmap.get(data["field"], None)
        if data["attribute"] is None:
            continue
        data["timestamp"] = int(data["timestamp"])
        data["value"] = float(data["value"])
        yield data

class Sar(Collector):
    fieldtoattr = {
        "%idle": "cpu_idle_pct",
        "%iowait": "cpu_iowait_pct",
        "%memused": "mem_used_pct",
        "%nice": "cpu_nice_pct",
        "%swpused": "swap_used_pct",
        "%system": "cpu_system_pct",
        "%user": "cpu_user_pct",
        "cswch/s": "context_switch/s",
        "kbbuffers": "mem_buffers_kb",
        "kbcached": "mem_cached_kb",
        "kbmemfree": "mem_free_kb",
        "kbmemused": "mem_used_kb",
        "kbswpfree": "swap_free_kb",
        "kbswpused": "swap_used_kb",
        "ldavg": "load_average",
        "plist-sz": "proc_list_size",
        "proc/s": "procs/s",
        "runq-sz": "runq_size",
        "rxbyt/s": "net_rx_bytes/s",
        "rxdrop/s": "net_rx_drop/s",
        "rxerr/s": "net_rx_errors/s",
        "rxmcst/s": "net_rx_multicast/s",
        "rxpck/s": "net_rx_packets/s",
        "txbyt/s": "net_tx_bytes/s",
        "txdrop/s": "net_tx_drop/s",
        "txerr/s": "net_tx_errors/s",
        "txmcst/s": "net_tx_multicast/s",
        "txpck/s": "net_tx_packets/s",
    }

    @staticmethod
    def main(self):
        if self.params.fields:
            fields = self.params.fields.split(',')
            fieldtoattr = dict((k, v) for k, v in self.fieldtoattr.items() if \
                k in fields or v in fields)

        cmd = self.params.command
        records = []
        for fname in self.params.files:
            process = helpers.runcmd(cmd.replace("<FILE>", fname), shell=True)
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                continue

            stdout = iter(stdout.splitlines())
            records.append(parsesadf(stdout, fieldmap=fieldtoattr))

        keys = "subject attribute timestamp value".split()
        data = [[r.get(k) for k in keys] for r in chain(*records)]
        data.sort(key=itemgetter(2))

        if self.params.newer:
            newer = int(self.params.newer)
            old = lambda r: r[2] < newer
            data = dropwhile(old, data)
        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("sar", 
            help="sar(1) system monitor")
        default_sadf = "/usr/bin/sadf -p <FILE> -- -c -n DEV -n EDEV -q -r -u -w"
        self.add_param("-c", "--command", default=default_sadf,
            help="sadf command (default: %r)" % default_sadf)
        self.add_param("-f", "--fields", default="",
            help="comma-separated list of fields to include (default: all fields)")
        self.add_param("-n", "--newer", default=None,
            help="only submit records newer than supplied UTC timestamp (default: all records)")
        self.add_param("files", help="system activity files", nargs="*")
