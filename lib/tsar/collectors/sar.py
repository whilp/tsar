#!/usr/bin/env python

import subprocess

from itertools import chain, dropwhile
from operator import itemgetter

from .helpers import Collector

median = lambda x: sorted(x)[len(x)/2]

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

def run(cmd, **kwargs):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        **kwargs)

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

@Collector
def sar(app):
    global fieldtoattr

    if app.params.fields:
        fields = app.params.fields.split(',')
        fieldtoattr = dict((k, v) for k, v in fieldtoattr.items() if \
            k in fields or v in fields)

    cmd = app.params.command
    records = []
    for fname in app.params.files:
        process = run(cmd.replace("<FILE>", fname), shell=True)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            continue

        stdout = iter(stdout.splitlines())
        records.append(parsesadf(stdout, fieldmap=fieldtoattr))

    keys = "subject attribute timestamp value".split()
    data = [tuple([r.get(k) for k in keys]) for r in chain(*records)]
    data.sort(key=itemgetter(2))

    if app.params.newer:
        newer = int(app.params.newer)
        old = lambda r: r[2] < newer
        data = dropwhile(old, data)
    data = list(app.prepare(data))
    if data:
        app.tsar.bulk(data)

default_sadf = "/usr/bin/sadf -p <FILE> -- -c -n DEV -n EDEV -q -r -u -w"
sar.add_param("-c", "--command", default=default_sadf,
    help="sadf command (default: %r)" % default_sadf)
sar.add_param("-f", "--fields", default="",
    help="comma-separated list of fields to include (default: all fields)")
sar.add_param("-n", "--newer", default=None,
    help="only submit records newer than supplied UTC timestamp (default: all records)")
sar.add_param("files", help="system activity files", nargs="*")

if __name__ == "__main__":
    sar.run()
