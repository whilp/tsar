#!/usr/bin/env python

#sadf -p /var/log/sa/sa* -- -c -n DEV -n EDEV -q -r -u -w

import csv
import os
import sys

from subprocess import PIPE, Popen

import cli

FIELDS = "subject attribute time value".split()
fieldattributes = {
    "%idle": "cpu_idle_percent",
    "%iowait": "cpu_iowait_percent",
    "%memused": "memory_used_percent",
    "%nice": "cpu_nice_percent",
    "%swpused": "swap_used_percent",
    "%system": "cpu_system_percent",
    "%user": "cpu_user_percent",
    "cswch/s": "context_switch_per_second",
    "kbbuffers": "memory_buffers_kb",
    "kbcached": "memory_cached_kb",
    "kbmemfree": "memory_free_kb",
    "kbmemused": "memory_used_kb",
    "kbswpfree": "swap_free_kb",
    "kbswpused": "swap_used_kb",
    "ldavg-1": "load_average_1_minute",
    "ldavg-15": "load_average_15_minutes",
    "ldavg-5": "load_average_5_minutes",
    "plist-sz": "process_list_size",
    "proc/s": "processes_per_second",
    "runq-sz": "runq_size",
    "rxbyt/s": "network_rx_bytes_per_second",
    "rxdrop/s": "network_rx_drop_per_second",
    "rxerr/s": "network_rx_errors_per_second",
    "rxmcst/s": "network_rx_multicast_per_second",
    "rxpck/s": "network_rx_packets_per_second",
    "txbyt/s": "network_tx_bytes_per_second",
    "txdrop/s": "network_tx_drop_per_second",
    "txerr/s": "network_tx_errors_per_second",
    "txmcst/s": "network_tx_multicast_per_second",
    "txpck/s": "network_tx_packets_per_second",
}

def sar2tsar(line):
    subject, interval, time, device, field, value = line.strip().split('\t')
    attribute = fieldattributes.get(field, None)
    if attribute is None:
        return

    return subject, "system_" + attribute, time, value

@cli.LoggingApp
def sartsar(app):
    writer = csv.writer(app.stdout)

    sadfcmd = "/usr/bin/sadf -p FILE -- -c -n DEV -n EDEV -q -r -u -w".split()

    for fname in app.params.files:
        sadfcmd[2] = fname
        sadf = Popen(sadfcmd, stdout=PIPE, stderr=PIPE)
        for line in sadf.stdout:
            row = sar2tsar(line)
            if row is None:
                continue
            csvwriter.writerow(row)

    app.stdout.flush()

sartsar.add_param("files", help="system activity files", nargs="+")
sartsar.add_param("-f", "--fieldnames", default=False,  action="store_true",
    help="print field names")

if __name__ == "__main__":
    sartsar.run()
