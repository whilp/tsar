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
        "proc/s": "proc/s",
        "runq-sz": "runq_size",
        "rxbyt/s": "net_rx_byte/s",
        "rxdrop/s": "net_rx_drop/s",
        "rxerr/s": "net_rx_error/s",
        "rxmcst/s": "net_rx_multicast/s",
        "rxpck/s": "net_rx_packet/s",
        "txbyt/s": "net_tx_byte/s",
        "txdrop/s": "net_tx_drop/s",
        "txerr/s": "net_tx_error/s",
        "txmcst/s": "net_tx_multicast/s",
        "txpck/s": "net_tx_packet/s",
        "%dquot-sz": "disk_quota_entries_pct",
        "%rtsig-sz": "queued_rt_sigs_pct",
        "%steal": "cpu_steal_pct",
        "%super-sz": "super_block_handlers_pct",
        "access/s": "nfs_access/s",
        "badcall/s": "nfsd_error/s",
        "bread/s": "io_read_block/s",
        "brk/s": "tty_break/s",
        "bufpg/s": "mem_buffered_page/s",
        "bwrtn/s": "io_write_block/s",
        "call/s": "nfs_call/s",
        "campg/s": "mem_cached_page/s",
        "coll/s": "net_collision/s",
        "dentunusd": "fs_unused_cache",
        "dquot-sz": "fs_quota_entries",
        "fault/s": "page_fault/s",
        "file-sz": "fs_files",
        "framerr/s": "tty_frame_err/s",
        "frmpg/s": "mem_freed_page/s",
        "getatt/s": "nfs_getattr/s",
        "hit/s": "nfsd_cache_hit/s",
        #"i000/s": "",
        #"i001/s": "",
        #"i008/s": "",
        #"i009/s": "",
        #"i050/s": "",
        #"i082/s": "",
        #"i177/s": "",
        #"i233/s": "",
        "inode-sz": "fs_inodes",
        #"intr/s": "",
        "ip-frag": "socket_ip_frags",
        "kbswpcad": "swap_cached_kb",
        "ldavg-1": "load_avg_1",
        "ldavg-15": "load_avg_15",
        "ldavg-5": "load_avg_5",
        "majflt/s": "process_major_fault/s",
        "miss/s": "nfsd_cache_miss/s",
        "ovrun/s": "tty_overrun/s",
        "packet/s": "nfsd_packet/s",
        "pgpgin/s": "page_in_kb",
        "pgpgout/s": "page_out_kb",
        "prtyerr/s": "tty_parity_err/s",
        "pswpin/s": "swap_in_page/s",
        "pswpout/s": "swap_out_page/s",
        "rawsck": "sockets_raw",
        "rcvin/s": "tty_receive_interrupt/s",
        "read/s": "nfs_read/s",
        "retrans/s": "nfs_retransmit/s",
        "rtps": "io_read/s",
        "rtsig-sz": "fs_queued_rts",
        "rxcmp/s": "net_rx_compressed/s",
        "rxfifo/s": "net_rx_fifo/s",
        "rxfram/s": "net_rx_frame/s",
        "saccess/s": "nfsd_access/s",
        "scall/s": "nfsd_request/s",
        "sgetatt/s": "nfsd_getatt/s",
        "sread/s": "nfsd_read/s",
        "super-sz": "fs_super_blocks",
        "swrite/s": "nfsd_write/s",
        "tcp/s": "nfsd_tcp/s",
        "tcpsck": "sockets_tcp",
        "totsck": "sockets",
        "tps": "io_transfer/s",
        "txcarr/s": "net_tx_carrier/s",
        "txcmp/s": "net_tx_compressed/s",
        "txfifo/s": "net_tx_fifo/s",
        "udp/s": "nfsd_udp/s",
        "udpsck": "sockets_udp",
        "write/s": "nfs_write/s",
        "wtps": "io_write/s",
        "xmtin/s": "tty_transmit/s",
    }

    def main(self):
        fieldtoattr = self.fieldtoattr
        if self.params.fields:
            fields = [x for x in self.params.fields if x in "KEYS VALUES".split()]
            if fields:
                self.stdout.write("Available fields:\n")
                if "KEYS" in fields:
                    self.stdout.write('\n'.join(self.fieldtoattr) + '\n')
                if "VALUES" in fields:
                    self.stdout.write('\n'.join(self.fieldtoattr.values()) + '\n')
                return 0
            fields = self.params.fields
            fieldtoattr = dict((k, v) for k, v in self.fieldtoattr.items() if \
                k in fields or v in fields)

        cmd = self.params.sadfcmd
        records = []
        for fname in self.params.files:
            fullcmd = cmd.replace("<FILE>", fname)
            process, stdout, stderr = self.runcmd(fullcmd, abort=False, shell=True)

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
        default_sadf = "/usr/bin/sadf -p <FILE> -- -A"
        self.add_param("-c", "--sadfcmd", default=default_sadf,
            help="sadf command (default: %r)" % default_sadf)
        self.add_param("-f", "--fields", nargs="*", 
            help="fields to include; KEYS or VALUES to see choices (default: all fields)")
        self.add_param("-n", "--newer", default=None,
            help="only submit records newer than supplied UTC timestamp (default: all records)")
        self.add_param("files", help="system activity files", nargs="*")
