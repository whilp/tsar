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
        if not line or line.endswith("RESTART"):
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
        "cswch/s": "context_switch_s",
        "kbbuffers": "mem_buffers_kb",
        "kbcached": "mem_cached_kb",
        "kbmemfree": "mem_free_kb",
        "kbmemused": "mem_used_kb",
        "kbswpfree": "swap_free_kb",
        "kbswpused": "swap_used_kb",
        "ldavg": "load_average",
        "plist-sz": "proc_list_size",
        "proc/s": "proc_s",
        "runq-sz": "runq_size",
        "rxbyt/s": "net_rx_byte_s",
        "rxdrop/s": "net_rx_drop_s",
        "rxerr/s": "net_rx_error_s",
        "rxmcst/s": "net_rx_multicast_s",
        "rxpck/s": "net_rx_packet_s",
        "txbyt/s": "net_tx_byte_s",
        "txdrop/s": "net_tx_drop_s",
        "txerr/s": "net_tx_error_s",
        "txmcst/s": "net_tx_multicast_s",
        "txpck/s": "net_tx_packet_s",
        "%dquot-sz": "disk_quota_entries_pct",
        "%rtsig-sz": "queued_rt_sigs_pct",
        "%steal": "cpu_steal_pct",
        "%super-sz": "super_block_handlers_pct",
        "access/s": "nfs_access_s",
        "badcall/s": "nfsd_error_s",
        "bread/s": "io_read_block_s",
        "brk/s": "tty_break_s",
        "bufpg/s": "mem_buffered_page_s",
        "bwrtn/s": "io_write_block_s",
        "call/s": "nfs_call_s",
        "campg/s": "mem_cached_page_s",
        "coll/s": "net_collision_s",
        "dentunusd": "fs_unused_cache",
        "dquot-sz": "fs_quota_entries",
        "fault/s": "page_fault_s",
        "file-sz": "fs_files",
        "framerr/s": "tty_frame_err_s",
        "frmpg/s": "mem_freed_page_s",
        "getatt/s": "nfs_getattr_s",
        "hit/s": "nfsd_cache_hit_s",
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
        "majflt/s": "process_major_fault_s",
        "miss/s": "nfsd_cache_miss_s",
        "ovrun/s": "tty_overrun_s",
        "packet/s": "nfsd_packet_s",
        "pgpgin/s": "page_in_kb",
        "pgpgout/s": "page_out_kb",
        "prtyerr/s": "tty_parity_err_s",
        "pswpin/s": "swap_in_page_s",
        "pswpout/s": "swap_out_page_s",
        "rawsck": "sockets_raw",
        "rcvin/s": "tty_receive_interrupt_s",
        "read/s": "nfs_read_s",
        "retrans/s": "nfs_retransmit_s",
        "rtps": "io_read_s",
        "rtsig-sz": "fs_queued_rts",
        "rxcmp/s": "net_rx_compressed_s",
        "rxfifo/s": "net_rx_fifo_s",
        "rxfram/s": "net_rx_frame_s",
        "saccess/s": "nfsd_access_s",
        "scall/s": "nfsd_request_s",
        "sgetatt/s": "nfsd_getatt_s",
        "sread/s": "nfsd_read_s",
        "super-sz": "fs_super_blocks",
        "swrite/s": "nfsd_write_s",
        "tcp/s": "nfsd_tcp_s",
        "tcpsck": "sockets_tcp",
        "totsck": "sockets",
        "tps": "io_transfer_s",
        "txcarr/s": "net_tx_carrier_s",
        "txcmp/s": "net_tx_compressed_s",
        "txfifo/s": "net_tx_fifo_s",
        "udp/s": "nfsd_udp_s",
        "udpsck": "sockets_udp",
        "write/s": "nfs_write_s",
        "wtps": "io_write_s",
        "xmtin/s": "tty_transmit_s",
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
