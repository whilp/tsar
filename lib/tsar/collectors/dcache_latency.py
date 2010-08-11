#!/usr/bin/env python

import os
import time

from tempfile import mktemp, mkstemp

from . import helpers
from .commands import Collector

def localprefix(proto, path):
    if proto in ("srm", "gsiftp"):
        path = "file:///%s" % path

    return path

class DcacheLatency(Collector):
    copiers = {
        "dcap": "dccp %s %s",
        "srm": "srmcp -2 -retry_num=0 %s %s",
        "gsiftp": "globus-url-copy -nodcau %s %s",
    }
    
    def __init__(self, main=None, timeout=600, **kwargs):
        super(DcacheLatency, self).__init__(main=main, **kwargs)
        self.timeout = timeout

    def timecp(proto, src, dst):
        copier = self.copiers[proto]

        start = time.time()
        process = helpers.runcmd(copier % (src, dst), shell=True)
        process.wait()
        stop = time.time()

        duration = stop - start
        return process, duration

    @staticmethod
    def main(self):
        srcfd, src = mkstemp(prefix="dcache-latency-testfile.")
        srcfile = os.fdopen(srcfd, 'a')
        srcfile.write("dcache-latency-testfile\n")
        srcfile.close()

        data = []
        dst = mktemp(prefix="dcache-latency-testfile.", dir=self.params.destination)
        try:
            for proto in self.params.protos:
                t = self.now
                prefix = getattr(self.params, proto)
                process, duration = self.timecp(proto, localprefix(proto, src), 
                    "%s%s.%s" % (prefix, dst, proto))
                if process.returncode == 0:
                    data.append(("dcache", "%s_write_latency" % proto, t, duration))
                else:
                    self.log.warn("%s write transfer failed", proto)
        finally:
            os.remove(src)

        for proto in self.params.protos:
            t = self.now
            prefix = getattr(self.params, proto)
            process, duration = self.timecp(proto, "%s%s" % (prefix, self.params.test_file),
                localprefix(proto, os.devnull))
            if process.returncode == 0:
                data.append(("dcache", "%s_read_latency" % proto, t, duration))
            else:
                self.log.warn("%s read transfer failed", proto)

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("dcache-latency", 
            help="per-protocol dCache transfer latency (read and write)")

        default_testfile = "/pnfs/hep.wisc.edu/cmsprod/latency-test/testfile"
        default_destination = "/pnfs/hep.wisc.edu/cmsprod/latency-test"
        default_srm = "srm://cmssrm.hep.wisc.edu:8443/srm/managerv2?SFN="
        default_dcap = ""
        default_gsiftp = "gsiftp://cmsgridftp.hep.wisc.edu:2811"

        self.add_param("-T", "--test-file", default=default_testfile,
            help="Remote test file (default: %s)" % default_testfile)
        self.add_param("-d", "--destination", default=default_destination,
            help="Destination directory (default: %s)" % default_destination)
        self.add_param("--srm", default=default_srm,
            help="SRM base URL (default: %s)" % default_srm)
        self.add_param("--dcap", default=default_dcap,
            help="dcap base URL (default: %s)" % default_dcap)
        self.add_param("--gsiftp", default=default_gsiftp,
            help="gsiftp base URL (default: %s)" % default_gsiftp)
        self.add_param("protos", nargs="+", default=[],
            choices=self.copiers.keys(), help="Protocols to test (default: none)")
