#!/usr/bin/env python

import os
import subprocess
import time

from tempfile import mktemp, mkstemp

from . import Collector

def run(cmd):
    return subprocess.Popen(cmd, shell=True, 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

copiers = {
    "dcap": "dccp %s %s",
    "srm": "srmcp -2 -retry_num=0 %s %s",
    "gsiftp": "globus-url-copy -nodcau %s %s",
}

def timecp(proto, src, dst):
    global copiers
    copier = copiers[proto]

    start = time.time()
    process = run(copier % (src, dst))
    process.wait()
    stop = time.time()

    duration = stop - start
    return process, duration

def localprefix(proto, path):
    if proto in ("srm", "gsiftp"):
        path = "file:///%s" % path

    return path

@Collector(timeout=300)
def dcache_latency(app):
    srcfd, src = mkstemp(prefix="dcache-latency-testfile.")
    srcfile = os.fdopen(srcfd, 'a')
    srcfile.write("dcache-latency-testfile\n")
    srcfile.close()

    data = []
    dst = mktemp(prefix="dcache-latency-testfile.", dir=app.params.destination)
    try:
        for proto in app.params.protos:
            t = app.now
            prefix = getattr(app.params, proto)
            process, duration = timecp(proto, localprefix(proto, src), 
                "%s%s.%s" % (prefix, dst, proto))
            if process.returncode == 0:
                data.append(("dcache", "%s_write_latency" % proto, t, duration))
            else:
                app.log.warn("%s write transfer failed", proto)
    finally:
        os.remove(src)

    for proto in app.params.protos:
        t = app.now
        prefix = getattr(app.params, proto)
        process, duration = timecp(proto, "%s%s" % (prefix, app.params.test_file),
            localprefix(proto, os.devnull))
        if process.returncode == 0:
            data.append(("dcache", "%s_read_latency" % proto, t, duration))
        else:
            app.log.warn("%s read transfer failed", proto)

    if data:
        data = app.prepare(data)
        app.tsar.bulk(data)

default_testfile = "/pnfs/hep.wisc.edu/cmsprod/latency-test/testfile"
default_destination = "/pnfs/hep.wisc.edu/cmsprod/latency-test"
default_srm = "srm://cmssrm.hep.wisc.edu:8443/srm/managerv2?SFN="
default_dcap = "dcap://cmsdcap.hep.wisc.edu:22125"
default_gsiftp = "gsiftp://cmsgridftp.hep.wisc.edu:2811"

dcache_latency.add_param("-T", "--test-file", default=default_testfile,
    help="Remote test file (default: %s)" % default_testfile)
dcache_latency.add_param("-d", "--destination", default=default_destination,
    help="Destination directory (default: %s)" % default_destination)
dcache_latency.add_param("--srm", default=default_srm,
    help="SRM base URL (default: %s)" % default_srm)
dcache_latency.add_param("--dcap", default=default_dcap,
    help="dcap base URL (default: %s)" % default_dcap)
dcache_latency.add_param("--gsiftp", default=default_gsiftp,
    help="gsiftp base URL (default: %s)" % default_gsiftp)
dcache_latency.add_param("protos", nargs="+", default=[],
    choices=copiers.keys(), help="Protocols to test (default: none)")

if __name__ == "__main__":
    dcache_latency.run()
