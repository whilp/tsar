#!/usr/bin/env python

import operator
import subprocess

from . import Collector

incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)

def run(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

@Collector
def condor_queue(app):
    attributes = """Owner RemoteWallClockTime User ServerTime x509userproxysubject
         OSG_VO JobStartDate JobStatus GlobalJobId""".split()

    cmd = ["condor_q", "-global",
        "-attributes", ','.join(attributes),
        "-format", "\n\nruntime=%d\n", "RemoteWallClockTime + (ServerTime - JobStartDate)",
        "-format", "jobstatus=%d\n", "JobStatus",
        "-format", "owner=%s\n", "Owner",
        "-format", "user=%s\n", "User",
        "-format", "x509userproxysubject=%s\n", "x509userproxysubject",
        "-format", "osgvo=%s\n", "OSG_VO",
        "-format", "globaljobid=%s\n", "GlobalJobId",
    ]
    t = app.now
    process = run(cmd)
    if process.returncode != 0:
        app.log.warn("Failed to run condor_q (%d): %r", process.returncode, 
            ' '.join(cmd))
        return 1

    cqdata = {}
    stdout, stderr = process.communicate()
    for line in stdout.splitlines():
        k, v = line.split('=', 1)
        if k == "runtime":
            cqdata["runtime"].append(int(v))
        kdata = cqdata.setdefault(k, {})
        kdata.setdefault(v, 0)
        kdata[v] += 1

    data = []
    runtimes = cqdata.pop("runtime")
    data.append(("condor", "max_job_runtime", t, max(runtimes)))
    data.append(("condor", "ave_job_runtime", t, (1.0 * sum(runtimes))/len(runtimes)))
    data.append(("condor", "users", t, len(cqdata["user"])))

if __name__ == "__main__":
    condor_queue.run()
