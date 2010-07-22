#!/usr/bin/env python

import operator
import subprocess

from . import Collector

incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)

def run(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

jobstatusmap = {
    0: "unexpanded",
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
}

@Collector(timeout=60)
def condor_queue(app):
    attributes = """Owner RemoteWallClockTime CurrentTime x509userproxysubject
         JobStartDate JobStatus GlobalJobId""".split()

    cmd = ["/condor/bin/condor_q", "-global", "-pool", "glow.cs.wisc.edu",
        "-attributes", ','.join(attributes),
        "-format", "runtime=%d\n", "RemoteWallClockTime + (CurrentTime - EnteredCurrentStatus)",
        "-format", "jobstatus=%d\n", "JobStatus",
        "-format", "user=%s", "Owner",
        "-format", "|%s", "x509userproxysubject",
        "-format", "\n", "Owner",
        "-format", "globaljobid=%s\n\n", "GlobalJobId",
    ]
    t = app.now
    process = run(cmd)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        app.log.warn("Failed to run condor_q (%d): %r", process.returncode, 
            ' '.join(cmd))
        return 1

    cqdata = {}
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
