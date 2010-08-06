#!/usr/bin/env python

from . import helpers

jobstatusmap = {
    0: "unexpanded",
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
}

@helpers.Collector(timeout=120)
def condor_queue(app):
    attributes = """Owner RemoteWallClockTime CurrentTime x509userproxysubject
         JobStartDate JobStatus GlobalJobId""".split()

    pool = app.params.pool[0]
    cmd = ["/condor/bin/condor_q", "-global", "-pool", pool,
        "-attributes", ','.join(attributes),
        "-format", "runtime=%d\n", "RemoteWallClockTime + (CurrentTime - EnteredCurrentStatus)",
        "-format", "jobstatus=%d\n", "JobStatus",
        "-format", "user=%s", "Owner",
        "-format", "|%s", "x509userproxysubject",
        "-format", "\n", "Owner",
        "-format", "globaljobid=%s\n\n", "GlobalJobId",
    ]
    t = app.now
    process = helpers.runcmd(cmd)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        app.log.warn("Failed to run condor_q (%d): %r", process.returncode, 
            ' '.join(cmd))
        return 1

    cqdata = {}
    for line in stdout.splitlines():
        if not line:
            continue

        key = None
        k, v = line.split('=', 1)
        if k == "runtime":
            runtimes = cqdata.setdefault("runtime", [])
            runtimes.append(int(v))
        elif k == "user":
            users = cqdata.setdefault("condor_users", set())
            users.add(v)
        elif k == "globaljobid":
            key = "total_jobs"
        elif k == "jobstatus":
            status = jobstatusmap[int(v)]
            if status in ("running", "held", "idle"):
                key = "%s_jobs" % status
            
        if key is not None:
            incrkey(cqdata, key)

    data = []
    subject = pool
    runtimes = cqdata.pop("runtime", None)
    users = cqdata.pop("condor_users", None)
    if users:
        data.append((subject, "users", t, len(users)))

    for k, v in cqdata.items():
        data.append((subject, k, t, v))

    data = list(helpers.prepare(data))

    if runtimes:
        runtimes = [float(x) for x in runtimes]
        data.append((subject, "job_runtime", "max", t, max(runtimes)))
        data.append((subject, "job_runtime", "min", t, max(runtimes)))
        data.append((subject, "job_runtime", "ave", t, 
            sorted(runtimes)[len(runtimes)/2]))

    app.tsar.bulk(data)

condor_queue.add_param("pool", nargs=1, help="Condor pool to query")

if __name__ == "__main__":
    condor_queue.run()
