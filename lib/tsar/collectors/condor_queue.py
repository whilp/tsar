#!/usr/bin/env python

from . import helpers
from .commands import Collector

class CondorQueue(Collector):
    attributes = """Owner RemoteWallClockTime CurrentTime x509userproxysubject
         JobStartDate JobStatus ProdAgent_JobType GridResource GlobalJobId""".split()
    jobstatusmap = {
        0: "unexpanded",
        1: "idle",
        2: "running",
        3: "removed",
        4: "completed",
        5: "held",
    }

    def main(self):
        cmd = ["/condor/bin/condor_q", "-global",
            # Pool/schedd will be inserted here.
            "-attributes", ','.join(self.attributes),
            "-format", "runtime=%d\n", "RemoteWallClockTime + (CurrentTime - EnteredCurrentStatus)",
            "-format", "user=%s", "Owner",
            "-format", "|%s", "x509userproxysubject",
            "-format", "\n", "Owner",
            "-format", "prodagentjobtype=%s\n", "ProdAgent_JobType",
            "-format", "gridresource=%s\n", "GridResource",
            "-format", "jobstatus=%d\n", "JobStatus",
            "-format", "globaljobid=%s\n\n", "GlobalJobId",
        ]
        pool = self.params.pool[0]
        target = ["-pool", pool]
        if self.params.name:
            target = ["-name", pool]
        cmd = helpers.insert(cmd, 1, target)

        t = self.now
        if self.params.input:
            stdout = open(self.params.input, 'r').read()
        else:
            process, stdout, stderr = self.runcmd(cmd)
            if self.params.output:
                open(self.params.output, 'w').write(stdout)
            
        cqdata = {}
        state = {}
        for line in stdout.splitlines():
            if not line:
                continue

            key = None
            k, v = line.split('=', 1)
            if k == "runtime":
                runtimes = cqdata.setdefault("job_runtime", [])
                runtimes.append(int(v))
            elif k == "user":
                users = cqdata.setdefault("condor_users", set())
                users.add(v)
                state["user"] = v.partition('|')[0]
            elif k == "prodagentjobtype":
                state["pajobtype"] = v.lower()
            elif k == "gridresource":
                gridresource = v.split()[1]
                state["gridresource"] = gridresource.split('/')[0]
            elif k == "globaljobid":
                key = "total_jobs"
                state = {}
            elif k == "jobstatus":
                status = self.jobstatusmap[int(v)]
                state["status"] = status
                if status in ("running", "held", "idle"):
                    keys = []
                    if "user" in state:
                        keys.append("owner_%(user)s_%(status)s_jobs")
                    if "gridresource" in state:
                        keys.append("prod_%(gridresource)s_%(status)s_jobs")
                    if "pajobtype" in state:
                        keys.append("prod_%(pajobtype)s_%(status)s_jobs")
                    if all(k in state for k in ("gridresource", "pajobtype")):
                        keys.append("prod_%(gridresource)s_%(pajobtype)s_%(status)s_jobs")
                    for key in keys:
                        helpers.incrkey(cqdata, key % state)
                    key = "%(status)s_jobs" % state
                
            if key is not None:
                helpers.incrkey(cqdata, key)

        data = []
        subject = pool

        users = cqdata.pop("condor_users", None)
        if users:
            data.append((subject, "users", t, len(users)))

        data.extend((subject, k, t, v) for k, v in cqdata.items())

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("condor-queue", 
            help="Condor batch queues")
        self.add_param("pool", nargs=1, help="Condor pool to query")
        self.add_param("-i", "--input", default=False,
            help="use file INPUT instead of running condor_q")
        self.add_param("-o", "--output", default=False,
            help="write condor_q output to file OUTPUT")
        self.add_param("-n", "--name", default=False, action="store_true",
            help="POOL is a schedd, not a pool (default: false)")


if __name__ == "__main__":
    condor_queue.run()
