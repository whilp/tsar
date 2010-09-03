#!/usr/bin/env python

import shlex

from itertools import chain

from . import helpers
from .commands import Collector

class CondorQueue(Collector):
    attributes = """Owner RemoteWallClockTime CurrentTime x509userproxysubject
         JobStartDate JobStatus ProdAgent_JobType GridResource GlobalJobId""".split()
    terminator = "__TERMINATOR__"
    jobstatusmap = {
        0: "unexpanded",
        1: "idle",
        2: "running",
        3: "removed",
        4: "completed",
        5: "held",
    }
    recordtypes = {
        "runtime": int,
        "user": lambda x: x.partition('|')[0],
        "prodagentjobtype": lambda x: x.lower(),
        "gridresource": lambda x: x.split()[1],
        "jobstatus": jobstatusmap[int(x)],
        "globaljobid": lambda x: x.split('#'),
    }
    keys = [
        "owner_%(user)s_%(status)s_jobs",
        "prod_%(gridresource)s_%(status)s_jobs",
        "prod_%(pajobtype)s_%(status)s_jobs",
        "prod_%(gridresource)s_%(pajobtype)s_%(status)s_jobs",
    ]

    def convert(self, record, types):
        return dict((k, types[k](v) if k in types else v) for k, v in record.items())

    def handlerecord(self, record, data, aggregate=None):
        resource = record.get("gridresource", None)
        schedd = record.get("globaljobid", [None])[0]
        status = record.get("status", None)
        runtime = record.get("runtime", None)
        user = record.get("user", None)
        if not all((resource, schedd, status in ("running", "held", "idle"))):
            return
            
        subjects = [schedd]
        if aggregate is not None:
            subjects.append(aggregate)

        for subject in subjects:
            _data = data[subject]
            if runtime:
                _data.setdefault("job_runtime", []).append(runtime)
            if user:
                _data.setdefault("condor_users", set()).add(user)

            for key in self.keys:
                try:
                    key = key % record
                except TypeError:
                    # The key needs data this record doesn't have.
                    continue
                helpers.incrkey(_data, key)

    def parse(self, lines, aggregate=None):
        data = {}
        record = {}
        for line in lines:
            if not line:
                continue
            elif line == self.terminator:
                # End of a record.
                record = self.convert(record, self.recordtypes)
                self.handlerecord(record, data, aggregate)
                record = {}
            else:
                # Entry in a record.
                k, _, v = line.partition('=')
                record[k] = v

        return data

    def main(self):
        cmd = shlex.split(self.params.condorq)
        if self.params.pool:
            cmd.extend(["-pool", self.params.pool])
        cmd.extend(chain(*[("-name", s) for s in self.params.names]))
        cmd.extend([
            "-attributes", ','.join(self.attributes),
            "-format", "runtime=%d\n", "RemoteWallClockTime + (CurrentTime - EnteredCurrentStatus)",
            "-format", "user=%s", "Owner",
            "-format", "|%s", "x509userproxysubject",
            "-format", "\n", "Owner",
            "-format", "prodagentjobtype=%s\n", "ProdAgent_JobType",
            "-format", "gridresource=%s\n", "GridResource",
            "-format", "jobstatus=%d\n", "JobStatus",
            "-format", "globaljobid=%%s\n%s\n" % self.terminator, "GlobalJobId",
        ])

        if not self.params.aggregate and self.params.only_aggregate:
            self.argparser.error("must specify -a/--aggregate with -A/--only-aggregate")

        t = self.now
        if self.params.input:
            stdout = open(self.params.input, 'r').read()
        else:
            process, stdout, stderr = self.runcmd(cmd, expect=False)
            if process.returncode == 1 and "All queues are empty" in stdout:
                return 0
            if self.params.output:
                open(self.params.output, 'w').write(stdout)

        data = []
        parsed = self.parse(stdout.splitlines(), self.params.aggregate)
        for subject, _data in parsed.items():
            users = _data.pop("condor_users", None)
            if users:
                data.append((subject, "users", t, len(users)))
            data.extend((subject, k, t, v) for k, v in _data.items())

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("condor-queue", 
            help="Condor batch queues")
        self.add_param("names", nargs="+", help="schedds to query")
        self.add_param("-a", "--aggregate", default=None,
            help="sum metrics for each schedd and report as AGGREGATE")")
        self.add_param("-A", "--only-aggregate", default=False, action="store_true",
            help="only report aggregated metrics (see -a)")
        self.add_param("-c", "--condorq", default="/condor/bin/condor_q",
            help="path to condor_q executable (and optional extra arguments)")
        self.add_param("-i", "--input", default=False,
            help="use file INPUT instead of running condor_q")
        self.add_param("-o", "--output", default=False,
            help="write condor_q output to file OUTPUT")
        self.add_param("-p", "--pool", default=None,
            help="pool in which to find the schedd(s)")

if __name__ == "__main__":
    condor_queue.run()
