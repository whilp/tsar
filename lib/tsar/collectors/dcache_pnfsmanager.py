#!/usr/bin/env python

from . import helpers
from .commands import Collector

from dcache.admin import Admin

class PnfsManager(Collector):

    @staticmethod
    def main(self):
        connection = self.params.admin.as_dict
        connection["passwd"] = str(self.params.password)

        # Connect to the admin interface.
        admin = Admin()
        admin.connect(**connection)

        admin.cd("PnfsManager")
        t = self.now
        lines = admin.do("info").splitlines()
        admin.close()

        if not lines:
            return 1

        stats = {}
        state = ""
        stats["failrates"] = dict((k, 0) for k in 
            "getfilemetadata getstorageinfo total".split())

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("Threads"):
                state = "threads"
            elif line.startswith("Statistics:"):
                state = "stats"
            elif state == "threads":
                if line.startswith("Thread groups"):
                    state = ""
                    continue
                _, depth = line.split(None, 1)
                stats.setdefault("queue_depth", []).append(depth)
            elif state == "stats":
                if not line or line.startswith("PnfsManagerV3"):
                    continue
                elif line.startswith("Total"):
                    state = ""
                msgtype, requests, failed = line.split(None, 2)
                msgtype = helpers.rtrim(msgtype, "Message")
                msgtype = helpers.trim(msgtype, "Pnfs")
                msgtype = msgtype.lower()
                if msgtype not in stats["failrates"]: continue

                requests = int(requests)
                failed = float(failed)
                failrate = requests == 0 and requests or failed/requests
                stats["failrates"][msgtype] = 100 * failrate

        data = []
        subject = "dcache_pnfsmanager"

        failrates = stats["failrates"]
        for msgtype, rate in failrates.items():
            data.append((subject, "failrate_%s" % msgtype, t, rate))

        depth = stats.pop("queue_depth", [])
        if not depth:
            return 1

        data.append((subject, "queue_depth", t, [int(x) for x in depth]))

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("dcache-pnfsmanager", 
            help="dCache PNFSManager")

        # XXX: Can't use AdminApp here cause it inherits from cli.LoggingApp; it
        # should be a mixin...
        from dcache.app import AdminConnection, Password
        connection = AdminConnection()

        self.add_param("-P", "--password", type=Password,
            help="if PASSWORD starts with '%s', assume it's a path to "
                "a file containing the password; otherwise, use it "
                "as the password" % (Password.pathprefix))
        self.add_param("-a", "--admin", type=AdminConnection,
                help="connect to the admin interface as [USER@]HOST[:PORT] "
                    "(defaults: %s)" % connection)
