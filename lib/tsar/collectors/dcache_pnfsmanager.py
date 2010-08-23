#!/usr/bin/env python

from . import helpers
from .commands import Collector

try:
    from dcache.app import AdminMixin
except ImportError:
    AdminMixin = None

class PnfsManager(AdminMixin, Collector):

    def main(self):
        connection = self.params.admin.as_dict
        connection["passwd"] = str(self.params.password)

        # Connect to the admin interface.
        from dcache.admin import Admin
        admin = Admin()
        admin.connect(**connection)

        admin.cd("PnfsManager")
        t = self.now
        lines = admin.do("info").splitlines()
        admin.close()

        if not lines:
            return 1

        state = ""
        stats = {}
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
                helpers.appendkey(stats, "msg_queue_depth", depth)
            elif state == "stats":
                if not line or line.startswith("PnfsManagerV3"):
                    continue
                elif line.startswith("Total"):
                    state = ""
                msgtype, requests, failed = line.split(None, 2)
                msgtype = helpers.rtrim(msgtype, "Message")
                msgtype = helpers.trim(msgtype, "Pnfs")
                msgtype = msgtype.lower()

                requests = int(requests)
                failed = int(failed)

                helpers.incrkey(stats, "msg_%s_requests" % msgtype, requests)
                helpers.incrkey(stats, "msg_%s_fails" % msgtype, failed)
                helpers.incrkey(stats, "msg_%s_failrate" % msgtype, 
                    helpers.pct(failed, requests))

        data = []
        subject = "dcache_pnfsmanager"

        depth = stats.pop("msg_queue_depth", [])
        if not depth:
            return 1

        data.append((subject, "msg_queue_depth", t, [int(x) for x in depth]))
        data.extend((subject, k, t, v) for k, v in stats.items())

        self.submit(data)

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("dcache-pnfsmanager", 
            help="dCache PNFSManager")
        AdminMixin.setup(self)
