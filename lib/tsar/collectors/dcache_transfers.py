#!/usr/bin/env python

import operator
import urllib2

from . import helpers
from .commands import Collector

class DcacheTransfers(Collector):
    protocols = {
        "dcap-3": "dcap",
        "GFtp-1": "gsiftp",
    }
    sharedkeys = "door domain seq prot owner proc pnfsid pool host "
    fieldkeys = {
        12: (sharedkeys + "status since error").split(),
        16: (sharedkeys + "status since s _ transferred speed _").split(),
        18: (sharedkeys + "_ mover direction since s _ transferred speed _").split(),
    }

    @staticmethod
    def main(self):
        url = self.params.url[0]

        txdata = {}
        t = self.now
        for line in urllib2.urlopen(url):
            if not line:
                continue

            helpers.incrkey(txdata, "active_transfers")
            fields = line.split()
            keys = self.fieldkeys.get(len(fields), None)
            if not keys:
                continue

            fields = dict(zip(keys, fields))

            speed = fields.get("speed", None)
            if speed is not None:
                txdata.setdefault("speed", []).append(speed)

            duration = fields.get("since", None)
            if duration is not None:
                txdata.setdefault("duration", []).append(duration)

            protocol = fields.get("prot", None)
            protocol = self.protocols.get(protocol, None)
            if protocol is not None:
                helpers.incrkey(txdata, "%s_transfers" % protocol)

            if "error" in fields:
                helpers.incrkey(txdata, "transfer_errors")

        data = []
        subject = "dcache"

        data.append((subject, "transfer_errors", t, txdata.get("transfer_errors", 0)))
        data.append((subject, "active_transfers", t, txdata.get("active_transfers", 0)))
        for prot in set(self.protocols.values()):
            key = "%s_transfers" % prot
            data.append((subject, key, t, txdata.get(key, 0)))

        data.append((subject, "transfer_duration", t, 
            [float(x) for x in txdata.pop("duration", [])]))
        data.append((subject, "transfer_speed", t,
            [float(x) for x in txdata.pop("speed", [])]))

        self.submit(data)
    
    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("dcache-transfers", 
            help="dCache active transfers")
        self.add_param("url", nargs=1, help="Active transfers text URL")
