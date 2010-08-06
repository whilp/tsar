#!/usr/bin/env python

import operator
import urllib2

from . import helpers

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

@helpers.Collector
def dcache_transfers(app):
    url = app.params.url[0]

    txdata = {}
    t = app.now
    for line in urllib2.urlopen(url):
        if not line:
            continue

        incrkey(txdata, "active_transfers")
        fields = line.split()
        keys = fieldkeys.get(len(fields), None)
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
        protocol = protocols.get(protocol, None)
        if protocol is not None:
            incrkey(txdata, "%s_transfers" % protocol)

        if "error" in fields:
            incrkey(txdata, "transfer_errors")

    data = []
    subject = "dcache"

    data.append((subject, "transfer_errors", t, txdata.get("transfer_errors", 0)))
    data.append((subject, "active_transfers", t, txdata.get("active_transfers", 0)))
    for prot in set(protocols.values()):
        key = "%s_transfers" % protocol
        data.append((subject, key, t, txdata.get(key, 0)))

    data = list(helpers.prepare(data))

    speed = txdata.pop("speed", None)
    duration = txdata.pop("duration", None)

    if duration:
        duration = [float(x) for x in duration]
        data.append((subject, "transfer_duration", "max", t, max(duration)))
        data.append((subject, "transfer_duration", "min", t, min(duration)))
        data.append((subject, "transfer_duration", "ave", t, 
            sorted(duration)[len(duration)/2]))

    if speed:
        speed = [float(x) for x in speed]
        data.append((subject, "transfer_speed", "max", t, max(speed)))
        data.append((subject, "transfer_speed", "min", t, min(speed)))
        data.append((subject, "transfer_speed", "ave", t, 
            sorted(speed)[len(speed)/2]))

    if data:
        app.tsar.bulk(data)

dcache_transfers.add_param("url", nargs=1, help="Active transfers text URL")

if __name__ == "__main__":
    dcache_transfers.run()
