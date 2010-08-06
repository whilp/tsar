#!/usr/bin/env python

from . import helpers

from dcache.admin import Admin
from dcache.app import AdminApp

class AdminCollector(AdminApp, helpers.Collector):
    pass

@AdminCollector
def dcache_pnfsmanager(app):
    connection = app.params.admin.as_dict
    connection["passwd"] = str(app.params.password)

    # Connect to the admin interface.
    admin = Admin()
    admin.connect(**connection)

    admin.cd("PnfsManager")
    t = app.now
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
            depth = int(depth)
            stats.setdefault("queue_depth", []).append(depth)
        elif state == "stats":
            if not line or line.startswith("PnfsManagerV3"):
                continue
            elif line.startswith("Total"):
                state = ""
            msgtype, requests, failed = line.split(None, 2)
            msgtype = rtrim(msgtype, "Message")
            msgtype = trim(msgtype, "Pnfs")
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

    data = list(helpers.prepare(data))

    depth = stats.pop("queue_depth", [])
    if not depth:
        return 1

    data.append((subject, "queue_depth", "max", t, max(depth)))
    data.append((subject, "queue_depth", "min", t, min(depth)))
    data.append((subject, "queue_depth", "ave", t, median(depth)))

    if data:
        app.tsar.bulk(data)

if __name__ == "__main__":
    dcache_pnfsmanager.run()
