#!/usr/bin/env python

import subprocess

from . import Collector

incrkey = lambda d, k, i=1: operator.setitem(d, k, d.setdefault(k, 0) + i)

def run(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

@Collector
def afs_server(app):
    server = app.params.server[0]
    cmd = ["/usr/sbin/rxdebug", server]

    t = app.now
    process = run(cmd)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        app.log.warn("Failed to run rxdebug (%d): %r", process.returncode,
            ' '.join(cmd))

    rxdata = {}
    for line in stdout.splitlines():
        if line.startswith("Connection from"):
            incrkey(rxdata, "afs_connections")
        elif "waiting_for_process" in line:
            incrkey(rxdata, "blocked_afs_connections")

    data = []
    subject = server
    data.append((subject, "afs_connections", t, rxdata.get("afs_connections", 0)))
    data.append((subject, "blocked_afs_connections", t, 
        rxdata.get("blocked_afs_connections", 0)))

    data = app.prepare(data)
    app.tsar.bulk(data)

afs_server.add_param("server", nargs=1, help="AFS server name")

if __name__ == "__main__":
    afs_server.run()
