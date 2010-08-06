#!/usr/bin/env python

from . import helpers

@helpers.Collector
def afs_server(app):
    server = app.params.server[0]
    cmd = ["/usr/sbin/rxdebug", server]

    t = app.now
    process = helpers.runcmd(cmd)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        app.log.warn("Failed to run rxdebug (%d): %r", process.returncode,
            ' '.join(cmd))

    rxdata = {}
    for line in stdout.splitlines():
        if line.startswith("Connection from"):
            helpers.incrkey(rxdata, "afs_connections")
        elif "waiting_for_process" in line:
            helpers.incrkey(rxdata, "blocked_afs_connections")

    data = []
    subject = server
    data.append((subject, "afs_connections", t, rxdata.get("afs_connections", 0)))
    data.append((subject, "blocked_afs_connections", t, 
        rxdata.get("blocked_afs_connections", 0)))

    app.submit(data)

afs_server.add_param("server", nargs=1, help="AFS server name")

if __name__ == "__main__":
    afs_server.run()
