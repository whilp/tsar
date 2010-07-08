import cli

def parsedsn(dsnstring, **defaults):
    "<driver>://<username>:<password>@<host>:<port>/<database>"
    dsn = dict((x, None) for x in "driver username password host port database".split())
    dsn.update(defaults)
    dsn["driver"], _, rest = dsnstring.partition("://")
    if '@' in rest:
        user, _, rest = rest.partition('@')
        if ':' in user:
            user, _, dsn["password"] = user.partition(':')
        dsn["user"] = user
    if '/' in rest:
        host, _, dsn["database"] = rest.partition('/')
    if ':' in host:
        host, _, dsn["port"] = host.partition(':')
    dsn["host"] = host
    if dsn["port"] is not None:
        dsn["port"] = int(dsn["port"])

    return dsn

@cli.DaemonizingApp(name="tsar-server")
def server(app):
    from .web import Server

    host, _, port = app.params.server.partition(':')
    if not port:
        port = 8000
    dsn = parsedsn(app.params.dsn)

    app.log.info("Starting server at http://%s:%s/", host, port)
    server = Server(host, int(port))
    if app.params.daemonize:
        app.daemonize()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

default_server = "0.0.0.0:8000"
default_dsn = "redis://localhost:6379/0"
server.add_param("server", nargs="?",
    help="<host>:<port> (default: %s)" % default_server, default=default_server)
server.add_param("-D", "--dsn", default=default_dsn,
    help="<driver>://<username>:<password>@<host>:<port>/<database> (default: %s)" % default_dsn)
