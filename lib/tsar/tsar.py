import cli

from .util import parsedsn

@cli.DaemonizingApp(name="tsar-server")
def server(app):
    from . import model
    from .web import Server

    host, _, port = app.params.server.partition(':')
    if not port:
        port = 8000
    dsn = parsedsn(app.params.dsn)
    del(dsn["username"])
    del(dsn["driver"])
    dsn["db"] = dsn.pop("database")
    model.db = model.connect(**dsn)

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
