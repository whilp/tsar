import cli

@cli.DaemonizingApp(name="tsar-server")
def server(app):
    from .web import Server

    host, _, port = app.params.server.partition(':')
    if not port:
        port = 8000

    app.log.info("Starting server at http://%s:%s/", host, port)
    server = Server(host, int(port))
    if app.params.daemonize:
        app.daemonize()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

default_server = "0.0.0.0:8000"
server.add_param("server", nargs="?",
    help="<host>:<port> (default: %s)" % default_server, default=default_server)
