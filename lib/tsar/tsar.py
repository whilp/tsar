import cli

@cli.DaemonizingApp
def server(app):
    from .ext import wsgiserver
    from .web import service

    host, _, port = app.params.server.partition(':')
    if not port:
        port = 8000

    app.log.info("Starting server at http://%s:%s/", host, port)
    server = wsgiserver.CherryPyWSGIServer((host, int(port)), service)
    if app.params.daemonize:
        app.daemonize()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

server.add_param("server", nargs="?", help="<host>:<port>", default="0.0.0.0:8000")
