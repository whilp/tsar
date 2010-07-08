def server():
    from .ext import wsgiserver
    from .web import service
    server = wsgiserver.CherryPyWSGIServer(
        ('0.0.0.0', 8000), service)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()
