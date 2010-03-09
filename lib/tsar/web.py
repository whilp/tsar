import cli

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler
from tornado.wsgi import WSGIApplication

class APIHandler(RequestHandler):
    pass

routes = [
]

application = WSGIApplication(routes)

@cli.DaemonizingApp(name="tsar-server")
def tsar_server(app):
    server = HTTPServer(application)
    server.listen(app.params.port)
    IOLoop.instance().start()

tsar_server.add_param("-P", "--port", default=8000, type=int,
	help="server port")

if __name__ == "__main__":
    tsar_server.run()
