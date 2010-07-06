import logging

from neat import Resource

def logger(cls): # pragma: nocover
    name = "%s.%s" % (__name__, cls.__class__.__name__)
    return logging.getLogger(name)

class Records(Resource):
    prefix = "/records/"

    def record(self, subject, attribute, cf, data):
        records = Records(subject, attribute, cf)
        records.extend(data)

    def parseuri(self, uri):
        """Parse a URI.

        Returns a tuple (subject, attribute, cf).
        """
        return uri.lstrip('/').split('/', 3)[1:]

    def post(self):
        subject, attribute, cf = self.parseuri(self.req.path_info)
        self.record(subject, attribute, cf, self.req.content)
        self.response.status_int = 204 # No Content
