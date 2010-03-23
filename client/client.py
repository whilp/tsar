"""\
Communicate with a TSAR service
-------------------------------

The TSAR client can send new records to and query existing records from
a TSAR service.

    >>> from datetime import datetime
    >>> tsar = Tsar("http://tsar.hep.wisc.edu/observations")
    >>> tsar.record("some_subject", "some_attribute", datetime.now(), 10)
    True
"""

import datetime

from calendar import timegm
from time import struct_time
from urllib import urlencode
from urllib2 import HTTPError, HTTPRedirectHandler, Request, build_opener

try:
    from mimeparse import best_match
except ImportError:
    best_match = False

__all__ = ["Tsar", "TsarError"]

class TsarError(Exception):
    pass

class APIError(TsarError):
    """Raised when the server rejects the client's API call."""
    
    def __init__(self, message, response, request):
        self.response = response
        self.request = request

        message = message + " (HTTP status: %d)" % self.response.getcode()

        super(RecordError, self).__init__(message)

def timetoint(time, now=None):
    """Convert a time representation to an integer.

    *time* may be a :class:`datetime.datetime` instance, a
    :class:`datetime.timedelta` instance, a :mod:`time` timetuple or
    an integer. The result represents seconds since the Unix Epoch,
    UTC.
    """
    # Convert datetime instances and timetuples to seconds since the
    # Epoch UTC.
    if isinstance(time, datetime.timedelta):
        if now is None:
            now = datetime.datetime.now()
        time = now - time

    if isinstance(time, datetime.datetime):
        time = time.timetuple()

    if isinstance(time, struct_time):
        time = timegm(time)

    return time

class TsarHandler(HTTPRedirectHandler):
    codes = (201, 301, 302, 303, 307)

    # This is a success, actually, that returns a 'location' header.
    # Treat it as a redirect.
    http_error_201 = HTTPRedirectHandler.http_error_302

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Return a Request or None in response to a redirect.
        
        urllib2.HTTPRedirectHandler only activates on certain codes.
        Override redirect_request() to make the codes more easily
        selectable. Copied from 2.5.4's urllib2.HTTPRedirectHandler.
        """
        m = req.get_method()
        if code in self.codes and (
                (m in ("GET", "HEAD")) or 
                (m == "POST" and code != 307)):
            # Strictly (according to RFC 2616), 301 or 302 in response
            # to a POST MUST NOT cause a redirection without confirmation
            # from the user (of urllib2, in this case).  In practice,
            # essentially all clients do redirect in this case, so we
            # do the same.
            # be conciliant with URIs containing a space
            newurl = newurl.replace(' ', '%20')
            return Request(newurl,
                           headers=req.headers,
                           origin_req_host=req.get_origin_req_host(),
                           unverifiable=True)
        else:
            raise HTTPError(req.get_full_url(), code, msg, headers, fp)

json = False
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        pass
    pass

def parse_json(response):
    return json.load(response)

class Tsar(object):
    """A Tsar client.

    The client can submit new records to the service or query the
    service for existing records. Arguments are:

    *service* should be a URL pointing to the server's API endpoint.
    """

    debuglevel = 0
    headers = {
        "User-agent": "TSAR-client/0.1"
    }
    httphandlers = [TsarHandler]
    request_factory = Request
    timeout = 5
    parsers = {
    }
    """A dictionary of registered parsers.

    Keys in this dictionary should correspond to supported values of the
    Content-type header set by the server. Values should be callables
    that accept a single :class:`httplib.HTTPResponse` instance as an
    argument and return a string.
    """

    if json:
        parsers["application/json"] = parse_json
        parsers["text/javascript"] = parse_json
    
    def __init__(self, service):
        self.service = service
        self.opener = build_opener()

        for handler in self.httphandlers:
            self.opener.add_handler(handler())
        for handler in self.opener.handlers:
            handler._debuglevel = self.debuglevel

    def request(self, url, method="GET", data=None):
        """Send a request to the service, returning its response.

        The response is a httplib.HTTPResponse instance.
        """
        req = self.request_factory(url, data, self.headers)
        req.get_method = lambda : method
        response = self.opener.open(req)

        return response

    def get(self, **params):
        """Send a GET request to the service, returning its response.

        *params* will be urlencoded and added to the service URL. The
        response is a httplib.HTTPResponse instance.
        """
        data = params and urlencode(params)
        url = '?'.join((self.service, data))
        return self.request(url, method="GET")

    def post(self, **params):
        """Send a POST request to the service, returning its response.

        *params* will be passed as POST parameters to the server. The
        response is a httplib.HTTPResponse instance.
        """
        data = params and urlencode(params)
        return self.request(self.service, method="POST", data=data)

    def parse(self, response):
        """Parse and return the service's response as a string.

        This method tries to find an appropriate parser by matching
        the *response*'s Content-type header against the registered
        parsers (:attr:`parsers`). If the mimeparse package is
        available, it is used to find the best match; if it is not
        available, only exact matches are considered. If no match is
        found, the *response* data is simply read and returned.
        """
        contenttype = response.headers.get("Content-type", None)
        if best_match is not False:
            match = best_match(self.parsers.keys(), contenttype)
            parser = self.parsers[match]
        else:
            parser = self.parsers.get(contenttype, None)

        if parser is None:
            parser = lambda r: r.read()

        return parser(response)

    def record(self, subject, attribute, time, value):
        """Record a new observation.

        *subject* and *attribute* are free-form string fields. *time* is
        either a :instance:`datetime.datetime` instance, a standard time
        tuple or an integer representing seconds since the Epoch, UTC
        (as produced by :func:`time.gmtime`). *value* may be either a
        float or an integer.

        If the server accepts the new record, :meth:`record` returns
        True. Otherwise, :class:`APIError` is raised.
        """
        time = timetoint(time)

        response = self.post(subject=subject, attribute=attribute,
            time=time, value=value)

        if response.getcode() != 201:
            raise APIError("failed to create record", response,
                (subject, attribute, time, value))

        return True

    def bulk(self, fileobj):
        raise NotImplementedError

    def query(self, subject, attribute, start, stop, **kwargs):
        istart, istop = timetoint(start), timetoint(stop)
        response = self.get(subject=subject, attribute=attribute,
            start=istart, stop=istop, **kwargs)

        if response.getcode() != 200:
            raise APIError("query failed", response,
                (subject, attribute, start, stop))

        results = self.parse(response)
        return results["results"]

if __name__ == "__main__":
    Tsar.debuglevel = 100
    tsar = Tsar("http://tsar.hep.wisc.edu/observations")
    tsar.record("foo","bar", datetime.datetime.now(), 10)
    r = tsar.query("production_*_stevia","router_Running",-3600,-1)
    print r
