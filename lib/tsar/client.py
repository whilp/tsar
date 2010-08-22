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

import csv
import datetime

from calendar import timegm
from time import struct_time
from urllib import urlencode
from urllib2 import HTTPError, HTTPRedirectHandler, Request, build_opener
from urllib2 import quote, unquote

from . import errors

__all__ = ["Tsar"]

def timetostamp(time, now=None):
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
        time = time.utctimetuple()

    if isinstance(time, struct_time):
        time = timegm(time)

    return time

def stamptotime(stamp):
    """Convert a Unix timestamp to a datetime object.

    *stamp* should be an integer representing seconds since the Unix
    Epoch, UTC.
    """
    return datetime.datetime.fromtimestamp(stamp)

class RESTHandler(HTTPRedirectHandler):
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

class RESTError(Exception):
    pass

class RESTClient(object):
    """A REST client.

    The client implements a REST-style protocol, similar to that
    described in the "Protocol Operations" section of the ATOM
    Publishing Protocol (RFC 5023):

        http://bitworking.org/projects/atom/rfc5023.html#operation

    The client supports methods for deserializing and serializing
    representations to and from native objects, respectively. The client
    raises RESTError when anything unexpected happens.
    """
    debuglevel = 0
    handlers = [RESTHandler]
    headers = {}
    requestfactory = Request
    agent = None

    def __init__(self, service, agent=None):
        self.service = service
        if agent is None:
            agent = self.agent
        self.headers["User-agent"] = agent
        self.opener = build_opener()

        for handler in self.handlers:
            self.opener.add_handler(handler())
        for handler in self.opener.handlers:
            handler._debuglevel = self.debuglevel

    def request(self, url, method="GET", data=None, headers={}):
        """Send a request to the service, returning its response.

        The response is a httplib.HTTPResponse instance.
        """
        _headers = self.headers.copy()
        _headers.update(headers)

        req = self.requestfactory(url, data, _headers)
        req.get_method = lambda : method
        try:
            response = self.opener.open(req)
        except HTTPError, e:
            e.req = req
            raise

        return response

class Tsar(RESTClient):
    """A Tsar client.

    The client can submit new records to the service or query the
    service for existing records. Arguments are:

    *service* should be a URL pointing to the server's API endpoint.
    """
    mediatype = "application/vnd.tsar.records.v2"

    def __init__(self, service="http://tsar.hep.wisc.edu/records"):
        super(Tsar, self).__init__(service)

    def resource(self, subject, attribute, cf):
        return quote(u'/'.join((subject, attribute, cf)))

    def record(self, subject, attribute, cf, time, value):
        """Record a new observation.

        *subject* and *attribute* are free-form string fields. *time* is
        either a :instance:`datetime.datetime` instance, a standard time
        tuple or an integer representing seconds since the Epoch, UTC
        (as produced by :func:`time.gmtime`). *value* may be either a
        float or an integer.

        If the server accepts the new record, :meth:`record` returns
        True. Otherwise, :class:`APIError` is raised.
        """
        data = [[subject, attribute, cf, time, value]]
        return self.bulk(data)

        return True

    def bulk(self, data):
        buffer = csv.StringIO()
        writer = csv.writer(buffer)
        writer.writerow("subject attribute cf timestamp value".split())
        nodata = True
        for s, a, c, t, v in data:
            nodata = False
            writer.writerow((s, a, c, timetostamp(t), v))
        if nodata:
            return
        buffer.seek(0); body = buffer.read()
        response = self.request(self.service, method="POST", 
            data=body, headers={"Content-Type": self.mediatype + "+csv"})

        if hasattr(response, "getcode") and response.getcode() != 204:
            raise errors.APIError("failed to create records", response)

        return True

    def query(self, subject, attribute, cf, 
            start=None, stop=None, now=None, filters=[], interval=None):
        """Query the tsar service.

        *subject* and *attribute* are free-form string fields which may include
        the wildcard operator '*'. *cf* is a consolidation function supported by
        the service. *start*, *stop* and *now* are absolute or relative times.
        See :func:`timetostamp` for more information.

        Returns an iterable yielding (time, value) tuples.
        """
        rid = self.resource(subject, attribute, cf)
        resource = '/'.join((self.service, rid))
        query = {}
        if start is not None:
            query["start"] = start
        if stop is not None:
            query["stop"] = stop
        if now is not None:
            query["now"] = now
        if interval is not None:
            query["interval"] = interval
        if filters:
            query["filters"] = ','.join(filters)
        if query:
            resource += '?' + urlencode(query)

        response = self.request(resource, method="GET",
            headers={"Accept": self.mediatype + "+csv"})

        if hasattr(response, "getcode") and response.getcode() != 200:
            raise errors.APIError("query failed", response)

        body = response.read()
        reader = csv.reader(iter(body.splitlines()))
        # Discard headers.
        _ = reader.next()
        for s, a, c, t, v in reader:
            t = stamptotime(int(t))
            if v in ("None", ''):
                v = None
            try:
                v = int(v)
            except TypeError:
                # v=None.
                pass
            except ValueError:
                v = float(v)
            yield (s, a, c, t, v)

if __name__ == "__main__":
    Tsar.debuglevel = 100
    tsar = Tsar("http://g13n01.hep.wisc.edu:8080/records")
    tsar.record("test_foo","baz", datetime.datetime.utcnow(), 30)
    print list(tsar.query("test_foo", "baz", start=-1600, stop=-1))
    #r = tsar.query("production_*_stevia","router_Running",-3600,-1)
    #print r
