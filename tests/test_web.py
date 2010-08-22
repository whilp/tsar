import csv

from itertools import chain
from math import cos

from tsar import model
from tsar.web import AllRecords, Records
from tsar.util import json

from tests import AppTest, BaseTest, log, unittest

model.db = model.connect(db=15)

class RecordsTest(AppTest):
    first = 1278007837
    cls = Records
    columns = u"subject attribute cf timestamp value".split()

    def setUp(self):
        super(RecordsTest, self).setUp()
        self.db = model.db
        self.db.flushdb()
        self.application = self.cls()

        interval = 300
        self.data = []
        self.last = self.first
        for i in range(1000):
            value = int(100 * cos(i))
            self.data.append((self.last, value))
            self.last += interval + ((interval/5) * (.709 - .5))
            
    def tearDown(self):
        super(RecordsTest, self).tearDown()
        self.db.flushdb()

    def post(self, path, body, content_type):
        req = self.req(path, method="POST")
        req.content_type = content_type
        req.body = body

        return req.get_response(self.application)

    def get(self, path, accept):
        req = self.req(path, headers=dict(accept=accept), method="GET")
        return req.get_response(self.application)

    def datatocsvf(self, data):
        buffer = csv.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(data)
        buffer.seek(0)
        return buffer

class TestAllRecords(RecordsTest):
    cls = AllRecords

    def setUp(self):
        super(TestAllRecords, self).setUp()
        self.data = [l.strip().split() for l in """\
            foo bar last 1278028800 10
            spam eggs last 1278028800 100
            foo bar last 1278028880 9
            spam eggs last 1278028880 110
            foo bar last 1278028960 8
            spam eggs last 1278028960 120
        """.splitlines() if l]

    def test_get(self):
        response = self.get("/records", accept="*/*")
        self.assertEquals(response.status_int, 415)

    def test_get_json(self):
        data = chain([self.columns], self.data)
        body = self.datatocsvf(data).read()
        response = self.post("/records", content_type="text/csv",
            body=body)
        self.assertEquals(response.status_int, 204)

        response = self.get("/records?subject=foo&attribute=bar&cf=last"
            "&subject=spam&attribute=eggs&cf=last", 
            accept="application/json")
        self.assertEquals(response.status_int, 200)
        body = json.loads(response.body)
        self.assertTrue(body.get("foo/bar/last", []) is not [])
        self.assertTrue(body.get("spam/eggs/last", []) is not [])

    def test_post(self):
        data = chain([self.columns], self.data)
        body = self.datatocsvf(data).read()
        response = self.post("/records", content_type="text/csv",
            body=body)
        self.assertEquals(response.status_int, 204)

class TestAllRecordsGet(RecordsTest):
    cls = AllRecords

    def setUp(self):
        super(TestAllRecordsGet, self).setUp()
        for i in ("foo bar", "spam eggs"):
            s, a = i.split()
            record = model.Records(s, a, "last")
            record.extend(self.data)

    def test_get_multiquery(self):
        response = self.get("/records?" 
            "subject=foo&attribute=bar&cf=last&subject=spam&attribute=eggs&cf=last", 
            accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertTrue("foo/bar/last" in body)
        self.assertTrue("spam/eggs/last" in body)

    def test_get_multiquery_and_magicparams(self):
        response = self.get("/records?_accept=application/json&" 
            "subject=foo&attribute=bar&cf=last&subject=spam&attribute=eggs&cf=last", 
            accept="text/csv")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertTrue("foo/bar/last" in body)
        self.assertTrue("spam/eggs/last" in body)

class TestRecordsPost(RecordsTest):

    def test_post(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"foo/bar/last": [self.data[-1]]}))
        self.assertEqual(response.status_int, 204)

    def test_post_bulk(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"foo/bar/last": self.data}))
        self.assertEqual(response.status_int, 204)

    def test_post_bulk_csv(self):
        data = [self.columns]
        for t, v in self.data:
            data.append(("foo", "bar", "last", t, v))
        body = self.datatocsvf(data).read()
        response = self.post("/records/foo/bar/last", content_type="text/csv",
            body=body)
        self.assertEqual(response.status_int, 204)
    
    def test_post_badkey(self):
        response = self.post("/records/foo!/bar/last", content_type="application/json",
            body=json.dumps({"foo!/bar/last": [self.data[-1]]}))
        self.assertEqual(response.status_int, 400)
    
    def test_post_badtime(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"foo/bar/last": [("foo", 10)]}))
        self.assertEqual(response.status_int, 400)
    
    def test_post_badvalue(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"foo/bar/last": [(self.first, "ten")]}))
        self.assertEqual(response.status_int, 400)

    def test_post_doublepost(self):
        response = self.post("/records/dblfoo/bar/last", content_type="application/json",
            body=json.dumps({"dblfoo/bar/last": [self.data[-1]]}))
        self.assertEqual(response.status_int, 204)
        response = self.post("/records/dblfoo/bar/last", content_type="application/json",
            body=json.dumps({"dblfoo/bar/last": [self.data[-2]]}))
        self.assertEqual(response.status_int, 204)
    
    def test_magic_params_content(self):
        response = self.post("/records/magicjson/bar/last?_content=application/json",
            content_type="text/csv",
            body=json.dumps({"magicjson/bar/last": [self.data[-1]]}))
        self.assertEqual(response.status_int, 204)
        response = self.post("/records/magicbad/bar/last?_content=text/csv",
            content_type="application/json",
            body=json.dumps({"magicbad/bar/last": [self.data[-1]]}))
        self.assertEqual(response.status_int, 400)
    
    def test_magic_params_method(self):
        path = "/records/magicjson/bar/last?_method=POST"
        req = self.req(path, method="GET")
        req.content_type = "application/json"
        req.body = json.dumps({"magicjson/bar/last": [self.data[-1]]})
        response = req.get_response(self.application)
        self.assertEqual(response.status_int, 204)

class TestRecordsGet(RecordsTest):

    def setUp(self):
        super(TestRecordsGet, self).setUp()

        full = model.Records("fullfoo", "bar", "last")
        full.extend(self.data)

    def test_get(self):
        response = self.get("/records/foo/bar/last", accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(body["foo/bar/last"], [])

    def test_get_full(self):
        response = self.get("/records/fullfoo/bar/last", accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(body["fullfoo/bar/last"], 
            [[1278028800, -69], [1278115200, -94], 
            [1278201600, -64], [1278288000, 99]])

    def test_get_params_now(self):
        start, now = 2 * -86400, 1278201600
        response = self.get("/records/fullfoo/bar/last?start=%d&now=%d" % (start, now),
            accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        data = body["fullfoo/bar/last"]
        self.assertEqual(len(data), 49)
        self.assertEqual(data[0], [1278028800, -96])
        self.assertEqual(data[-1], [1278201600, -98])

    def test_get_params_startstop(self):
        start, stop = 1278115200, 1278201600
        response = self.get("/records/fullfoo/bar/last?start=%d&stop=%d" % (start, stop),
            accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(len(body["fullfoo/bar/last"]), 25)

    def test_get_jsonp(self):
        response = self.get("/records/fullfoo/bar/last?callback=foo", 
            accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertTrue(response.body.startswith("foo("))
        self.assertTrue(response.body.endswith(")"))

    def test_get_csv(self):
        response = self.get("/records/fullfoo/bar/last", accept="text/csv")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "text/csv")
        reader = csv.reader(iter(response.body.splitlines()))
        self.assertEqual(list(reader), 
            [['subject', 'attribute', 'cf', 'timestamp', 'value'],
            ['fullfoo', 'bar', 'last', '1278028800', '-69'],
            ['fullfoo', 'bar', 'last', '1278115200', '-94'],
            ['fullfoo', 'bar', 'last', '1278201600', '-64'],
            ['fullfoo', 'bar', 'last', '1278288000', '99']] )
    
    def test_magic_params_accept(self):
        response = self.get("/records/fullfoo/bar/last?_accept=application/json", accept="*/*")
        self.assertEqual(response.status_int, 200)
        result = json.loads(response.body)["fullfoo/bar/last"]
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], [1278028800, -69])
        self.assertEqual(result[-1], [1278288000, 99])

    def test_skip_null(self):
        response = self.get("/records/fullfoo/bar/last?interval=60&filters=skipnull", 
            accept="application/json")
        self.assertEqual(response.status_int, 200)
        body = json.loads(response.body)
        data = body["fullfoo/bar/last"]
        values = [x[1] for x in data]
        self.assertTrue(None not in values)
        self.assertEqual(len(data), 277)
