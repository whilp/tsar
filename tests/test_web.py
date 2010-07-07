from math import cos

from tsar import model
from tsar.web import Records
from tsar.util import json

from tests import AppTest, BaseTest, log, unittest

model.db = model.connect(db=15)

class RecordsTest(AppTest):
    first = 1278007837
    
    def setUp(self):
        super(RecordsTest, self).setUp()
        self.db = model.db
        self.db.flushdb()
        self.application = Records()

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

class TestRecordsPost(RecordsTest):

    def post(self, path, body, content_type):
        req = self.req(path, method="POST")
        req.content_type = content_type
        req.body = body

        return req.get_response(self.application)

    def test_post(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"data": [self.data[-1]]}))
        self.assertEqual(response.status_int, 204)

    def test_post_bulk(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"data": self.data}))
        self.assertEqual(response.status_int, 204)
    
    def test_post_badkey(self):
        response = self.post("/records/foo!/bar/last", content_type="application/json",
            body=json.dumps({"data": [self.data[-1]]}))
        self.assertEqual(response.status_int, 400)
    
    def test_post_badtime(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"data": [("foo", 10)]}))
        self.assertEqual(response.status_int, 400)
    
    def test_post_badvalue(self):
        response = self.post("/records/foo/bar/last", content_type="application/json",
            body=json.dumps({"data": [(self.first, "ten")]}))
        self.assertEqual(response.status_int, 400)

    def test_post_doublepost(self):
        response = self.post("/records/dblfoo/bar/last", content_type="application/json",
            body=json.dumps({"data": [self.data[-1]]}))
        self.assertEqual(response.status_int, 204)
        response = self.post("/records/dblfoo/bar/last", content_type="application/json",
            body=json.dumps({"data": [self.data[-2]]}))
        self.assertEqual(response.status_int, 409)

class TestRecordsGet(RecordsTest):

    def setUp(self):
        super(TestRecordsGet, self).setUp()

        full = model.Records("fullfoo", "bar", "last")
        full.extend(self.data)

    def get(self, path, accept):
        req = self.req(path, headers=dict(accept=accept), method="GET")
        return req.get_response(self.application)

    def test_get(self):
        response = self.get("/records/foo/bar/last", accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(body["data"], [])

    def test_get_full(self):
        response = self.get("/records/fullfoo/bar/last", accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(len(body["data"]), 3)
