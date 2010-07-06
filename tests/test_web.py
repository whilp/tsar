from math import cos
from random import random

from tsar import model
from tsar.web import Records
from tsar.util import json

from tests import AppTest, BaseTest, log

model.db = model.connect(db=15)

class TestRecords(AppTest):
    application = Records
    first = 1278007837
    
    def setUp(self):
        super(TestRecords, self).setUp()
        self.db = model.db
        self.db.flushdb()
        self.application = Records()

        interval = 300
        self.data = []
        self.last = self.first
        for i in range(1000):
            value = int(100 * cos(i))
            self.data.append((self.last, value))
            self.last += interval + ((interval/5) * (random() - .5))
            
    def tearDown(self):
        super(TestRecords, self).tearDown()
        self.db.flushdb()

    def post(self, path, body, content_type):
        req = self.req(path, method="POST")
        req.content_type = content_type
        req.body = body

        return req.get_response(self.application)

    def get(self, path, accept):
        req = self.req(path, headers=dict(accept=accept), method="GET")
        return req.get_response(self.application)

    def test_get(self):
        response = self.get("/records/foo/bar/last", accept="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, "application/json")
        body = json.loads(response.body)
        self.assertEqual(body["data"], [])

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
