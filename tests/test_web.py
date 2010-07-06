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

    def test_post(self):
        req = self.req("/records/foo/bar/last", method="POST")
        req.content_type = "application/json"
        req.body = json.dumps({"data": self.data})
        response = req.get_response(self.application)
        self.assertEqual(response.status_int, 204)
