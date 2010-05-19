import math

import webob.exc

from webob import Request
from tsar.tsar import Records, RedisResource, validate
from tsar.util import json

from tests import AppTest, BaseTest, log

class testvalidate(validate):

    def Time(self, value):
        return super(testvalidate, self).Time(value, now="1272286116.421756")

class TestValidate(BaseTest):
    
    def setUp(self):
        super(TestValidate, self).setUp()
        self.validate = testvalidate()

    def test_key(self):
        self.assertEqual(self.validate.Key("foo"), "foo")

    def test_key_int(self):
        self.assertEqual(self.validate.Key(1), "1")

    def test_key_float(self):
        self.assertEqual(self.validate.Key(10.034), "10.034")

    def test_key_toolong(self):
        self.assertRaises(TypeError, self.validate.Key, 150 * "a")

    def test_key_badchars(self):
        self.assertRaises(TypeError, self.validate.Key, "foo!bar")
        self.assertRaises(TypeError, self.validate.Key, "foo/bar")
        self.assertRaises(TypeError, self.validate.Key, "foo/!bar")

    def test_number(self):
        self.assertEqual(self.validate.Number("1"), 1)

    def test_number_number(self):
        self.assertEqual(self.validate.Number(1), 1)

    def test_number_big(self):
        self.assertEqual(self.validate.Number("4294967296"), 2**32)

    def test_number_float(self):
        self.assertTrue(isinstance(self.validate.Number("1.0"), float))

    def test_number_float_precision(self):
        self.assertEqual("%0.1f" % self.validate.Number("1.0"), "1.0")

    def test_number_float_precision3(self):
        self.assertEqual("%0.3f" % self.validate.Number("1.00002"), "1.000")

    def test_number_notanumber(self):
        self.assertRaises(TypeError, self.validate.Number, "foo")

    def test_time_simple(self):
        self.assertEqual(self.validate.Time("1"), 1)

    def test_time_float(self):
        self.assertEqual(self.validate.Time("1272286116.421756"), 1272286116)

    def test_time_relative(self):
        self.assertEqual(self.validate.Time("-10"), 1272286106)

    def test_time_badinput(self):
        self.assertRaises(TypeError, self.validate.Time, "foo")

    @testvalidate(foo="Key", start="Time", someint="Number", stop="Time")
    def foo(self, foo, someint, start, stop):
        self.assertEqual(foo, "bar")
        self.assertEqual(someint, 10.1)
        self.assertEqual(start, 1272286116)
        self.assertEqual(stop, 1272286106)

    @testvalidate(foo="Key")
    def default_foo(self, foo="baz"):
        self.assertEqual(foo, "baz")

    def test_validate_plaindict(self):
        self.foo(foo="bar", someint="10.1", start="1272286116.421756", stop="-10")

    def test_validate_req_params(self):
        req = Request.blank("/foo?foo=bar&someint=10.1&start=1272286116.421756&stop=-10")
        self.foo(**req.params)

    def test_validate_default(self):
        self.default_foo()

    def test_validate_missing(self):
        self.assertRaises(TypeError, self.foo, someint="10.1",
            start="1272286116.421756", stop="-10")

    def test_validate_badinput(self):
        self.assertRaises(webob.exc.HTTPBadRequest, self.foo, foo="b!ar",
            someint="10.1", start="1272286116.421756", stop="-10")

class TestRedisResource(BaseTest):
    
    def setUp(self):
        super(TestRedisResource, self).setUp()
        self.resource = RedisResource()

    def test_tokey(self):
        self.assertEqual(self.resource.tokey("foo", "bar"), "foo!bar")

    def test_fromkey(self):
        self.assertEqual(self.resource.fromkey("foo!bar"), ["foo", "bar"])

    def test_key_roundtrip(self):
        input = "foo!bar"
        self.assertEqual(self.resource.tokey(*self.resource.fromkey(input)), input)

class RecordsTest(BaseTest):

    def setUp(self):
        super(RecordsTest, self).setUp()
        self.records = Records(connection={"port": 16379})
        self.db = self.records.db
        self.db.flushall()
        self.postdict = dict(subject="bar", attribute="foo", stamp=1274110760, value=10)

    def tearDown(self):
        self.db.flushall()

class TestRecords(RecordsTest):

    def test_tovalue(self):
        self.assertEqual(self.records.tovalue("1274110760", "10"), "1274110760!10")

    def test_tovalue_invalid(self):
        self.assertRaises(webob.exc.HTTPBadRequest, self.records.tovalue, "foo", "bar")

    def test_fromvalue(self):
        self.assertEqual(self.records.fromvalue("1274110760!10"), (1274110760, 10))

    def test_fromvalue_invalid(self):
        self.assertRaises(TypeError, self.records.fromvalue, "foo!10")

    def test_fromvalue_notavalue(self):
        self.assertRaises(TypeError, self.records.fromvalue, "12312030123")

    def test_create(self):
        self.records.create("foo", "bar", 1274110760, 10)
        self.assertEqual(self.db.zcard("records!foo!bar"), 1)

    def test_create_invalid(self):
        self.assertRaises(webob.exc.HTTPBadRequest, self.records.create,
            "foo!", "bar", 1274110760, 10)
        self.assertEqual(self.db.keys("*"), [])

class TestRecordsPost(RecordsTest):

    def test_post_form(self):
        req = Request.blank("/record", POST=self.postdict)
        response = req.get_response(self.records)
        self.assertEqual(response.status_int, 201)

    def test_post_json(self):
        req = Request.blank("/record", method="POST", body=json.dumps(self.postdict))
        req.content_type = "application/json"
        response = req.get_response(self.records)
        self.assertEqual(response.status_int, 201)

class TestRecordsGet(RecordsTest):

    def setUp(self):
        super(TestRecordsGet, self).setUp()
        time = 1274203741
        pipe = self.db.pipeline()
        for interval in Records.intervals:
            _time = time
            key = "records!foo!bar!%s!average" % interval.interval
            for i in range(interval.samples):
                pipe.zadd(key, "%d!%d" % (_time, 100 + 75 * math.cos(i)), _time)
                _time -= interval.interval
        pipe.execute()

    def test_list_start_bigger_than_stop(self):
        self.assertRaises(webob.exc.HTTPBadRequest, self.records.list,
            "foo", "bar", 1274203741, 1274203731)

    def test_list_nothing(self):
        results = self.records.list("dne", "dne", 1274103741, 1274203741)
        self.assertEqual(len(results["records"]), 0)

    def test_list_interval_60(self):
        results = self.records.list("foo", "bar", 1274160541, 1274203741)
        self.assertEqual(results["interval"], 60)
        records = results.pop("records")
        self.assertEqual(results,
            {'attribute': 'bar', 'interval': 60, 'cf': 'average',
            'samples': 720, 'subject': 'foo'})
        self.assertEqual(len(records), 720)
        self.assertTrue(records[0][0] < records[-1][0])
    
    def test_list_odd_range(self):
        results = self.records.list("foo", "bar", 1271131741, 1271774541)
        records = results.pop("records")
        self.assertEqual(results["interval"], 86400)
        self.assertEqual(len(records), 7)

    def test_get_json_v1(self):
        req = Request.blank(
            "/record?subject=foo&attribute=bar&start=1274160541&stop=1274203741",
            accept="application/vnd.tsar.record.v1+json")
        response = req.get_response(self.records)
        results = json.loads(response.body)
        self.assertEqual(response.content_type, "application/vnd.tsar.record.v1+json")
        self.assertEqual(len(results["records"]), 720)

    def test_get_json_generic(self):
        req = Request.blank(
            "/record?subject=foo&attribute=bar&start=1274160541&stop=1274203741",
            accept="application/json")
        response = req.get_response(self.records)
        results = json.loads(response.body)
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(len(results["records"]), 720)
