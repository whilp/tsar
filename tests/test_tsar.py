from functools import partial

from webob import Request
from webob.exc import *

from tests import AppTest, BaseTest, DBTest, log

from neat import Dispatch
from tsar import DBResource, Record

class Record(Record):
    dsn = {"db": 15}

dispatch = Dispatch(Record("records"))

class TestDBResource(BaseTest):

    def setUp(self):
        self.resource = DBResource("foo")
        self.db_key = self.resource.db_key
        self.db_int = self.resource.db_int
        self.db_reltime_plain = self.resource.db_reltime
        self.db_reltime = partial(self.resource.db_reltime, now="1272286116.421756")
        self.validate = self.resource.validate
        self.encodeval = self.resource.encodeval
        self.decodeval = self.resource.decodeval
    
    def test_db_key_simple(self):
        self.assertEqual(self.db_key("foo"), "foo")

    def test_db_key_badchars(self):
        self.assertRaises(TypeError, self.db_key, "foo!")
        self.assertRaises(TypeError, self.db_key, "foo/")
        self.assertRaises(TypeError, self.db_key, "foo ")
        self.assertRaises(TypeError, self.db_key, 50 * "foo")

    def test_db_int_simple(self):
        self.assertEqual(self.db_int("1"), 1)
        self.assertEqual(self.db_int("4294967296"), 2**32)

    def test_db_int_float(self):
        self.assertTrue(isinstance(self.db_int("1.0"), float))
        self.assertEqual("%0.1f" % self.db_int("1.0"), "1.0")
        self.assertEqual("%0.3f" % self.db_int("1.00002"), "1.000")

    def test_db_int_notanint(self):
        self.assertRaises(ValueError, self.db_int, "foo")

    def test_db_reltime_simple(self):
        self.assertEqual(self.db_reltime("1"), 1)

    def test_db_reltime_float(self):
        self.assertEqual(self.db_reltime("1272286116.421756"), 1272286116)

    def test_db_reltime_relative(self):
        self.assertEqual(self.db_reltime_plain("-10", 1272286116.421756), 1272286106)

    def test_db_reltime_badinput(self):
        self.assertRaises(ValueError, self.db_reltime, "foo")

    def test_validate_plaindict(self):
        params = {
            "foo": "bar",
            "someint": "10.1",
            "start": "1272286116.421756",
            "stop": "-10",
        }
        result = self.validate(params,
            foo=self.db_key,
            start=self.db_reltime,
            someint=self.db_int,
            stop=self.db_reltime)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["someint"], 10.1)
        self.assertEqual(result["start"], 1272286116)
        self.assertEqual(result["stop"], 1272286106)

    def test_validate_req_params(self):
        req = Request.blank("/foo?foo=bar&someint=10.1&start=1272286116.421756&stop=-10")
        result = self.validate(req.params,
            foo=self.db_key,
            start=self.db_reltime,
            someint=self.db_int,
            stop=self.db_reltime)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["someint"], 10.1)
        self.assertEqual(result["start"], 1272286116)
        self.assertEqual(result["stop"], 1272286106)

    def test_validate_default(self):
        result = self.validate({},
            foo=(self.db_key, "bar"))
        self.assertEqual(result["foo"], "bar")

    def test_validate_missing(self):
        self.assertRaises(HTTPBadRequest, self.validate, {}, foo=self.db_key)

    def test_validate_badinput(self):
        self.assertRaises(HTTPBadRequest, self.validate, {"foo": "bar"}, foo=self.db_int)
        self.assertRaises(HTTPBadRequest, self.validate, {"f!oo": "bar"}, foo=self.db_key)

    def test_encodeval(self):
        self.assertEqual(self.encodeval(1272286116, 10), "1272286116:10")

    def test_decodeval(self):
        self.assertEqual(self.decodeval("1272286116:10"), (1272286116, 10))

class TestSample(BaseTest):

    def setUp(self):
        self.sample = Record().sample
        self.input = range(10)

    def test_sample_simple(self):
        self.assertEqual(self.sample(self.input, 6), range(1, 10)[::2])

    def test_sample_conversion(self):
        self.assertEqual(self.sample(range(5), 3, lambda x: x*2), [2, 6])

    def test_sample_zero_size(self):
        self.assertEqual(self.sample(self.input, 0), self.input)

    def test_sample_negative_size(self):
        self.assertEqual(self.sample(self.input, -10), self.input)

    def test_sample_big_size(self):
        self.assertEqual(self.sample(self.input, 20), self.input)

    def test_sample_twicethesize(self):
        self.assertEqual(self.sample(range(20), 10), range(1, 20)[::2])

    def test_sample_notmuchbigger(self):
        self.assertEqual(self.sample(range(20), 17),
            [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 19])

class TestRecord(DBTest):
    application = dispatch

    @log
    def test_list_simple_json(self):
        response = self.app("/records.json")
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.body, '')

class FooTestTsarRecordEmpty(DBTest):
    resource = Record("records")

    def test_list_simple(self):
        response = self.resource.list(self.req(""))
        self.assertEqual(response.data, {"results": {}})

    def test_list_params(self):
        response = self.resource.list(self.req("/records?start=0&stop=-1&subject=foo"))
        self.assertEqual(response.data, {"results": {}})

    def test_list_impossible_times(self):
        req = self.req("/records?start=10&stop=8")
        self.assertRaises(HTTPBadRequest, self.resource.list, req)

class TestTsarRecordPopulated(DBTest):
    resource = Record("records")
    
    def setUp(self):
        super(TestTsarRecordPopulated, self).setUp()
        zadd = lambda k, t, v: self.redis.zadd(k, self.resource.encodeval(t, v), t)
        zadd("records!foo!bar", 10, 1272286106)
        zadd("records!foo!bar", 11, 1272286116)
        zadd("records!foo!bar", 12, 1272286126)
        zadd("records!foo!bar", 13, 1272286136)
        zadd("records!foo!bar", 14, 1272286146)
        zadd("records!foo!bar", 15, 1272286156)
        zadd("records!foo!bar", 16, 1272286166)
        zadd("records!foo!bar", 17, 1272286176)
        zadd("records!foo!bar", 18, 1272286186)
        zadd("records!spam!eggs", 0, 1272286186)

    def test_list_simple(self):
        response = self.resource.list(self.req(""))
        self.assertEqual(response.data["results"]["foo"]["bar"][0], (10000, 1272286106))
        self.assertEqual(len(response.data["results"]["foo"]["bar"]), 9)
        self.assertEqual(response.data["results"]["spam"]["eggs"], [(0, 1272286186)])

    def test_list_subject(self):
        response = self.resource.list(self.req("/records?subject=foo"))
        self.assertEqual(len(response.data["results"]["foo"]["bar"]), 9)

    def test_list_attribute(self):
        response = self.resource.list(self.req("/records?attribute=bar"))
        self.assertEqual(len(response.data["results"]["foo"]["bar"]), 9)

    def test_list_nonexistent_key(self):
        response = self.resource.list(self.req("/records?attribute=doesnotexist"))
        self.assertEqual(len(response.data["results"]), 0)
