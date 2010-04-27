from functools import partial

from webob import Request
from webob.exc import *

from tests import AppTest, BaseTest, log

from tsar import *

class TestDBResource(BaseTest):

    def setUp(self):
        self.resource = DBResource("foo")
        self.db_string = self.resource.db_string
        self.db_int = self.resource.db_int
        self.db_reltime_plain = self.resource.db_reltime
        self.db_reltime = partial(self.resource.db_reltime, now="1272286116.421756")
        self.validate = self.resource.validate
        self.encodeval = self.resource.encodeval
        self.decodeval = self.resource.decodeval
    
    def test_db_string_simple(self):
        self.assertEqual(self.db_string("foo"), "foo")

    def test_db_string_badchars(self):
        self.assertRaises(TypeError, self.db_string, "foo!")
        self.assertRaises(TypeError, self.db_string, "foo/")
        self.assertRaises(TypeError, self.db_string, "foo ")
        self.assertRaises(TypeError, self.db_string, 50 * "foo")

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
            "end": "-10",
        }
        result = self.validate(params,
            foo=self.db_string,
            start=self.db_reltime,
            someint=self.db_int,
            end=self.db_reltime)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["someint"], 10.1)
        self.assertEqual(result["start"], 1272286116)
        self.assertEqual(result["end"], 1272286106)

    def test_validate_req_params(self):
        req = Request.blank("/foo?foo=bar&someint=10.1&start=1272286116.421756&end=-10")
        result = self.validate(req.params,
            foo=self.db_string,
            start=self.db_reltime,
            someint=self.db_int,
            end=self.db_reltime)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["someint"], 10.1)
        self.assertEqual(result["start"], 1272286116)
        self.assertEqual(result["end"], 1272286106)

    def test_validate_default(self):
        result = self.validate({},
            foo=(self.db_string, "bar"))
        self.assertEqual(result["foo"], "bar")

    def test_validate_missing(self):
        self.assertRaises(HTTPBadRequest, self.validate, {}, foo=self.db_string)

    def test_validate_badinput(self):
        self.assertRaises(HTTPBadRequest, self.validate, {"foo": "bar"}, foo=self.db_int)
        self.assertRaises(HTTPBadRequest, self.validate, {"f!oo": "bar"}, foo=self.db_string)

    def test_encodeval(self):
        self.assertEqual(self.encodeval(1272286116, 10), "1272286116:10")

    def test_decodeval(self):
        self.assertEqual(self.decodeval("1272286116:10"), (1272286116, 10))

class TestResource(BaseTest):

    def setUp(self):
        self.record = Record("foo")
        self.sample = self.record.sample
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

class TestTsar(AppTest):

    def setUp(self):
        self.tsar = service
        self.application = service
