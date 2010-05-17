from webob import Request
from tsar.tsar import RedisResource, validate

from tests import AppTest, BaseTest

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
        self.assertRaises(ValueError, self.validate.Number, "foo")

    def test_time_simple(self):
        self.assertEqual(self.validate.Time("1"), 1)

    def test_time_float(self):
        self.assertEqual(self.validate.Time("1272286116.421756"), 1272286116)

    def test_time_relative(self):
        self.assertEqual(self.validate.Time("-10"), 1272286106)

    def test_time_badinput(self):
        self.assertRaises(ValueError, self.validate.Time, "foo")

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
        self.assertRaises(TypeError, self.foo, foo="b!ar",
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
