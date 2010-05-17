from tsar.tsar import validate

from tests import AppTest, BaseTest

class TestValidate(BaseTest):
    
    def setUp(self):
        super(TestValidate, self).setUp()
        self.validate = validate()

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
