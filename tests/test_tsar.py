from tests import AppTest, BaseTest, log

from tsar import *

class TestDBResource(BaseTest):

    def setUp(self):
        self.resource = DBResource("foo")
        self.db_string = self.resource.db_string
        self.db_int = self.resource.db_int
        self.db_reltime = self.resource.db_reltime
    
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

class TestTsar(AppTest):

    def setUp(self):
        self.tsar = service
        self.application = service
