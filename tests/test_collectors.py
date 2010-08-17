from tests import BaseTest, log
from tsar.collectors.helpers import *

class TestHelpers(BaseTest):
    
    def test_replace_string(self):
        self.assertEqual(replace(range(10), 3, "abc"),
            [0, 1, 2, 'abc', 4, 5, 6, 7, 8, 9])
    
    def test_replace_list(self):
        self.assertEqual(replace(range(10), 3, list("abc")),
            [0, 1, 2, 'a', 'b', 'c', 6, 7, 8, 9])
    
    def test_replace_dbllist(self):
        self.assertEqual(replace(range(10), 3, [list("abc")]),
            [0, 1, 2, ['a', 'b', 'c'], 4, 5, 6, 7, 8, 9])
    
    def test_replace_int(self):
        self.assertEqual(replace(range(10), 3, 10),
            [0, 1, 2, 10, 4, 5, 6, 7, 8, 9])
    
    def test_insert_string(self):
        self.assertEqual(insert(range(10), 3, "abc"),
            [0, 1, 2, 'abc', 3, 4, 5, 6, 7, 8, 9])
    
    def test_insert_list(self):
        self.assertEqual(insert(range(10), 3, list("abc")),
            [0, 1, 2, 'a', 'b', 'c', 3, 4, 5, 6, 7, 8, 9])
    
    def test_insert_dbllist(self):
        self.assertEqual(insert(range(10), 3, [list("abc")]),
            [0, 1, 2, ['a', 'b', 'c'], 3, 4, 5, 6, 7, 8, 9])
    
    def test_insert_int(self):
        self.assertEqual(insert(range(10), 3, 10),
            [0, 1, 2, 10, 3, 4, 5, 6, 7, 8, 9])
