from math import cos

from tsar import util
from tests import BaseTest, log, unittest

class TestFilters(BaseTest):
    first = 1278007837
    
    def setUp(self):
        interval = 60
        self.data = []
        self.last = self.first
        for i in xrange(1000):
            value = None
            if i % 45 != 0:
                value = int(100 * cos(i))
            self.data.append((self.last, value))
            self.last += interval

    def test_derive(self):
        data = list(util.derive(self.data))
        # Because self.data[2] has None as a neighbor
        self.assertEqual(data[0][0], self.data[3][0])
        self.assertEqual(len(self.data) - len(data), 5)
        f = open("data.json", 'w')
        import json
        json.dump({"data": self.data, "derive": data}, f)
