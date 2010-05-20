import math

from tsar.maintenance import consolidate

from tests import BaseTest, log

class TestConsolidate(BaseTest):
    
    def setUp(self):
        self.data = [(t, (t/5)**2) for t in range(1, 20, 3)]

    def test_consolidate_last(self):
        want = [(0, 0), (5, 1), (10, 4), (15, 9), (20, 9)]
        self.assertEqual(list(consolidate(self.data, 5)), want)
