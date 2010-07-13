from math import cos

from tsar import errors, model

from tests import AppTest, BaseTest, log, unittest

model.db = model.connect(db=15)

class TestRecords(BaseTest):
    first = 1278007837
    
    def setUp(self):
        super(TestRecords, self).setUp()
        self.db = model.db
        self.db.flushdb()
        self.records = model.Records("foo", "bar", "last")

        interval = 300
        self.data = []
        self.last = self.first
        for i in range(1000):
            value = int(100 * cos(i))
            self.data.append((self.last, value))
            self.last += interval + ((interval/5) * (.709 - .5))
            
    def tearDown(self):
        super(TestRecords, self).tearDown()
        self.db.flushdb()

    def test_append(self):
        t = 1278508719
        self.records.append((t, 10))
        data = list(self.records.query(t - 60, t + 60))
        self.assertEquals(data, [])

    def test_append_badtime(self):
        self.assertRaises(TypeError, self.records.append, ("foo", 10))

    def test_append_badval(self):
        self.assertRaises(TypeError, self.records.append, (1278508719, "foo"))

    def test_append_oldtime(self):
        t = 1278508719
        self.records.append((t, 10))
        self.assertRaises(errors.RecordError, self.records.append, (t-10000, 11))

    def test_append_repeated(self):
        t = 1278508719
        data = [(t + i*80, i) for i in range(20)]
        for t, v in data:
            self.records.append((t, v))
        data = list(self.records.query(t, t + (20 * 80)))
        self.assertEquals(data, [])

    def test_extend_quick(self):
        t = 1278508719
        data = [(t + i*10, i) for i in range(20)]
        self.records.extend(data)
        result = list(self.records.query(t, t + 20 * 10))
        self.assertEquals(result, 
            [(1278508740, 5), (1278508800, 11), (1278508860, 17), (1278508920, 19)])

    def test_extend(self):
        self.records.extend(self.data)

    def test_extend_oldtime(self):
        self.records.extend(self.data)
        self.assertRaises(errors.RecordError, self.records.extend, self.data)

    def test_query(self):
        t1, t2 = self.data[0][0], self.data[-1][0]
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(len(data), 88)
        self.assertEqual(data[0], (1278007200, -98))
        self.assertEqual(data[-1], (1278320400, 99))

    def test_query_lowstart(self):
        t1, t2 = 0, self.data[-1][0]
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, -69), (1278115200, -94), 
            (1278201600, -64), (1278288000, 99)])

    def test_query_highstop(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, -69), (1278115200, -94), 
            (1278201600, -64), (1278288000, 99)])

    def test_query_now(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, -69), (1278115200, -94),
            (1278201600, -64), (1278288000, 99)])

    def test_consolidate_min(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.cf = "min"
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, -99), (1278115200, -99), 
            (1278201600, -99), (1278288000, -99)])

    def test_consolidate_max(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.cf = "max"
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, 100), (1278115200, 99), 
            (1278201600, 99), (1278288000, 99)])
