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
        self.records.append((1278508719, 10))

    def test_append_badtime(self):
        self.assertRaises(TypeError, self.records.append, ("foo", 10))

    def test_append_badval(self):
        self.assertRaises(TypeError, self.records.append, (1278508719, "foo"))

    def test_append_oldtime(self):
        t = 1278508719
        self.records.append((t, 10))
        self.assertRaises(errors.RecordError, self.records.append, (t-100, 11))

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
        self.assertEqual(data[0], (1278007200, 100))
        self.assertEqual(data[-1], (1278320400, -63))

    def test_query_lowstart(self):
        t1, t2 = 0, self.data[-1][0]
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, 100), (1278115200, 22), 
            (1278201600, -23), (1278288000, 30)])

    def test_query_highstop(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, 100), (1278115200, 22), 
            (1278201600, -23), (1278288000, 30)])
