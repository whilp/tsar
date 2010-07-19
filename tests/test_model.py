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
        self.assertEquals(data, [(1278508740, 10)])

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
        data = [(t + i*80, i) for i in range(10)]
        for t, v in data:
            self.records.append((t, v))
        results = list(self.records.query(1278509023, 1278509452))# t, t + (10 * 80)))
        self.assertEquals(results, 
            [(1278509040, 4),
            (1278509100, 5),
            (1278509160, None),
            (1278509220, 6),
            (1278509280, 7),
            (1278509340, 8),
            (1278509400, None),
            (1278509460, 9)])

    def test_append_repeated_quick(self):
        t = 1279012518
        data = [(t + i, i) for i in range(10)]
        for t, v in data:
            self.records.append((t, v))
        results = list(self.records.query(t-60, t+60))
        self.assertEquals(results, [(1279011600, 9)])

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

    def test_consolidate(self):
        t = 1279004415
        data = [(t + i*80, i) for i in range(10)]
        result = list(self.records.consolidate(data, 60, self.records.cfs["last"]))
        self.assertEquals(result,
            [(1279004400, 0, 0),
             (1279004460, None, 0),
             (1279004520, 1, 0),
             (1279004580, 2, 0),
             (1279004640, 3, 0),
             (1279004700, None, 0),
             (1279004760, 4, 0),
             (1279004820, 5, 0),
             (1279004880, 6, 0),
             (1279004940, None, 0),
             (1279005000, 7, 0),
             (1279005060, 8, 0),
             (1279005120, 9, 0)])

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

    def test_consolidate_ave(self):
        t1, t2 = self.data[0][0], (self.data[-1][0] * 2)
        self.records.cf = "ave"
        self.records.extend(self.data)
        data = list(self.records.query(t1, t2))
        self.assertEqual(data,
            [(1278028800, -0.23000000000000001),
            (1278115200, 0.070000000000000007),
            (1278201600, -0.080000000000000002),
            (1278288000, 0.60999999999999999)])

    def test_consolidate_avedirect(self):
        ave = self.records.cfs["ave"]
        data = self.data[:15]
        result = list(self.records.consolidate(data, 3600, ave))
        bins = {}
        for t, v in data:
            t = model.nearest(t, 3600)
            bins.setdefault(t, [])
            bins[t].append(v)
        for k, v in bins.items():
            bins[k] = float(sum(v))/len(v)
        for t, v, i in result:
            self.assertEqual(bins[t], v)
