# coding: utf-8
# Copyright 2010 Mark L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bson import BSON, ObjectId
from twisted.internet import defer
from twisted.trial import unittest
import txmongo
from txmongo.protocol import MongoClientProtocol
from collections import OrderedDict

mongo_host = "localhost"
mongo_port = 27017


class _CallCounter(object):
    def __init__(self, original):
        self.call_count = 0
        self.original = original

    def __call__(self, this, *args, **kwargs):
        self.call_count += 1
        return self.original(this, *args, **kwargs)


class TestMongoQueries(unittest.TestCase):

    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def test_SingleCursorIteration(self):
        yield self.coll.insert([{'v': i} for i in range(10)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 10)

    @defer.inlineCallbacks
    def test_MultipleCursorIterations(self):
        yield self.coll.insert([{'v': i} for i in range(450)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 450)

    @defer.inlineCallbacks
    def test_FindWithCursor(self):
        yield self.coll.insert([{'v': i} for i in range(750)], safe=True)
        docs, d = yield self.coll.find_with_cursor()
        self.assertEqual(len(docs), 101)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 750)

        # Same thing, but with the "cursor" keyword argument on find()
        docs, d = yield self.coll.find(cursor=True)
        self.assertEqual(len(docs), 101)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 750)

    @defer.inlineCallbacks
    def test_FindWithCursorLimit(self):
        yield self.coll.insert([{'v': i} for i in range(750)], safe=True)

        docs, d = yield self.coll.find_with_cursor(limit=150)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 150)

        # Same using find(cursor=True)
        docs, d = yield self.coll.find(limit=150, cursor=True)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 150)

    @defer.inlineCallbacks
    def test_LargeData(self):
        yield self.coll.insert([{'v': ' '*(2**19)} for _ in range(4)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 4)

    @defer.inlineCallbacks
    def test_SpecifiedFields(self):
        yield self.coll.insert([{k: v for k in "abcdefg"} for v in range(5)], safe=True)
        res = yield self.coll.find(fields={'a': 1, 'c': 1})
        yield self.coll.count(fields={'a': 1, 'c': 1})
        self.assertEqual(res[0].keys(), ['a', 'c', "_id"])
        res = yield self.coll.find(fields=['a', 'c'])
        yield self.coll.count(fields=['a', 'c'])
        self.assertEqual(res[0].keys(), ['a', 'c', "_id"])
        res = yield self.coll.find(fields=[])
        yield self.coll.count(fields=[])
        self.assertEqual(res[0].keys(), ["_id"])
        self.assertRaises(TypeError, self.coll._fields_list_to_dict, [1])

    @defer.inlineCallbacks
    def test_group(self):
        yield self.coll.insert([{'v': i % 2} for i in range(5)], safe=True)
        reduce_ = """
        function(curr, result) {
            result.total += curr.v;
        }
        """
        keys = {'v': 1}
        initial = {"total": 0}
        cond = {'v': {"$in": [0, 1]}}
        final = """
        function(result) {
            result.five = 5;
        }
        """
        res = yield self.coll.group(keys, initial, reduce_, cond, final)
        self.assertEqual(len(res["retval"]), 2)

        keys = """
        function(doc) {
            return {"value": 5, 'v': 1};
        }
        """

        res = yield self.coll.group(keys, initial, reduce_, cond, final)
        self.assertEqual(len(res["retval"]), 1)

    def __make_big_object(self):
        return {"_id": ObjectId(), 'x': 'a' * 1000}

    @defer.inlineCallbacks
    def test_CursorClosing(self):
        # Calculate number of objects in 4mb batch
        obj_count_4mb = 4 * 1024**2 / len(BSON.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert([self.__make_big_object() for _ in range(first_batch + obj_count_4mb)])
        result = yield self.coll.find(limit=first_batch)

        self.assertEqual(len(result), 5)

        status = yield self.db["$cmd"].find_one({"serverStatus": 1})
        self.assertEqual(status["metrics"]["cursor"]["open"]["total"], 0)

    @defer.inlineCallbacks
    def test_CursorClosingWithCursor(self):
        # Calculate number of objects in 4mb batch
        obj_count_4mb = 4 * 1024**2 / len(BSON.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert([self.__make_big_object() for _ in range(first_batch + obj_count_4mb)])

        result = []
        docs, dfr = yield self.coll.find_with_cursor({}, limit=first_batch)
        while docs:
            result.extend(docs)
            docs, dfr = yield dfr

        self.assertEqual(len(result), 5)

        status = yield self.db["$cmd"].find_one({"serverStatus": 1})
        self.assertEqual(status["metrics"]["cursor"]["open"]["total"], 0)

    @defer.inlineCallbacks
    def test_GetMoreCount(self):
        counter = _CallCounter(MongoClientProtocol.send_GETMORE)
        self.patch(MongoClientProtocol, 'send_GETMORE', counter)

        yield self.coll.insert([{'x': 42} for _ in range(20)])
        result = yield self.coll.find({}, limit=10)

        self.assertEqual(len(result), 10)
        self.assertEqual(counter.call_count, 0)

    @defer.inlineCallbacks
    def test_GetMoreCountWithCursor(self):
        counter = _CallCounter(MongoClientProtocol.send_GETMORE)
        self.patch(MongoClientProtocol, 'send_GETMORE', counter)

        yield self.coll.insert([{'x': 42} for _ in range(20)])

        result = []
        docs, dfr = yield self.coll.find_with_cursor({}, limit=5)
        while docs:
            result.extend(docs)
            docs, dfr = yield dfr

        self.assertEqual(len(result), 5)
        self.assertEqual(counter.call_count, 0)

    @defer.inlineCallbacks
    def test_AsClass(self):
        yield self.coll.insert({'x': 42})

        doc = yield self.coll.find_one({})
        self.assertIs(type(doc), dict)

        doc = yield self.coll.find_one({}, as_class=OrderedDict)
        self.assertIs(type(doc), OrderedDict)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


class TestMongoQueriesEdgeCases(unittest.TestCase):

    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_BelowBatchThreshold(self):
        yield self.coll.insert([{'v': i} for i in range(100)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 100)

    @defer.inlineCallbacks
    def test_EqualToBatchThreshold(self):
        yield self.coll.insert([{'v': i} for i in range(101)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 101)

    @defer.inlineCallbacks
    def test_AboveBatchThreshold(self):
        yield self.coll.insert([{'v': i} for i in range(102)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 102)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


class TestLimit(unittest.TestCase):

    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_LimitBelowBatchThreshold(self):
        yield self.coll.insert([{'v': i} for i in range(50)], safe=True)
        res = yield self.coll.find(limit=20)
        self.assertEqual(len(res), 20)

    @defer.inlineCallbacks
    def test_LimitAboveBatchThreshold(self):
        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=150)
        self.assertEqual(len(res), 150)

    @defer.inlineCallbacks
    def test_LimitAtBatchThresholdEdge(self):
        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=100)
        self.assertEqual(len(res), 100)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=101)
        self.assertEqual(len(res), 101)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=102)
        self.assertEqual(len(res), 102)

    @defer.inlineCallbacks
    def test_LimitAboveMessageSizeThreshold(self):
        yield self.coll.insert([{'v': ' '*(2**20)} for _ in range(8)], safe=True)
        res = yield self.coll.find(limit=5)
        self.assertEqual(len(res), 5)

    @defer.inlineCallbacks
    def test_HardLimit(self):
        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=-150)
        self.assertEqual(len(res), 150)

    @defer.inlineCallbacks
    def test_HardLimitAboveMessageSizeThreshold(self):
        yield self.coll.insert([{'v': ' '*(2**20)} for _ in range(8)], safe=True)
        res = yield self.coll.find(limit=-6)
        self.assertEqual(len(res), 4)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop(safe=True)
        yield self.conn.disconnect()


class TestSkip(unittest.TestCase):

    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_Skip(self):
        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=3)
        self.assertEqual(len(res), 2)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=5)
        self.assertEqual(len(res), 0)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=6)
        self.assertEqual(len(res), 0)

    @defer.inlineCallbacks
    def test_SkipWithLimit(self):
        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=3, limit=1)
        self.assertEqual(len(res), 1)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=4, limit=2)
        self.assertEqual(len(res), 1)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=4, limit=1)
        self.assertEqual(len(res), 1)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=5, limit=1)
        self.assertEqual(len(res), 0)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop(safe=True)
        yield self.conn.disconnect()
