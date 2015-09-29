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

from __future__ import absolute_import, division

from bson import BSON, ObjectId
from bson.son import SON
from pymongo.errors import OperationFailure, WriteError
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, \
    DeleteResult
from pymongo.collection import ReturnDocument
from pymongo.write_concern import WriteConcern
from twisted.internet import defer
from twisted.trial import unittest
import txmongo
from txmongo.protocol import MongoClientProtocol
import txmongo.filter as qf

mongo_host = "localhost"
mongo_port = 27017


class _CallCounter(object):
    def __init__(self, original):
        self.call_count = 0
        self.original = original

    def __call__(self, this, *args, **kwargs):
        self.call_count += 1
        return self.original(this, *args, **kwargs)


class _SingleCollectionTest(unittest.TestCase):

    def setUp(self):
        self.conn = txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


class TestMongoQueries(_SingleCollectionTest):

    timeout = 15

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
        yield self.coll.insert([dict((k, v) for k in "abcdefg") for v in range(5)], safe=True)
        res = yield self.coll.find(fields={'a': 1, 'c': 1})
        yield self.coll.count(fields={'a': 1, 'c': 1})
        self.assertTrue(all(x in ['a', 'c', "_id"] for x in res[0].keys()))
        res = yield self.coll.find(fields=['a', 'c'])
        yield self.coll.count(fields=['a', 'c'])
        self.assertTrue(all(x in ['a', 'c', "_id"] for x in res[0].keys()))
        res = yield self.coll.find(fields=[])
        yield self.coll.count(fields=[])
        self.assertTrue(all(x in ["_id"] for x in res[0].keys()))
        yield self.assertFailure(self.coll.find({}, fields=[1]), TypeError)

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
    def __check_no_open_cursors(self):
        status = yield self.db.command("serverStatus")
        if "cursor" in status["metrics"]:
            self.assertEqual(status["metrics"]["cursor"]["open"]["total"], 0)
        else:
            self.assertEqual(status["cursors"]["totalOpen"], 0)

    @defer.inlineCallbacks
    def test_CursorClosing(self):
        # Calculate number of objects in 4mb batch
        obj_count_4mb = 4 * 1024**2 // len(BSON.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert(
            [self.__make_big_object() for _ in range(first_batch + obj_count_4mb)])
        result = yield self.coll.find(limit=first_batch)

        self.assertEqual(len(result), 5)

        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_CursorClosingWithCursor(self):
        # Calculate number of objects in 4mb batch
        obj_count_4mb = 4 * 1024**2 // len(BSON.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert(
            [self.__make_big_object() for _ in range(first_batch + obj_count_4mb)])

        result = []
        docs, dfr = yield self.coll.find_with_cursor({}, limit=first_batch)
        while docs:
            result.extend(docs)
            docs, dfr = yield dfr

        self.assertEqual(len(result), 5)

        yield self.__check_no_open_cursors()

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
        self.assertTrue(type(doc) is dict)

        class CustomDict(dict):
            pass

        doc = yield self.coll.find_one({}, as_class=CustomDict)
        self.assertTrue(type(doc) is CustomDict)

    @defer.inlineCallbacks
    def test_FindOneNone(self):
        doc = yield self.coll.find_one()
        self.assertEqual(doc, None)


class TestMongoQueriesEdgeCases(_SingleCollectionTest):

    timeout = 15

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


class TestLimit(_SingleCollectionTest):

    timeout = 15

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

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(200)], safe=True)
        res = yield self.coll.find(limit=101)
        self.assertEqual(len(res), 101)

        yield self.coll.drop()

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


class TestSkip(_SingleCollectionTest):

    timeout = 15

    @defer.inlineCallbacks
    def test_Skip(self):
        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=3)
        self.assertEqual(len(res), 2)

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=5)
        self.assertEqual(len(res), 0)

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=6)
        self.assertEqual(len(res), 0)

    @defer.inlineCallbacks
    def test_SkipWithLimit(self):
        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=3, limit=1)
        self.assertEqual(len(res), 1)

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=4, limit=2)
        self.assertEqual(len(res), 1)

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=4, limit=1)
        self.assertEqual(len(res), 1)

        yield self.coll.drop()

        yield self.coll.insert([{'v': i} for i in range(5)], safe=True)
        res = yield self.coll.find(skip=5, limit=1)
        self.assertEqual(len(res), 0)


class TestCommand(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_SimpleCommand(self):
        pong = yield self.db.command("ping")
        self.assertEqual(pong["ok"], 1)

    @defer.inlineCallbacks
    def test_ComplexCommand(self):
        yield self.coll.insert([{'x': 42}, {'y': 123}], safe=True)

        # In form of command name, value and additional params
        result = yield self.db.command("count", "mycol", query={'x': 42})
        self.assertEqual(result['n'], 1)

        # In form of SON object
        result = yield self.db.command(SON([("count", "mycol"), ("query", {'y': 123})]))
        self.assertEqual(result['n'], 1)

    @defer.inlineCallbacks
    def test_CheckResult(self):
        yield self.coll.insert([{'x': 42}, {'y': 123}], safe=True)

        # missing 'deletes' argument
        self.assertFailure(self.db.command("delete", "mycol"), OperationFailure)

        result = yield self.db.command("delete", "mycol", check=False)
        self.assertFalse(result["ok"])

        result = yield self.db.command("delete", "mycol", check=True,
                                       allowable_errors=["missing deletes field"])
        self.assertFalse(result["ok"])


class TestUpdate(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_SimpleUpdate(self):
        yield self.coll.insert([{'x': 42}, {'x': 123}])

        yield self.coll.update({}, {"$set": {'x': 456}})

        docs = yield self.coll.find(fields={"_id": 0})

        # Check that only one document was updated
        self.assertTrue({'x': 456} in docs)
        self.assertTrue(({'x': 42} in docs) or ({'x': 123} in docs))

    @defer.inlineCallbacks
    def test_MultiUpdate(self):
        yield self.coll.insert([{'x': 42}, {'x': 123}])

        yield self.coll.update({}, {"$set": {'x': 456}}, multi=True)

        docs = yield self.coll.find(fields={"_id": 0})

        self.assertEqual(len(docs), 2)
        self.assertTrue(all(doc == {'x': 456} for doc in docs))

    @defer.inlineCallbacks
    def test_Upsert(self):
        yield self.coll.update({}, {"$set": {'x': 42}}, upsert=True)
        yield self.coll.update({}, {"$set": {'x': 123}}, upsert=True)

        docs = yield self.coll.find(fields={"_id": 0})

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0], {'x': 123})


class TestSave(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Save(self):
        yield self.assertFailure(self.coll.save(123), TypeError)

        yield self.coll.save({'x': 1})
        oid = ObjectId()
        yield self.coll.save({"_id": oid, 'x': 2})
        yield self.coll.save({"_id": oid, 'x': 3})

        docs = yield self.coll.find()
        self.assertTrue(any(doc['x'] == 1 for doc in docs))
        self.assertTrue({"_id": oid, 'x': 3} in docs)


class TestRemove(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_RemoveOne(self):
        docs = [{'x': 42}, {'x': 123}]
        yield self.coll.insert(docs)
        yield self.coll.remove({}, single=True)

        remaining = yield self.coll.find()
        self.assertEqual(len(remaining), 1)
        self.assertTrue(remaining[0] in docs)

    @defer.inlineCallbacks
    def test_RemoveMulti(self):
        yield self.coll.insert([{'x': 42}, {'x': 123}, {'y': 456}])
        yield self.coll.remove({'x': {"$exists": True}})

        remaining = yield self.coll.find(fields={"_id": 0})
        self.assertEqual(remaining, [{'y': 456}])

    @defer.inlineCallbacks
    def test_RemoveById(self):
        oid = ObjectId()
        yield self.coll.insert([{"_id": oid, 'x': 42}, {'y': 123}])
        yield self.coll.remove(oid)

        remaining = yield self.coll.find(fields={"_id": 0})
        self.assertEqual(remaining, [{'y': 123}])

    def test_RemoveInvalid(self):
        self.assertFailure(self.coll.remove(123), TypeError)


class TestDistinct(unittest.TestCase):

    def setUp(self):
        self.conn = txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()

    @defer.inlineCallbacks
    def test_Simple(self):
        yield self.coll.insert([{'x': 13}, {'x': 42}, {'x': 13}])

        d = yield self.coll.distinct('x')
        self.assertEqual(set(d), {13, 42})

    @defer.inlineCallbacks
    def test_WithQuery(self):
        yield self.coll.insert([{'x': 13}, {'x': 42}, {'x': 123}, {'x': 42}])

        d = yield self.coll.distinct('x', {'x': {"$gt": 20}})
        self.assertEqual(set(d), {42, 123})


class TestMapReduce(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_MapReduce(self):
        yield self.coll.insert([
            {"kid": "John", "grade": 5},
            {"kid": "Kate", "grade": 4},
            {"kid": "John", "grade": 4},
            {"kid": "Kate", "grade": 4},
            {"kid": "Adam", "grade": 4},
            {"kid": "Kate", "grade": 2},
            {"kid": "John", "grade": 5},
        ])

        t_map = """
            function () {
                emit(this.kid, this.grade);
            }
        """

        t_reduce = """
            function (key, values) {
                return Array.sum(values);
            }
        """

        result = yield self.coll.map_reduce(t_map, t_reduce, out={"inline": 1})
        self.assertEqual(len(result), 3)
        self.assertTrue({"_id": "John", "value": 14} in result)
        self.assertTrue({"_id": "Kate", "value": 10} in result)
        self.assertTrue({"_id": "Adam", "value": 4} in result)

        result = yield self.coll.map_reduce(t_map, t_reduce, out={"inline": 1}, full_response=True)
        self.assertTrue(result["ok"], 1)
        self.assertTrue("counts" in result)
        self.assertTrue("results" in result)


class TestInsertOne(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.insert_one({'x': 42})
        self.assertTrue(isinstance(result, InsertOneResult))
        self.assertEqual(result.acknowledged, True)
        self.assertTrue(isinstance(result.inserted_id, ObjectId))

        count = yield self.coll.count()
        self.assertEqual(count, 1)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        oid = ObjectId()
        doc = {'x': 42, "_id": oid}
        result = yield self.coll.with_options(write_concern=WriteConcern(w=0)).insert_one(doc)
        self.assertEqual(result.acknowledged, False)
        self.assertEqual(result.inserted_id, oid)

        # It's ok to issue count() right after unacknowledged insert because
        # we have exactly one connection
        count = yield self.coll.count()
        self.assertEqual(count, 1)


class TestInsertMany(_SingleCollectionTest):

    @defer.inlineCallbacks
    def test_InvalidArg(self):
        yield self.assertFailure(self.coll.insert_many({'x': 42}), TypeError)

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.insert_many([{'x': 42} for _ in range(100)])
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertEqual(result.acknowledged, True)

        docs = yield self.coll.find()
        ids = set(doc["_id"] for doc in docs)

        self.assertEqual(ids, set(result.inserted_ids))

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        result = yield self.coll.with_options(write_concern=WriteConcern(w=0))\
                                .insert_many([{'x': 42} for _ in range(100)])
        self.assertTrue(isinstance(result, InsertManyResult))
        self.assertEqual(result.acknowledged, False)

        docs = yield self.coll.find()
        ids = set(doc["_id"] for doc in docs)

        self.assertEqual(ids, set(result.inserted_ids))

    @defer.inlineCallbacks
    def test_OrderedAck(self):
        docs = [{'x': 1}, {'x': 2, '$': "error"}, {'x': 3}]
        yield self.assertFailure(self.coll.insert_many(docs), WriteError)

        count = yield self.coll.count()
        self.assertEqual(count, 1)

    @defer.inlineCallbacks
    def test_OrderedUnack(self):
        docs = [{'x': 1}, {'x': 2, '$': "error"}, {'x': 3}]
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield coll.insert_many(docs)

        count = yield self.coll.count()
        self.assertEqual(count, 1)

    @defer.inlineCallbacks
    def test_Unordered(self):
        docs = [{'x': 1}, {'x': 2, '$': "error"}, {'x': 3}]
        yield self.assertFailure(self.coll.insert_many(docs, ordered=False), WriteError)

        count = yield self.coll.count()
        self.assertEqual(count, 2)

    @defer.inlineCallbacks
    def test_UnorderedUnack(self):
        docs = [{'x': 1}, {'x': 2, '$': "error"}, {'x': 3}]
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield coll.insert_many(docs, ordered=False)

        count = yield self.coll.count()
        self.assertEqual(count, 2)


class TestUpdateOne(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestUpdateOne, self).setUp()
        yield self.coll.insert_many([{'x': 1}, {'x': 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.update_one({'x': {"$exists": True}}, {"$set": {'y': 123}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.upserted_id, None)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_one({'x': {"$exists": True}}, {"$set": {'y': 123}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, False)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.update_one({'y': 123}, {"$set": {'z': 456}}, upsert=True)
        self.assertTrue(isinstance(result.upserted_id, ObjectId))

        doc = yield self.coll.find_one({"_id": result.upserted_id}, fields={"_id": 0})
        self.assertEqual(doc, {'y': 123, 'z': 456})

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_one({'y': 123}, {"$set": {'z': 456}}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    @defer.inlineCallbacks
    def test_InvalidUpdate(self):
        # update_one/update_many only allow $-operators, not whole document replace)
        yield self.assertFailure(self.coll.update_one({'x': 1}, {'y': 123}), ValueError)


class TestReplaceOne(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestReplaceOne, self).setUp()
        yield self.coll.insert_many([{'x': 1}, {'x': 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.replace_one({'x': {"$exists": True}}, {'y': 123})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.upserted_id, None)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.replace_one({'x': {"$exists": True}}, {'y': 123})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, False)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.replace_one({'x': 5}, {'y': 123}, upsert=True)
        self.assertTrue(isinstance(result.upserted_id, ObjectId))

        doc = yield self.coll.find_one({"_id": result.upserted_id}, fields={"_id": 0})
        self.assertEqual(doc, {'y': 123})

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.replace_one({'x': 5}, {'y': 123}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        doc = yield self.coll.find_one({"y": {"$exists": True}}, fields={"_id": 0})
        self.assertEqual(doc, {'y': 123})

    @defer.inlineCallbacks
    def test_InvalidReplace(self):
        # replace_one does not allow $-operators, only whole document replace
        yield self.assertFailure(self.coll.replace_one({'x': 1}, {"$set": {'y': 123}}),
                                 ValueError)


class TestUpdateMany(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestUpdateMany, self).setUp()
        yield self.coll.insert_many([{'x': 1}, {'x': 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.update_many({'x': {"$exists": True}}, {"$set": {'y': 5}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 2)
        self.assertEqual(result.modified_count, 2)
        self.assertEqual(result.upserted_id, None)

        cnt = yield self.coll.count({'y': 5})
        self.assertEqual(cnt, 2)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_many({'x': {"$exists": True}}, {"$set": {'y': 5}})
        self.assertTrue(isinstance(result, UpdateResult))
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count({'y': 5})
        self.assertEqual(cnt, 2)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.update_many({'x': 5}, {"$set": {'y': 5}}, upsert=True)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertTrue(isinstance(result.upserted_id, ObjectId))

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_many({'x': 5}, {"$set": {'y': 5}}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    @defer.inlineCallbacks
    def test_InvalidUpdate(self):
        # update_one/update_many only allow $-operators, not whole document replace)
        yield self.assertFailure(self.coll.update_many({'x': 1}, {'y': 123}), ValueError)


class TestDeleteOne(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestDeleteOne, self).setUp()
        yield self.coll.insert_many([{'x': 1}, {'x': 1}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.delete_one({'x': 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.deleted_count, 1)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.delete_one({'x': 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)


class TestDeleteMany(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestDeleteMany, self).setUp()
        yield self.coll.insert_many([{'x': 1}, {'x': 1}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.delete_many({'x': 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.deleted_count, 2)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 0)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.delete_many({'x': 1})
        self.assertTrue(isinstance(result, DeleteResult))
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 0)


class TestFindOneAndDelete(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestFindOneAndDelete, self).setUp()
        yield self.coll.insert_many([{'x': 1, 'y': 1},
                                     {'x': 2, 'y': 2},
                                     {'x': 3, 'y': 3}])

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_delete({'x': {"$exists": True}},
                                                  sort=qf.sort(qf.ASCENDING('y')))
        self.assertEqual(doc['x'], 1)

        doc = yield self.coll.find_one_and_delete({'x': {"$exists": True}},
                                                  sort=qf.sort(qf.DESCENDING('y')))
        self.assertEqual(doc['x'], 3)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Projection(self):
        doc = yield self.coll.find_one_and_delete({'x': 2}, {'y': 1, "_id": 0})
        self.assertEqual(doc, {'y': 2})


class TestFindOneAndReplace(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestFindOneAndReplace, self).setUp()
        yield self.coll.insert_many([{'x': 10, 'y': 10},
                                     {'x': 20, 'y': 20},
                                     {'x': 30, 'y': 30}])

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_replace({}, {'x': 5, 'y': 5},
                                                   projection={"_id": 0},
                                                   sort=qf.sort(qf.ASCENDING('y')))
        self.assertEqual(doc, {'x': 10, 'y': 10})

        doc = yield self.coll.find_one_and_replace({}, {'x': 40, 'y': 40},
                                                   projection={"_id": 0},
                                                   sort=qf.sort(qf.DESCENDING('y')))
        self.assertEqual(doc, {'x': 30, 'y': 30})

        ys = yield self.coll.distinct('y')
        self.assertEqual(set(ys), {5, 20, 40})

    @defer.inlineCallbacks
    def test_InvalidReplace(self):
        yield self.assertFailure(self.coll.find_one_and_replace({}, {"$set": {'z': 1}}), ValueError)

    @defer.inlineCallbacks
    def test_Upsert(self):
        doc = yield self.coll.find_one_and_replace({'x': 40}, {'x': 50}, upsert=True)
        self.assertEqual(doc, None)

        xs = yield self.coll.distinct('x')
        self.assertEqual(set(xs), {10, 20, 30, 50})

    @defer.inlineCallbacks
    def test_ReturnDocument(self):
        doc = yield self.coll.find_one_and_replace({'x': 10}, {'x': 15},
                                                   return_document=ReturnDocument.BEFORE)
        self.assertEqual(doc['x'], 10)

        doc = yield self.coll.find_one_and_replace({'x': 20}, {'x': 25},
                                                   return_document=ReturnDocument.AFTER)
        self.assertEqual(doc['x'], 25)

    @defer.inlineCallbacks
    def test_InvalidReturnDocument(self):
        yield self.assertFailure(self.coll.find_one_and_replace({}, {}, return_document=1),
                                 ValueError)


class TestFindOneAndUpdate(_SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestFindOneAndUpdate, self).setUp()
        yield self.coll.insert_many([{'x': 10, 'y': 10},
                                     {'x': 20, 'y': 20},
                                     {'x': 30, 'y': 30}])

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_update({}, {"$set": {'y': 5}},
                                                  projection={"_id": 0},
                                                  sort=qf.sort(qf.ASCENDING('y')))
        self.assertEqual(doc, {'x': 10, 'y': 10})

        doc = yield self.coll.find_one_and_update({}, {"$set": {'y': 35}},
                                                  projection={"_id": 0},
                                                  sort=qf.sort(qf.DESCENDING('y')))
        self.assertEqual(doc, {'x': 30, 'y': 30})

        ys = yield self.coll.distinct('y')
        self.assertEqual(set(ys), {5, 20, 35})

    def test_InvalidUpdate(self):
        self.assertFailure(self.coll.find_one_and_update({}, {'x': 123}), ValueError)

    @defer.inlineCallbacks
    def test_Upsert(self):
        doc = yield self.coll.find_one_and_update({'x': 40}, {"$set": {'y': 40}},
                                                  upsert=True)
        self.assertEqual(doc, None)

        inserted = yield self.coll.find_one({'x': 40})
        self.assertEqual(inserted['y'], 40)

    @defer.inlineCallbacks
    def test_ReturnDocument(self):
        doc = yield self.coll.find_one_and_update({'x': 10}, {"$set": {'y': 5}},
                                                  return_document=ReturnDocument.BEFORE)
        self.assertEqual(doc['y'], 10)

        doc = yield self.coll.find_one_and_update({'x': 10}, {"$set": {'y': 15}},
                                                  return_document=ReturnDocument.AFTER)
        self.assertEqual(doc['y'], 15)
