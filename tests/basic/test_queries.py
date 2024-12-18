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
import time
from unittest.mock import patch

import bson
from bson import CodecOptions, ObjectId
from bson.son import SON
from pymongo.collection import ReturnDocument
from pymongo.errors import (
    BulkWriteError,
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
    WriteError,
)
from pymongo.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from pymongo.write_concern import WriteConcern
from twisted.internet import defer

import txmongo.filter as qf
from tests.basic.utils import (
    only_for_mongodb_older_than,
    only_for_mongodb_starting_from,
)
from tests.utils import SingleCollectionTest, patch_send_msg
from txmongo.errors import TimeExceeded
from txmongo.protocol import MongoProtocol


class _CallCounter:
    def __init__(self, original):
        self.call_count = 0
        self.original = original

    def __call__(self, this, *args, **kwargs):
        self.call_count += 1
        return self.original(this, *args, **kwargs)


class TestFind(SingleCollectionTest):

    timeout = 15

    @defer.inlineCallbacks
    def test_FindReturnType(self):
        dfr = self.coll.find()
        dfr_one = self.coll.find_one()
        try:
            self.assertIsInstance(dfr, defer.Deferred)
            self.assertIsInstance(dfr_one, defer.Deferred)
        finally:
            yield dfr
            yield dfr_one

    @defer.inlineCallbacks
    def test_SingleCursorIteration(self):
        yield self.coll.insert_many([{"v": i} for i in range(10)])
        res = yield self.coll.find()
        self.assertEqual(len(res), 10)

    @defer.inlineCallbacks
    def test_MultipleCursorIterations(self):
        yield self.coll.insert_many([{"v": i} for i in range(450)])
        res = yield self.coll.find()
        self.assertEqual(len(res), 450)

    @defer.inlineCallbacks
    def test_FindWithCursor(self):
        yield self.coll.insert_many([{"v": i} for i in range(750)])
        docs, d = yield self.coll.find_with_cursor()
        self.assertEqual(len(docs), 101)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 750)

    @defer.inlineCallbacks
    def test_FindWithCursorLimit(self):
        yield self.coll.insert_many([{"v": i} for i in range(750)])

        docs, d = yield self.coll.find_with_cursor(limit=150)
        total = 0
        while docs:
            total += len(docs)
            docs, d = yield d
        self.assertEqual(total, 150)

    @defer.inlineCallbacks
    def test_FindWithCursorBatchSize(self):
        self.assertRaises(TypeError, self.coll.find_with_cursor, batch_size="string")

        yield self.coll.insert_many([{"v": i} for i in range(140)])

        docs, d = yield self.coll.find_with_cursor(batch_size=50)
        lengths = []
        while docs:
            lengths.append(len(docs))
            docs, d = yield d
        self.assertEqual(lengths, [50, 50, 40])

    @defer.inlineCallbacks
    def test_FindWithCursorBatchSizeLimit(self):
        yield self.coll.insert_many([{"v": i} for i in range(140)])

        docs, d = yield self.coll.find_with_cursor(batch_size=50, limit=10)
        lengths = []
        while docs:
            lengths.append(len(docs))
            docs, d = yield d
        self.assertEqual(lengths, [10])

    @defer.inlineCallbacks
    def test_FindWithCursorZeroBatchSize(self):
        yield self.coll.insert_many([{"v": i} for i in range(140)])

        docs, d = yield self.coll.find_with_cursor(batch_size=0)
        lengths = []
        while docs:
            lengths.append(len(docs))
            docs, d = yield d
        self.assertEqual(lengths, [101, 39])

    @defer.inlineCallbacks
    def test_LargeData(self):
        yield self.coll.insert_many([{"v": " " * (2**19)} for _ in range(4)])
        res = yield self.coll.find()
        self.assertEqual(len(res), 4)

    @defer.inlineCallbacks
    def test_SpecifiedFields(self):
        yield self.coll.insert_many([dict((k, v) for k in "abcdefg") for v in range(5)])

        res = yield self.coll.find(projection={"a": 1, "c": 1, "_id": 0})
        self.assertTrue(all(set(x.keys()) == {"a", "c"} for x in res))
        res = yield self.coll.find(projection=["a", "c"])
        self.assertTrue(all(set(x.keys()) == {"a", "c", "_id"} for x in res))
        res = yield self.coll.find(projection=[])
        self.assertTrue(all(set(x.keys()) == {"_id"} for x in res))
        yield self.assertFailure(self.coll.find({}, projection=[1]), TypeError)

        # Alternative form
        res = yield self.coll.find().projection({"a": 1, "c": 1, "_id": 0})
        self.assertTrue(all(set(x.keys()) == {"a", "c"} for x in res))
        res = yield self.coll.find().projection(["a", "c"])
        self.assertTrue(all(set(x.keys()) == {"a", "c", "_id"} for x in res))
        res = yield self.coll.find().projection([])
        self.assertTrue(all(set(x.keys()) == {"_id"} for x in res))
        yield self.assertFailure(self.coll.find().projection([1]), TypeError)

    def __make_big_object(self):
        return {"_id": ObjectId(), "x": "a" * 1000}

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
        obj_count_4mb = 4 * 1024**2 // len(bson.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert_many(
            [self.__make_big_object() for _ in range(first_batch + obj_count_4mb)]
        )
        result = yield self.coll.find(limit=first_batch)

        self.assertEqual(len(result), 5)

        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_CursorClosingWithCursor(self):
        # Calculate number of objects in 4mb batch
        obj_count_4mb = 4 * 1024**2 // len(bson.encode(self.__make_big_object())) + 1

        first_batch = 5
        yield self.coll.insert_many(
            [self.__make_big_object() for _ in range(first_batch + obj_count_4mb)]
        )

        result = []
        docs, dfr = yield self.coll.find_with_cursor({}, limit=first_batch)
        while docs:
            result.extend(docs)
            docs, dfr = yield dfr

        self.assertEqual(len(result), 5)

        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_TimeoutAndDeadline(self):
        yield self.coll.insert_many([{"a": i} for i in range(10)])

        # Success cases
        result = yield self.coll.find()
        self.assertEqual(len(result), 10)
        result = yield self.coll.find({"$where": "sleep(40); true"}, timeout=0.5)
        self.assertEqual(len(result), 10)
        result = yield self.coll.find(
            {"$where": "sleep(40); true"}, timeout=0.5, batch_size=2
        )
        self.assertEqual(len(result), 10)

        # Timeout cases
        dfr = self.coll.find({"$where": "sleep(55); true"}).timeout(0.5)
        yield self.assertFailure(dfr, TimeExceeded)
        dfr = self.coll.find({"$where": "sleep(55); true"}).timeout(0.5).batch_size(2)
        yield self.assertFailure(dfr, TimeExceeded)

        # Deadline cases
        dfr = self.coll.find({"$where": "sleep(55); true"}, deadline=time.time() + 0.5)
        yield self.assertFailure(dfr, TimeExceeded)
        dfr = self.coll.find(
            {"$where": "sleep(55); true"}, deadline=time.time() + 0.5, batch_size=2
        )
        yield self.assertFailure(dfr, TimeExceeded)

    @defer.inlineCallbacks
    def test_CursorClosingWithTimeout(self):
        yield self.coll.insert_many({"x": x} for x in range(10))

        # This $where will wait for 100ms for every object returned.
        # So the first batch will take 500ms to return and the second one
        # should fail with TimeExceeded.
        # We also check that find_with_cursor will properly close the cursor in this case
        # NB: $where is deprecated and this test may fail in future versions of MongoDB
        batch1, dfr = yield self.coll.find_with_cursor(
            {"$where": "sleep(100); true"}, batch_size=5, timeout=0.8
        )
        with patch_send_msg() as mock:
            with self.assertRaises(TimeExceeded):
                yield dfr

        self.assertTrue(
            any(
                [
                    "killCursors" in bson.decode(args[0][1].body)
                    for args in mock.call_args_list
                ]
            )
        )

        self.assertEqual(len(batch1), 5)

        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_AsClassCodecOption(self):
        yield self.coll.insert_one({"x": 42})

        doc = yield self.coll.find_one()
        self.assertIs(type(doc), dict)

        class CustomDict(dict):
            pass

        doc = yield self.coll.with_options(
            codec_options=CodecOptions(document_class=CustomDict)
        ).find_one()
        self.assertIs(type(doc), CustomDict)

    @defer.inlineCallbacks
    def test_FindOneNone(self):
        doc = yield self.coll.find_one()
        self.assertEqual(doc, None)

    @defer.inlineCallbacks
    def test_AllowPartialResults(self):
        with patch_send_msg() as mock:
            yield self.coll.find_one(allow_partial_results=True)

            mock.assert_called_once()
            msg = mock.call_args[0][1]
            cmd = bson.decode(msg.body)
            self.assertEqual(cmd["allowPartialResults"], True)

        with patch_send_msg() as mock:
            yield self.coll.find().limit(1).allow_partial_results()

            mock.assert_called_once()
            msg = mock.call_args[0][1]
            cmd = bson.decode(msg.body)
            self.assertEqual(cmd["allowPartialResults"], True)

    async def test_FindIterate(self):
        await self.coll.insert_many([{"b": i} for i in range(50)])

        sum_of_doc, doc_count = 0, 0
        async for doc in self.coll.find(batch_size=10):
            sum_of_doc += doc["b"]
            doc_count += 1

        self.assertEqual(sum_of_doc, 1225)
        self.assertEqual(doc_count, 50)

    async def test_FindIterateBatches(self):
        await self.coll.insert_many([{"a": i} for i in range(100)])

        all_batches_len = 0
        async for batch in self.coll.find(batch_size=10).batches():
            batch_len = len(batch)
            self.assertEqual(batch_len, 10)
            all_batches_len += batch_len

        self.assertEqual(all_batches_len, 100)

    async def test_FindIterateCloseCursor(self):
        await self.coll.insert_many([{"c": i} for i in range(50)])

        doc_count = 0
        async for _ in self.coll.find(batch_size=10):
            doc_count += 1
            if doc_count == 25:
                break

        self.assertEqual(doc_count, 25)

        await self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_IterateNextBatch(self):
        yield self.coll.insert_many([{"c": i} for i in range(50)])

        all_docs = []
        cursor = self.coll.find(batch_size=10)
        while not cursor.exhausted:
            batch = yield cursor.next_batch()
            all_docs.extend(batch)

        self.assertEqual(len(all_docs), 50)

        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_SettingOptionsAfterCommandIsSent(self):
        yield self.coll.insert_many([{"c": i} for i in range(50)])

        cursor = self.coll.find().batch_size(10)
        yield cursor.next_batch()

        # all these commands should raise InvalidOperation because query command is already sent
        self.assertRaises(InvalidOperation, cursor.projection, {"x": 1})
        self.assertRaises(InvalidOperation, cursor.sort, {"x": 1})
        self.assertRaises(InvalidOperation, cursor.hint, {"x": 1})
        self.assertRaises(InvalidOperation, cursor.comment, "hello")
        self.assertRaises(InvalidOperation, cursor.explain)
        self.assertRaises(InvalidOperation, cursor.skip, 10)
        self.assertRaises(InvalidOperation, cursor.limit, 10)
        self.assertRaises(InvalidOperation, cursor.batch_size, 10)
        self.assertRaises(InvalidOperation, cursor.allow_partial_results)
        self.assertRaises(InvalidOperation, cursor.timeout, 500)

        yield cursor.close()
        yield self.__check_no_open_cursors()

    @defer.inlineCallbacks
    def test_NextBatchBeforePreviousComplete(self):
        """If next_batch() is called before previous one is fired, it will return the same batch"""
        yield self.coll.insert_many([{"c": i} for i in range(50)])
        cursor = self.coll.find().batch_size(10)

        batches = yield defer.gatherResults([cursor.next_batch() for _ in range(5)])
        self.assertFalse(cursor.exhausted)
        for batch in batches[1:]:
            self.assertEqual(batch, batches[0])

        batch2 = yield cursor.next_batch()
        self.assertNotEqual(batch2, batches[0])

        yield cursor.close()

    @defer.inlineCallbacks
    def test_CursorId(self):
        yield self.coll.insert_many([{"c": i} for i in range(50)])

        cursor = self.coll.find().batch_size(45)
        yield cursor.next_batch()
        try:
            self.assertIsInstance(cursor.cursor_id, int)
            self.assertNotEqual(cursor.cursor_id, 0)
            self.assertIsNotNone(cursor.cursor_id)
        finally:
            yield cursor.close()

    def test_CursorCollection(self):
        cursor = self.coll.find().batch_size(45)
        self.assertIs(cursor.collection, self.coll)


class TestLimit(SingleCollectionTest):

    timeout = 15

    @defer.inlineCallbacks
    def test_LimitBelowBatchThreshold(self):
        yield self.coll.insert_many([{"v": i} for i in range(50)])
        res = yield self.coll.find(limit=20)
        self.assertEqual(len(res), 20)
        res = yield self.coll.find().limit(20)
        self.assertEqual(len(res), 20)

    @defer.inlineCallbacks
    def test_LimitAboveBatchThreshold(self):
        yield self.coll.insert_many([{"v": i} for i in range(200)])
        res = yield self.coll.find(limit=150)
        self.assertEqual(len(res), 150)
        res = yield self.coll.find().limit(150)
        self.assertEqual(len(res), 150)

    @defer.inlineCallbacks
    def test_LimitAtBatchThresholdEdge(self):
        yield self.coll.insert_many([{"v": i} for i in range(200)])
        for limit in [100, 101, 102]:
            res = yield self.coll.find(limit=limit, batch_size=100)
            self.assertEqual(len(res), limit)
            res = yield self.coll.find().limit(limit).batch_size(100)
            self.assertEqual(len(res), limit)

    @defer.inlineCallbacks
    def test_LimitAboveMessageSizeThreshold(self):
        yield self.coll.insert_many([{"v": " " * (2**20)} for _ in range(8)])
        res = yield self.coll.find(limit=5)
        self.assertEqual(len(res), 5)
        res = yield self.coll.find().limit(5)
        self.assertEqual(len(res), 5)

    @defer.inlineCallbacks
    def test_HardLimit(self):
        yield self.coll.insert_many([{"v": i} for i in range(200)])
        res = yield self.coll.find(limit=-150)
        self.assertEqual(len(res), 150)
        res = yield self.coll.find().limit(-150)
        self.assertEqual(len(res), 150)


class TestSkip(SingleCollectionTest):

    timeout = 15

    @defer.inlineCallbacks
    def test_Skip(self):
        yield self.coll.insert_many([{"v": i} for i in range(5)])

        tests = {
            2: 3,
            3: 2,
            5: 0,
            6: 0,
        }

        for skip, expected in tests.items():
            res = yield self.coll.find(skip=skip)
            self.assertEqual(len(res), expected)
            res = yield self.coll.find().skip(skip)
            self.assertEqual(len(res), expected)

    @defer.inlineCallbacks
    def test_SkipWithLimit(self):
        yield self.coll.insert_many([{"v": i} for i in range(5)])

        tests = {
            (3, 1): 1,
            (4, 2): 1,
            (4, 1): 1,
            (5, 1): 0,
        }

        for (skip, limit), expected in tests.items():
            res = yield self.coll.find(skip=skip, limit=limit)
            self.assertEqual(len(res), expected)
            res = yield self.coll.find().skip(skip).limit(limit)
            self.assertEqual(len(res), expected)


class TestCommand(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_SimpleCommand(self):
        pong = yield self.db.command("ping")
        self.assertEqual(pong["ok"], 1)

    @defer.inlineCallbacks
    def test_ComplexCommand(self):
        yield self.coll.insert_many([{"x": 42}, {"y": 123}])

        # In form of command name, value and additional params
        result = yield self.db.command("count", "mycol", query={"x": 42})
        self.assertEqual(result["n"], 1)

        # In form of SON object
        result = yield self.db.command(SON([("count", "mycol"), ("query", {"y": 123})]))
        self.assertEqual(result["n"], 1)

    @defer.inlineCallbacks
    def test_CheckResult(self):
        yield self.coll.insert_many([{"x": 42}, {"y": 123}])

        # missing 'deletes' argument
        self.assertFailure(self.db.command("delete", "mycol"), OperationFailure)

        result = yield self.db.command("delete", "mycol", check=False)
        self.assertFalse(result["ok"])

        result = yield self.db.command(
            "delete",
            "mycol",
            check=True,
            allowable_errors=[
                "missing deletes field",
                "The deletes option is required to the delete command.",
                "BSON field 'delete.deletes' is missing but a required field",
            ],
        )
        self.assertFalse(result["ok"])


class TestUpdate(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_SimpleUpdate(self):
        yield self.coll.insert_many([{"x": 42}, {"x": 123}])

        yield self.coll.update_one({}, {"$set": {"x": 456}})

        docs = yield self.coll.find(projection={"_id": 0})

        # Check that only one document was updated
        self.assertTrue({"x": 456} in docs)
        self.assertTrue(({"x": 42} in docs) or ({"x": 123} in docs))

    @defer.inlineCallbacks
    def test_MultiUpdate(self):
        yield self.coll.insert_many([{"x": 42}, {"x": 123}])

        yield self.coll.update_many({}, {"$set": {"x": 456}})

        docs = yield self.coll.find(projection={"_id": 0})

        self.assertEqual(len(docs), 2)
        self.assertTrue(all(doc == {"x": 456} for doc in docs))

    @defer.inlineCallbacks
    def test_Upsert(self):
        yield self.coll.update_one({}, {"$set": {"x": 42}}, upsert=True)
        yield self.coll.update_one({}, {"$set": {"x": 123}}, upsert=True)

        docs = yield self.coll.find(projection={"_id": 0})

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0], {"x": 123})


class TestDistinct(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Simple(self):
        yield self.coll.insert_many([{"x": 13}, {"x": 42}, {"x": 13}])

        d = yield self.coll.distinct("x")
        self.assertEqual(set(d), {13, 42})

    @defer.inlineCallbacks
    def test_WithQuery(self):
        yield self.coll.insert_many([{"x": 13}, {"x": 42}, {"x": 123}, {"x": 42}])

        d = yield self.coll.distinct("x", {"x": {"$gt": 20}})
        self.assertEqual(set(d), {42, 123})


class TestMapReduce(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_MapReduce(self):
        yield self.coll.insert_many(
            [
                {"kid": "John", "grade": 5},
                {"kid": "Kate", "grade": 4},
                {"kid": "John", "grade": 4},
                {"kid": "Kate", "grade": 4},
                {"kid": "Adam", "grade": 4},
                {"kid": "Kate", "grade": 2},
                {"kid": "John", "grade": 5},
            ]
        )

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

        result = yield self.coll.map_reduce(
            t_map, t_reduce, out={"inline": 1}, full_response=True
        )
        self.assertTrue(result["ok"], 1)
        self.assertTrue("results" in result)


class TestInsertOne(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.insert_one({"x": 42})
        self.assertIsInstance(result, InsertOneResult)
        self.assertEqual(result.acknowledged, True)
        self.assertIsInstance(result.inserted_id, ObjectId)

        count = yield self.coll.count()
        self.assertEqual(count, 1)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        oid = ObjectId()
        doc = {"x": 42, "_id": oid}
        result = yield self.coll.with_options(
            write_concern=WriteConcern(w=0)
        ).insert_one(doc)
        self.assertEqual(result.acknowledged, False)
        self.assertEqual(result.inserted_id, oid)

        # It's ok to issue count() right after unacknowledged insert because
        # we have exactly one connection
        count = yield self.coll.count()
        self.assertEqual(count, 1)

    @defer.inlineCallbacks
    def test_Failures(self):
        yield self.coll.insert_one({"_id": 1})

        yield self.assertFailure(self.coll.insert_one({"_id": 1}), DuplicateKeyError)
        yield self.coll.with_options(write_concern=WriteConcern(w=0)).insert_one(
            {"_id": 1}
        )

    @only_for_mongodb_older_than(
        [5, 0], "$ in field name is only forbidden in MongoDB<5.0"
    )
    def test_ForbiddenFieldNames(self):
        yield self.assertFailure(self.coll.insert_one({"$": 1}), WriteError)
        yield self.coll.with_options(write_concern=WriteConcern(w=0)).insert_one(
            {"$": 1}
        )


class TestInsertMany(SingleCollectionTest):

    def setUp(self):
        self.more_than_1k = [{"_id": i} for i in range(2016)]
        return super().setUp()

    def test_InvalidArg(self):
        self.assertRaises(TypeError, self.coll.insert_many, {"x": 42})

    @defer.inlineCallbacks
    def test_NoEmpty(self):
        yield self.assertFailure(self.coll.insert_many([]), InvalidOperation)

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.insert_many([{"x": 42} for _ in range(100)])
        self.assertIsInstance(result, InsertManyResult)
        self.assertEqual(result.acknowledged, True)

        docs = yield self.coll.find()
        ids = set(doc["_id"] for doc in docs)

        self.assertEqual(ids, set(result.inserted_ids))

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        result = yield self.coll.with_options(
            write_concern=WriteConcern(w=0)
        ).insert_many([{"x": 42} for _ in range(100)])
        self.assertIsInstance(result, InsertManyResult)
        self.assertEqual(result.acknowledged, False)

        docs = yield self.coll.find()
        self.assertEqual({doc["_id"] for doc in docs}, set(result.inserted_ids))

    @defer.inlineCallbacks
    def test_OrderedAck_Ok(self):
        result = yield self.coll.insert_many(self.more_than_1k)
        found = yield self.coll.find()
        self.assertEqual(len(result.inserted_ids), len(self.more_than_1k))
        self.assertEqual(len(found), len(self.more_than_1k))
        self.assertEqual(set(result.inserted_ids), {doc["_id"] for doc in found})

    @defer.inlineCallbacks
    def test_OrderedUnack_Ok(self):
        w_0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield w_0.insert_many(self.more_than_1k)
        found = yield self.coll.find()
        self.assertEqual(len(result.inserted_ids), len(self.more_than_1k))
        self.assertEqual(len(found), len(self.more_than_1k))
        self.assertEqual(set(result.inserted_ids), {doc["_id"] for doc in found})

    @defer.inlineCallbacks
    def test_OrderedAck_Fail(self):
        self.more_than_1k[500] = self.more_than_1k[499]
        error = yield self.assertFailure(
            self.coll.insert_many(self.more_than_1k), BulkWriteError
        )
        self.assertEqual(error.details["nInserted"], 500)
        self.assertEqual((yield self.coll.count()), 500)
        self.assertEqual(len(error.details["writeErrors"]), 1)
        self.assertEqual(error.details["writeErrors"][0]["index"], 500)
        self.assertEqual(error.details["writeErrors"][0]["op"], {"_id": 499})

    @defer.inlineCallbacks
    def test_OrderedUnack_Fail(self):
        self.more_than_1k[500] = self.more_than_1k[499]

        w_0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield w_0.insert_many(self.more_than_1k)
        self.assertEqual(len(result.inserted_ids), len(self.more_than_1k))
        found = yield self.coll.find()
        self.assertEqual(len(found), 500)
        self.assertEqual(
            {doc["_id"] for doc in found[:500]}, set(result.inserted_ids[:500])
        )

    @defer.inlineCallbacks
    def test_UnorderedAck_Fail(self):
        self.more_than_1k[500] = self.more_than_1k[499]
        error = yield self.assertFailure(
            self.coll.insert_many(self.more_than_1k, ordered=False), BulkWriteError
        )
        self.assertEqual(error.details["nInserted"], len(self.more_than_1k) - 1)
        self.assertEqual((yield self.coll.count()), len(self.more_than_1k) - 1)
        self.assertEqual(len(error.details["writeErrors"]), 1)
        self.assertEqual(error.details["writeErrors"][0]["index"], 500)
        self.assertEqual(error.details["writeErrors"][0]["op"], {"_id": 499})

    @defer.inlineCallbacks
    def test_UnorderedUnack_Fail(self):
        self.more_than_1k[500] = self.more_than_1k[499]

        w_0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield w_0.insert_many(self.more_than_1k, ordered=False)
        self.assertEqual(len(result.inserted_ids), len(self.more_than_1k))
        found = yield self.coll.find()
        self.assertEqual(len(found), len(self.more_than_1k) - 1)
        self.assertEqual(
            {doc["_id"] for doc in found}, set(result.inserted_ids) - {500}
        )

    @defer.inlineCallbacks
    def test_MoreThan16Mb(self):
        # 8mb x 5
        mb40 = [{"_id": i, "x": "y" * (8 * 1024**2)} for i in range(5)]

        result = yield self.coll.insert_many(mb40)
        self.assertEqual(result.inserted_ids, list(range(5)))
        found = yield self.coll.find()
        self.assertEqual(len(found), 5)
        total_size = sum(len(bson.encode(doc)) for doc in found)
        self.assertGreater(total_size, 40 * 1024**2)

    @defer.inlineCallbacks
    def test_small_and_huge(self):
        small = {"x": 42}
        huge = {"y": "a" * (16 * 1024 * 1024 - 100)}

        # This call will trigger ismaster which we don't want to include in call count
        yield self.coll.count()

        with patch_send_msg() as mock:
            result = yield self.coll.insert_many([small, huge])
        mock.assert_called_once()
        self.assertEqual(len(result.inserted_ids), 2)


class TestUpdateOne(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many([{"x": 1}, {"x": 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.update_one(
            {"x": {"$exists": True}}, {"$set": {"y": 123}}
        )
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.upserted_id, None)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_one({"x": {"$exists": True}}, {"$set": {"y": 123}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, False)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.update_one(
            {"y": 123}, {"$set": {"z": 456}}, upsert=True
        )
        self.assertIsInstance(result.upserted_id, ObjectId)

        doc = yield self.coll.find_one(
            {"_id": result.upserted_id}, projection={"_id": 0}
        )
        self.assertEqual(doc, {"y": 123, "z": 456})

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_one({"y": 123}, {"$set": {"z": 456}}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    def test_InvalidUpdate(self):
        # update_one/update_many only allow $-operators, not whole document replace)
        self.assertRaises(ValueError, self.coll.update_one, {"x": 1}, {"y": 123})

    @defer.inlineCallbacks
    def test_Failures(self):
        # can't alter _id
        yield self.assertFailure(
            self.coll.update_one({"x": 1}, {"$set": {"_id": 1}}), WriteError
        )
        # invalid field name
        yield self.assertFailure(
            self.coll.update_one({"x": 1}, {"$set": {"$": 1}}), WriteError
        )

        yield self.coll.create_index(qf.sort(qf.ASCENDING("x")), unique=True)
        yield self.assertFailure(
            self.coll.update_one({"x": 2}, {"$set": {"x": 1}}), DuplicateKeyError
        )


class TestReplaceOne(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many([{"x": 1}, {"x": 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.replace_one({"x": {"$exists": True}}, {"y": 123})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(result.upserted_id, None)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.replace_one({"x": {"$exists": True}}, {"y": 123})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, False)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.replace_one({"x": 5}, {"y": 123}, upsert=True)
        self.assertIsInstance(result.upserted_id, ObjectId)

        doc = yield self.coll.find_one(
            {"_id": result.upserted_id}, projection={"_id": 0}
        )
        self.assertEqual(doc, {"y": 123})

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.replace_one({"x": 5}, {"y": 123}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        doc = yield self.coll.find_one({"y": {"$exists": True}}, projection={"_id": 0})
        self.assertEqual(doc, {"y": 123})

    def test_InvalidReplace(self):
        # replace_one does not allow $-operators, only whole document replace
        self.assertRaises(
            ValueError, self.coll.replace_one, {"x": 1}, {"$set": {"y": 123}}
        )

    @defer.inlineCallbacks
    def test_Failures(self):
        yield self.coll.create_index(qf.sort(qf.ASCENDING("x")), unique=True)
        yield self.assertFailure(
            self.coll.replace_one({"x": 1}, {"x": 2}), DuplicateKeyError
        )

    @only_for_mongodb_older_than(
        [5, 0], "$ in field name is only forbidden in MongoDB<5.0"
    )
    def test_ForbiddenFieldNames(self):
        yield self.assertFailure(
            self.coll.replace_one({"x": 1}, {"x": {"$": 5}}), WriteError
        )


class TestUpdateMany(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many([{"x": 1}, {"x": 2}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        result = yield self.coll.update_many(
            {"x": {"$exists": True}}, {"$set": {"y": 5}}
        )
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 2)
        self.assertEqual(result.modified_count, 2)
        self.assertEqual(result.upserted_id, None)

        cnt = yield self.coll.count({"y": 5})
        self.assertEqual(cnt, 2)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_many({"x": {"$exists": True}}, {"$set": {"y": 5}})
        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count({"y": 5})
        self.assertEqual(cnt, 2)

    @defer.inlineCallbacks
    def test_UpsertAcknowledged(self):
        result = yield self.coll.update_many({"x": 5}, {"$set": {"y": 5}}, upsert=True)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsInstance(result.upserted_id, ObjectId)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    @defer.inlineCallbacks
    def test_UpsertUnacknowledged(self):
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.update_many({"x": 5}, {"$set": {"y": 5}}, upsert=True)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)

    def test_InvalidUpdate(self):
        # update_one/update_many only allow $-operators, not whole document replace)
        self.assertRaises(ValueError, self.coll.update_many, {"x": 1}, {"y": 123})

    @defer.inlineCallbacks
    def test_Failures(self):
        # can't alter _id
        yield self.assertFailure(
            self.coll.update_many({}, {"$set": {"_id": 1}}), WriteError
        )
        # invalid field name
        yield self.assertFailure(
            self.coll.update_many({}, {"$set": {"$": 1}}), WriteError
        )

        yield self.coll.create_index(qf.sort(qf.ASCENDING("x")), unique=True)
        yield self.assertFailure(
            self.coll.update_many({"x": 2}, {"$set": {"x": 1}}), DuplicateKeyError
        )


class TestDeleteOne(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        result = yield self.coll.delete_one({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.deleted_count, 1)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.delete_one({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Failures(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        yield self.assertFailure(self.coll.delete_one({"x": {"$": 1}}), WriteError)

    def test_InvalidArguments(self):
        self.assertRaises(TypeError, self.coll.delete_one, 123)
        self.assertRaises(TypeError, self.coll.delete_one, ObjectId())

    @only_for_mongodb_starting_from([5, 0], "`let` is only supported by MongoDB >= 5.0")
    @defer.inlineCallbacks
    def test_Let(self):
        yield self.coll.insert_many(
            [
                {"_id": 1, "flavor": "chocolate"},
                {"_id": 2, "flavor": "strawberry"},
                {"_id": 3, "flavor": "cherry"},
                {"_id": 4, "flavor": "strawberry"},
            ]
        )
        result = yield self.coll.delete_one(
            filter={"$expr": {"$eq": ["$flavor", "$$targetFlavor"]}},
            let={"targetFlavor": "strawberry"},
        )
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.deleted_count, 1)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 3)


class TestDeleteMany(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_DeleteMultiple(self):
        yield self.coll.insert_many([{"x": 42}, {"x": 123}, {"y": 456}])
        yield self.coll.delete_many({"x": {"$exists": True}})

        remaining = yield self.coll.find(projection={"_id": 0})
        self.assertEqual(remaining, [{"y": 456}])

    @defer.inlineCallbacks
    def test_Acknowledged(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        result = yield self.coll.delete_many({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.acknowledged, True)
        self.assertEqual(result.deleted_count, 2)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 0)

    @defer.inlineCallbacks
    def test_Unacknowledged(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        coll = self.coll.with_options(write_concern=WriteConcern(w=0))
        result = yield coll.delete_many({"x": 1})
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.acknowledged, False)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 0)

    @defer.inlineCallbacks
    def test_Failures(self):
        yield self.coll.insert_many([{"x": 1}, {"x": 1}])
        yield self.assertFailure(self.coll.delete_many({"x": {"$": 1}}), WriteError)

    @only_for_mongodb_starting_from([5, 0], "`let` is only supported by MongoDB >= 5.0")
    @defer.inlineCallbacks
    def test_Let(self):
        yield self.coll.insert_many(
            [
                {"_id": 1, "flavor": "chocolate"},
                {"_id": 2, "flavor": "strawberry"},
                {"_id": 3, "flavor": "cherry"},
                {"_id": 4, "flavor": "strawberry"},
            ]
        )
        result = yield self.coll.delete_many(
            filter={"$expr": {"$eq": ["$flavor", "$$targetFlavor"]}},
            let={"targetFlavor": "strawberry"},
        )
        self.assertIsInstance(result, DeleteResult)
        self.assertEqual(result.deleted_count, 2)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 2)

    def test_InvalidArguments(self):
        self.assertRaises(TypeError, self.coll.delete_many, 123)
        self.assertRaises(TypeError, self.coll.delete_many, ObjectId())


class TestFindOneAndDelete(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many(
            [{"x": 1, "y": 1}, {"x": 2, "y": 2}, {"x": 3, "y": 3}]
        )

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_delete(
            {"x": {"$exists": True}}, sort=qf.sort(qf.ASCENDING("y"))
        )
        self.assertEqual(doc["x"], 1)

        doc = yield self.coll.find_one_and_delete(
            {"x": {"$exists": True}}, sort=qf.sort(qf.DESCENDING("y"))
        )
        self.assertEqual(doc["x"], 3)

        cnt = yield self.coll.count()
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Projection(self):
        doc = yield self.coll.find_one_and_delete({"x": 2}, {"y": 1, "_id": 0})
        self.assertEqual(doc, {"y": 2})

    async def test_Unack(self):
        coll_unack = self.coll.with_options(write_concern=WriteConcern(w=0))
        self.assertIsNone(await coll_unack.find_one_and_delete({"x": 2}))


class TestFindOneAndReplace(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many(
            [{"x": 10, "y": 10}, {"x": 20, "y": 20}, {"x": 30, "y": 30}]
        )

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_replace(
            {}, {"x": 5, "y": 5}, projection={"_id": 0}, sort=qf.sort(qf.ASCENDING("y"))
        )
        self.assertEqual(doc, {"x": 10, "y": 10})

        doc = yield self.coll.find_one_and_replace(
            {},
            {"x": 40, "y": 40},
            projection={"_id": 0},
            sort=qf.sort(qf.DESCENDING("y")),
        )
        self.assertEqual(doc, {"x": 30, "y": 30})

        ys = yield self.coll.distinct("y")
        self.assertEqual(set(ys), {5, 20, 40})

    def test_InvalidReplace(self):
        self.assertRaises(
            ValueError, self.coll.find_one_and_replace, {}, {"$set": {"z": 1}}
        )

    @defer.inlineCallbacks
    def test_Upsert(self):
        doc = yield self.coll.find_one_and_replace({"x": 40}, {"x": 50}, upsert=True)
        self.assertEqual(doc, None)

        xs = yield self.coll.distinct("x")
        self.assertEqual(set(xs), {10, 20, 30, 50})

    @defer.inlineCallbacks
    def test_ReturnDocument(self):
        doc = yield self.coll.find_one_and_replace(
            {"x": 10}, {"x": 15}, return_document=ReturnDocument.BEFORE
        )
        self.assertEqual(doc["x"], 10)

        doc = yield self.coll.find_one_and_replace(
            {"x": 20}, {"x": 25}, return_document=ReturnDocument.AFTER
        )
        self.assertEqual(doc["x"], 25)

    def test_InvalidReturnDocument(self):
        self.assertRaises(
            ValueError, self.coll.find_one_and_replace, {}, {}, return_document=1
        )

    async def test_Unack(self):
        coll_unack = self.coll.with_options(write_concern=WriteConcern(w=0))
        self.assertIsNone(
            await coll_unack.find_one_and_replace(
                {"x": 10}, {"x": 15}, return_document=ReturnDocument.BEFORE
            )
        )


class TestFindOneAndUpdate(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many(
            [{"x": 10, "y": 10}, {"x": 20, "y": 20}, {"x": 30, "y": 30}]
        )

    @defer.inlineCallbacks
    def test_Sort(self):
        doc = yield self.coll.find_one_and_update(
            {},
            {"$set": {"y": 5}},
            projection={"_id": 0},
            sort=qf.sort(qf.ASCENDING("y")),
        )
        self.assertEqual(doc, {"x": 10, "y": 10})

        doc = yield self.coll.find_one_and_update(
            {},
            {"$set": {"y": 35}},
            projection={"_id": 0},
            sort=qf.sort(qf.DESCENDING("y")),
        )
        self.assertEqual(doc, {"x": 30, "y": 30})

        ys = yield self.coll.distinct("y")
        self.assertEqual(set(ys), {5, 20, 35})

    def test_InvalidUpdate(self):
        self.assertRaises(ValueError, self.coll.find_one_and_update, {}, {"x": 123})

    @defer.inlineCallbacks
    def test_Upsert(self):
        doc = yield self.coll.find_one_and_update(
            {"x": 40}, {"$set": {"y": 40}}, upsert=True
        )
        self.assertEqual(doc, None)

        inserted = yield self.coll.find_one({"x": 40})
        self.assertEqual(inserted["y"], 40)

    @defer.inlineCallbacks
    def test_ReturnDocument(self):
        doc = yield self.coll.find_one_and_update(
            {"x": 10}, {"$set": {"y": 5}}, return_document=ReturnDocument.BEFORE
        )
        self.assertEqual(doc["y"], 10)

        doc = yield self.coll.find_one_and_update(
            {"x": 10}, {"$set": {"y": 15}}, return_document=ReturnDocument.AFTER
        )
        self.assertEqual(doc["y"], 15)

    async def test_Unack(self):
        coll_unack = self.coll.with_options(write_concern=WriteConcern(w=0))
        self.assertIsNone(
            await coll_unack.find_one_and_update(
                {"x": 10}, {"$set": {"y": 5}}, return_document=ReturnDocument.BEFORE
            )
        )


class TestCount(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super().setUp()
        yield self.coll.insert_many([{"x": 10}, {"x": 20}, {"x": 30}])

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.system.profile.drop()
        yield super().tearDown()

    @defer.inlineCallbacks
    def test_count(self):
        self.assertEqual((yield self.coll.count()), 3)
        self.assertEqual((yield self.coll.count({"x": 20})), 1)
        self.assertEqual((yield self.coll.count({"x": {"$gt": 15}})), 2)

        self.assertEqual((yield self.db.non_existing.count()), 0)

    @defer.inlineCallbacks
    def test_hint(self):
        yield self.coll.create_index(qf.sort(qf.ASCENDING("x")))

        yield self.db.command("profile", 2)
        cnt = yield self.coll.count(hint=qf.hint(qf.ASCENDING("x")))
        self.assertEqual(cnt, 3)
        yield self.db.command("profile", 0)

        cmds = yield self.db.system.profile.count({"command.hint": {"x": 1}})
        self.assertEqual(cmds, 1)

        self.assertRaises(TypeError, self.coll.count, hint={"x": 1})
        self.assertRaises(TypeError, self.coll.count, hint=[("x", 1)])

    @defer.inlineCallbacks
    def test_skip_limit(self):
        cnt = yield self.coll.count(limit=2)
        self.assertEqual(cnt, 2)

        cnt = yield self.coll.count(skip=1)
        self.assertEqual(cnt, 2)
