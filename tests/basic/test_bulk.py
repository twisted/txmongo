from bson import BSON
from pymongo import InsertOne
from pymongo.errors import BulkWriteError, OperationFailure, NotMasterError
from pymongo.operations import UpdateOne, DeleteOne, UpdateMany, ReplaceOne
from pymongo.results import BulkWriteResult
from pymongo.write_concern import WriteConcern
from twisted.internet import defer
from unittest.mock import patch

from tests.utils import SingleCollectionTest
from txmongo.protocol import Reply


class TestArgsValidation(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_Generator(self):
        # bulk_write should accept comprehensions because they're cool
        yield self.coll.bulk_write(InsertOne({'x': i}) for i in range(10))
        self.assertEqual((yield self.coll.count()), 10)

    def test_Types(self):
        # requests argument must be iterable of _WriteOps
        self.assertRaises(TypeError, self.coll.bulk_write, InsertOne({'x': 42}))
        self.assertRaises(TypeError, self.coll.bulk_write, [{'x': 42}])


class TestErrorHandling(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestErrorHandling, self).setUp()
        yield self.coll.insert_one({'_id': 1})

    @defer.inlineCallbacks
    def test_Insert_Ordered_Ack(self):
        result = self.coll.bulk_write([InsertOne({'_id': 2}), InsertOne({'_id': 1}),
                                       InsertOne({'_id': 3})])
        yield self.assertFailure(result, BulkWriteError)
        self.assertEqual((yield self.coll.count()), 2)

    @defer.inlineCallbacks
    def test_Insert_Unordered_Ack(self):
        result = self.coll.bulk_write([InsertOne({'_id': 2}), InsertOne({'_id': 1}),
                                       InsertOne({'_id': 3})], ordered=False)

        yield self.assertFailure(result, BulkWriteError)
        self.assertEqual((yield self.coll.count()), 3)

    @defer.inlineCallbacks
    def test_Insert_Ordered_Unack(self):
        w0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield w0.bulk_write([InsertOne({'_id': 2}), InsertOne({'_id': 1}),
                             InsertOne({'_id': 3})])
        self.assertEqual((yield self.coll.count()), 2)

    @defer.inlineCallbacks
    def test_Insert_Unordered_Unack(self):
        w0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield w0.bulk_write([InsertOne({'_id': 2}), InsertOne({'_id': 1}),
                             InsertOne({'_id': 3})], ordered=False)
        self.assertEqual((yield self.coll.count()), 3)

    @defer.inlineCallbacks
    def test_Mixed_Ordered_Ack(self):
        result = self.coll.bulk_write([InsertOne({"_id": 2}),
                                       UpdateOne({"_id": 2}, {"$set": {'x': 1}}),
                                       InsertOne({"_id": 1}),
                                       UpdateOne({"_id": 1}, {"$set": {'x': 2}})])
        yield self.assertFailure(result, BulkWriteError)

        docs = yield self.coll.find()
        self.assertEqual(len(docs), 2)
        self.assertIn({"_id": 1}, docs)
        self.assertIn({"_id": 2, 'x': 1}, docs)

    @defer.inlineCallbacks
    def test_Mixed_Unordered_Ack(self):
        result = self.coll.bulk_write([InsertOne({"_id": 2}),
                                       UpdateOne({"_id": 2}, {"$set": {'x': 1}}),
                                       InsertOne({"_id": 1}),
                                       UpdateOne({"_id": 1}, {"$set": {'x': 2}})],
                                      ordered=False)

        yield self.assertFailure(result, BulkWriteError)

        docs = yield self.coll.find()
        self.assertEqual(len(docs), 2)
        self.assertIn({"_id": 1, 'x': 2}, docs)
        self.assertIn({"_id": 2, 'x': 1}, docs)

    @defer.inlineCallbacks
    def test_Mixed_Ordered_Unack(self):
        w0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield w0.bulk_write([InsertOne({"_id": 2}),
                             UpdateOne({"_id": 2}, {"$set": {'x': 1}}),
                             InsertOne({"_id": 1}),
                             UpdateOne({"_id": 1}, {"$set": {'x': 2}})])

        docs = yield self.coll.find()
        self.assertEqual(len(docs), 2)
        self.assertIn({"_id": 1}, docs)
        self.assertIn({"_id": 2, 'x': 1}, docs)

    @defer.inlineCallbacks
    def test_Mixed_Unordered_Unack(self):
        w0 = self.coll.with_options(write_concern=WriteConcern(w=0))
        yield w0.bulk_write([InsertOne({"_id": 2}),
                             UpdateOne({"_id": 2}, {"$set": {'x': 1}}),
                             InsertOne({"_id": 1}),
                             UpdateOne({"_id": 1}, {"$set": {'x': 2}})],
                            ordered=False)

        docs = yield self.coll.find()
        self.assertEqual(len(docs), 2)
        self.assertIn({"_id": 1, 'x': 2}, docs)
        self.assertIn({"_id": 2, 'x': 1}, docs)


class TestBulkInsert(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_InsertOne(self):
        result = yield self.coll.bulk_write([InsertOne({'x': 42}), InsertOne({'y': 123})])
        self.assertEqual((yield self.coll.count()), 2)
        self.assertIsInstance(result, BulkWriteResult)
        self.assertEqual(result.inserted_count, 2)

        result = yield self.coll.with_options(write_concern=WriteConcern(w=0))\
            .bulk_write([InsertOne({'x': 42}), InsertOne({'y': 123})])
        self.assertIsInstance(result, BulkWriteResult)
        self.assertFalse(result.acknowledged)


class TestBulkUpdate(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestBulkUpdate, self).setUp()
        yield self.coll.insert_many([
            {'x': 42},
            {'y': 123},
            {'z': 321},
        ])

    @defer.inlineCallbacks
    def test_UpdateOneAndMany(self):
        result = yield self.coll.bulk_write([
            UpdateOne({'x': {"$exists": True}}, {"$inc": {'x': 1}}),  # simple
            UpdateOne({'y': 123}, {"$inc": {'y': -1}, "$set": {'a': "hi"}}),  # set
            UpdateOne({'z': 322}, {"$inc": {'z': 1}}),  # missed
            UpdateOne({'w': 5}, {"$set": {"u": 7}}, upsert=True),  # upsert
            UpdateMany({}, {"$set": {"m": 1}}),  # many
        ])

        docs = yield self.coll.find(fields={"_id": 0})
        self.assertEqual(len(docs), 4)
        self.assertIn({'x': 43, 'm': 1}, docs)
        self.assertIn({'y': 122, 'a': "hi", 'm': 1}, docs)
        self.assertIn({'z': 321, 'm': 1}, docs)
        self.assertIn({'w': 5, 'u': 7, 'm': 1}, docs)

        self.assertIsInstance(result, BulkWriteResult)
        self.assertEqual(result.inserted_count, 0)
        self.assertEqual(result.matched_count, 6)
        self.assertEqual(result.modified_count, 6)
        self.assertEqual(result.upserted_count, 1)
        self.assertEqual(set(result.upserted_ids), {3})

    @defer.inlineCallbacks
    def test_ReplaceOne(self):
        result = yield self.coll.bulk_write([
            ReplaceOne({'x': 42}, {'j': 5}),
            ReplaceOne({'x': 555}, {'k': 5}, upsert=True),
        ])

        docs = yield self.coll.find(fields={"_id": 0})
        self.assertEqual(len(docs), 4)
        self.assertIn({'j': 5}, docs)
        self.assertIn({'y': 123}, docs)
        self.assertIn({'z': 321}, docs)
        self.assertIn({'k': 5}, docs)

        self.assertIsInstance(result, BulkWriteResult)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(set(result.upserted_ids), {1})


class TestBulkDelete(SingleCollectionTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield super(TestBulkDelete, self).setUp()
        yield self.coll.insert_many([
            {'x': 42},
            {'x': 123},
            {'x': 321},
        ])

    @defer.inlineCallbacks
    def test_DeleteOne(self):
        result = yield self.coll.bulk_write([
            DeleteOne({'x': 42}),
            DeleteOne({'x': {'$gt': 100}})
        ])
        self.assertEqual((yield self.coll.count()), 1)

        self.assertIsInstance(result, BulkWriteResult)
        self.assertEqual(result.deleted_count, 2)


class TestHuge(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_MoreThan1k(self):
        yield self.coll.bulk_write(InsertOne({"_id": i}) for i in range(2016))
        self.assertEqual((yield self.coll.count()), 2016)

    @defer.inlineCallbacks
    def test_MoreThan16Mb(self):
        str_8mb = 'y' * 8388608
        yield self.coll.bulk_write(InsertOne({'x': str_8mb}) for _ in range(5))
        self.assertEqual((yield self.coll.count()), 5)

        docs = yield self.coll.find()
        total_size = sum(len(BSON.encode(doc)) for doc in docs)
        self.assertGreater(total_size, 40 * 1024**2)


class TestOperationFailure(SingleCollectionTest):

    @defer.inlineCallbacks
    def test_OperationFailure(self):
        yield self.coll.bulk_write([UpdateOne({}, {'$set': {'x': 42}}, upsert=True)])

        def fake_send_query(*args):
            return defer.succeed(Reply(documents=[
                {'ok': 0.0, 'errmsg': 'operation was interrupted', 'code': 11602,
                 'codeName': 'InterruptedDueToReplStateChange'}]))

        with patch('txmongo.protocol.MongoProtocol.send_QUERY', side_effect=fake_send_query):
            yield self.assertFailure(
                    self.coll.bulk_write([UpdateOne({}, {'$set': {'x': 42}}, upsert=True)], ordered=True),
                    OperationFailure, NotMasterError)
