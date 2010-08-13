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

from twisted.internet import defer
from twisted.trial import unittest
import txmongo

mongo_host = "localhost"
mongo_port = 27017


class TestMongoQueries(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_SingleCursorIteration(self):
        yield self.coll.insert([{'v':i} for i in xrange(10)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 10)

    @defer.inlineCallbacks
    def test_MultipleCursorIterations(self):
        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 200)

    @defer.inlineCallbacks
    def test_LargeData(self):
        yield self.coll.insert([{'v':' '*(2**19)} for i in xrange(4)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 4)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


class TestMongoQueriesEdgeCases(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_BelowBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(100)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 100)

    @defer.inlineCallbacks
    def test_EqualToBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(101)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 101)

    @defer.inlineCallbacks
    def test_AboveBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(102)], safe=True)
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
        yield self.coll.insert([{'v':i} for i in xrange(50)], safe=True)
        res = yield self.coll.find(limit=20)
        self.assertEqual(len(res), 20)

    @defer.inlineCallbacks
    def test_LimitAboveBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find(limit=150)
        self.assertEqual(len(res), 150)

    @defer.inlineCallbacks
    def test_LimitAtBatchThresholdEdge(self):
        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find(limit=100)
        self.assertEqual(len(res), 100)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find(limit=101)
        self.assertEqual(len(res), 101)

        yield self.coll.drop(safe=True)

        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find(limit=102)
        self.assertEqual(len(res), 102)

    @defer.inlineCallbacks
    def test_LimitAboveMessageSizeThreshold(self):
        yield self.coll.insert([{'v':' '*(2**20)} for i in xrange(8)], safe=True)
        res = yield self.coll.find(limit=5)
        self.assertEqual(len(res), 5)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop(safe=True)
        yield self.conn.disconnect()
