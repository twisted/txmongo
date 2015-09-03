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

from twisted.internet import defer
from twisted.trial import unittest
import txmongo

mongo_host = "localhost"
mongo_port = 27017


class TestFindAndModify(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_Update(self):
        yield self.coll.insert([{"oh": "hai", "lulz": 123},
                                {"oh": "kthxbye", "lulz": 456}], safe=True)

        res = yield self.coll.find_one({"oh": "hai"})
        self.assertEqual(res["lulz"], 123)

        res = yield self.coll.find_and_modify({"o2h": "hai"}, {"$inc": {"lulz": 1}})
        self.assertEqual(res, None)

        res = yield self.coll.find_and_modify({"oh": "hai"}, {"$inc": {"lulz": 1}})
        self.assertEqual(res["lulz"], 123)
        res = yield self.coll.find_and_modify({"oh": "hai"}, {"$inc": {"lulz": 1}}, new=True)
        self.assertEqual(res["lulz"], 125)

        res = yield self.coll.find_one({"oh": "kthxbye"})
        self.assertEqual(res["lulz"], 456)

    def test_InvalidOptions(self):
        self.assertFailure(self.coll.find_and_modify(), ValueError)
        self.assertFailure(self.coll.find_and_modify(update={"$set": {'x': 42}}, remove=True),
                           ValueError)

    @defer.inlineCallbacks
    def test_Remove(self):
        yield self.coll.insert({'x': 42})
        doc = yield self.coll.find_and_modify(remove=True)
        self.assertEqual(doc['x'], 42)
        cnt = yield self.coll.count()
        self.assertEqual(cnt, 0)

    @defer.inlineCallbacks
    def test_Upsert(self):
        yield self.coll.find_and_modify({'x': 42}, update={"$set": {'y': 123}}, upsert=True)
        docs = yield self.coll.find(fields={"_id": 0})
        self.assertEqual(docs, [{'x': 42, 'y': 123}])

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()
