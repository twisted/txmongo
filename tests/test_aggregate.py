# coding: utf-8
# Copyright 2010 Tryggvi Bjorgvinsson
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

mongo_host = "127.0.0.1"
mongo_port = 27017


class TestAggregate(unittest.TestCase):

    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_Aggregate(self):
        yield self.coll.insert([{"oh": "hai", "lulz": 123},
                                {"oh": "kthxbye", "lulz": 456},
                                {"oh": "hai", "lulz": 789}, ], safe=True)

        res = yield self.coll.aggregate([
            {"$project": {"oh": 1, "lolz": "$lulz"}},
            {"$group": {"_id": "$oh", "many_lolz": {"$sum": "$lolz"}}},
            {"$sort": {"_id": 1}}
            ])

        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["_id"], "hai")
        self.assertEqual(res[0]["many_lolz"], 912)
        self.assertEqual(res[1]["_id"], "kthxbye")
        self.assertEqual(res[1]["many_lolz"], 456)

        res = yield self.coll.aggregate([{"$match": {"oh": "hai"}}], full_response=True)

        self.assertTrue("ok" in res)
        self.assertTrue("result" in res)
        self.assertEqual(len(res["result"]), 2)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()
