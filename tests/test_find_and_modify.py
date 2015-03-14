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


class TestFindAndModify(unittest.TestCase):

    timeout = 5

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


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()

