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

from twisted.trial import unittest
from twisted.internet import defer
import txmongo
import txmongo.filter as qf
from pymongo.errors import OperationFailure

mongo_host = "localhost"
mongo_port = 27017


class TestMongoFilters(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol
        yield self.coll.insert({'x': 42})

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


    @defer.inlineCallbacks
    def test_Hint(self):
        # Ensure there is no {x:1} index
        yield self.coll.dropIndex([('x', 1)])

        # find() should fail with 'bad hint' if hint specifier works correctly
        self.assertFailure(self.coll.find({}, filter=qf.hint([('x', 1)])), OperationFailure)

    @defer.inlineCallbacks
    def test_Comment(self):
        comment = "hello world"

        # Checking that $comment appears in profiler log
        yield self.db.system.profile.drop()

        yield self.db["$cmd"].find_one({"profile": 2})
        yield self.coll.find({}, filter=qf.comment(comment))
        yield self.db["$cmd"].find_one({"profile": 0})

        cnt = yield self.db.system.profile.count({"query.$comment": comment})

        try:
            self.assertEqual(cnt, 1)

        finally:
            yield self.db.system.profile.drop()
