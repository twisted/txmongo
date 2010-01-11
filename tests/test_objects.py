# coding: utf-8
# Copyright 2009 Alexandre Fiori
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

import txmongo
from txmongo import database
from txmongo import collection
from twisted.trial import unittest
from twisted.internet import base, defer, reactor

mongo_host="localhost"
mongo_port=27017
base.DelayedCall.debug = False

class TestMongoObjects(unittest.TestCase):
    @defer.inlineCallbacks
    def test_MongoObjects(self):
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        mydb = conn.mydb
        self.assertEqual(isinstance(mydb, database.Database), True)
        mycol = mydb.mycol
        self.assertEqual(isinstance(mycol, collection.Collection), True)
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_MongoOperations(self):
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        test = conn.foo.test
        
        # insert
        doc = {"foo":"bar", "items":[1, 2, 3]}
        yield test.safe_insert(doc)
        result = yield test.find_one(doc)
        self.assertEqual(result.has_key("_id"), True)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["items"], [1, 2, 3])

        # update
        yield test.safe_update({"_id":result["_id"]}, {"$set":{"one":"two"}})
        result = yield test.find_one({"_id":result["_id"]})
        self.assertEqual(result["one"], "two")

        # delete
        yield test.safe_remove(result["_id"])

        # disconnect
        yield conn.disconnect()
