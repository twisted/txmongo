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
from twisted.trial import unittest
from twisted.internet import base, defer, reactor

mongo_host="localhost"
mongo_port=27017
base.DelayedCall.debug = False

class TestMongoConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_MongoConnection(self):
        # MongoConnection returns deferred, which gets MongoAPI
        conn = txmongo.MongoConnection(mongo_host, mongo_port)
        self.assertEqual(isinstance(conn, defer.Deferred), True)
        rapi = yield conn
        self.assertEqual(isinstance(rapi, txmongo.MongoAPI), True)
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)
        
    @defer.inlineCallbacks
    def test_MongoConnectionPool(self):
        # MongoConnectionPool returns deferred, which gets MongoAPI
        conn = txmongo.MongoConnectionPool(mongo_host, mongo_port, pool_size=2)
        self.assertEqual(isinstance(conn, defer.Deferred), True)
        rapi = yield conn
        self.assertEqual(isinstance(rapi, txmongo.MongoAPI), True)
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)

    @defer.inlineCallbacks
    def test_lazyMongoConnection(self):
        # lazyMongoConnection returns MongoAPI
        rapi = txmongo.lazyMongoConnection(mongo_host, mongo_port)
        self.assertEqual(isinstance(rapi, txmongo.MongoAPI), True)
        yield rapi._connected
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)

    @defer.inlineCallbacks
    def test_lazyMongoConnectionPool(self):
        # lazyMongoConnection returns MongoAPI
        rapi = txmongo.lazyMongoConnectionPool(mongo_host, mongo_port, pool_size=2)
        self.assertEqual(isinstance(rapi, txmongo.MongoAPI), True)
        yield rapi._connected
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)
