# coding: utf-8
# Copyright 2010-2015 TxMongo Developers
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
from twisted.trial import unittest
from twisted.internet import defer
import txmongo

mongo_host = "127.0.0.1"
mongo_port = 27017


class TestMongoConnection(unittest.TestCase):

    def setUp(self):
        self.named_conn = txmongo.connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        self.unnamed_conn = txmongo.connection.ConnectionPool("127.0.0.1")

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.named_conn.disconnect()
        yield self.unnamed_conn.disconnect()

    def test_GetDefaultDatabase(self):
        self.assertEqual(self.named_conn.get_default_database().name,
                         self.named_conn["dbname"].name)
        self.assertEqual(self.unnamed_conn.get_default_database(), None)

    def test_Misc(self):
        result = self.named_conn.getprotocols()
        result[0].uri['nodelist'].pop()
        self.assertTrue(len(result[0].uri['nodelist']) == 0)
        self.assertEqual("Connection()", repr(self.named_conn))

    @defer.inlineCallbacks
    def test_uri_input(self):
        test = txmongo.connection.ConnectionPool()
        yield test.disconnect()
        test = txmongo.connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        yield test.disconnect()
        test = txmongo.connection.ConnectionPool(u"mongodb://127.0.0.1/dbname")
        yield test.disconnect()
        self.assertRaises(AssertionError, txmongo.connection.ConnectionPool, object)
        self.assertRaises(AssertionError, txmongo.connection.ConnectionPool, 1)
