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
from unittest.mock import patch
from time import time
from twisted.trial import unittest
from twisted.internet import defer
from txmongo import connection
from txmongo.utils import check_deadline
from txmongo.errors import TimeExceeded

mongo_host = "127.0.0.1"
mongo_port = 27017


class TestMongoConnection(unittest.TestCase):

    def setUp(self):
        self.named_conn = connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        self.unnamed_conn = connection.ConnectionPool("127.0.0.1")

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.named_conn.drop_database("db")
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
        test = connection.ConnectionPool()
        yield test.disconnect()
        test = connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        yield test.disconnect()
        test = connection.ConnectionPool(u"mongodb://127.0.0.1/dbname")
        yield test.disconnect()
        self.assertRaises(AssertionError, connection.ConnectionPool, object)
        self.assertRaises(AssertionError, connection.ConnectionPool, 1)

    @defer.inlineCallbacks
    def test_Timeout_and_Deadline(self):
        yield self.named_conn.db.coll.insert({'x': 42}, safe=True, timeout=10)
        yield self.named_conn.db.coll.insert({'x': 42}, safe=True, deadline=time()+10)

        self.assertRaises(TimeExceeded, self.named_conn.db.coll.insert, {'x': 42}, safe=True, deadline=time()-10)

        self.assertRaises(TimeExceeded, self.named_conn.db.coll.insert, {'x': 42}, safe=True, timeout=-10)

        def patch_deadline(_):
            check_deadline(time()-2)

        with patch('txmongo.collection.check_deadline', side_effect=patch_deadline):
            d_insert = self.named_conn.db.coll.find_one(
                {'x': 42}, deadline=time()+2)
            yield self.assertFailure(d_insert, TimeExceeded)


class TestDropDatabase(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = connection.ConnectionPool()
        self.db = self.conn.db
        self.coll = self.db.coll
        yield self.coll.insert({'x': 42})
        yield self.assert_coll_count(1)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.conn.drop_database(self.db)
        yield self.conn.disconnect()

    @defer.inlineCallbacks
    def assert_coll_count(self, n):
        self.assertEqual((yield self.coll.count()), n)

    @defer.inlineCallbacks
    def test_by_name(self):
        yield self.conn.drop_database("db")
        yield self.assert_coll_count(0)

    def test_invalid_args(self):
        self.assertRaises(TypeError, self.conn.drop_database, 42)
        self.assertRaises(TypeError, self.conn.drop_database, self.conn)
        self.assertRaises(TypeError, self.conn.drop_database, self.coll)

    @defer.inlineCallbacks
    def test_by_object(self):
        yield self.conn.drop_database(self.db)
        yield self.assert_coll_count(0)
