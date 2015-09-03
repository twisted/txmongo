# coding: utf-8
# Copyright 2015 Ilya Skriblovsky <ilyaskriblovsky@gmail.com>
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

from mock import patch
from pymongo.write_concern import WriteConcern
from twisted.internet import defer
from twisted.trial import unittest
from txmongo.connection import MongoConnection, ConnectionPool
from txmongo.database import Database
from txmongo.collection import Collection

mongo_host = "localhost"
mongo_port = 27017


class TestWriteConcern(unittest.TestCase):

    def mock_gle(self):
        return patch("txmongo.protocol.MongoProtocol.get_last_error")

    @defer.inlineCallbacks
    def test_Priority(self):
        """
            Check that connection-level, database-level, collection-level
            and query-level write concerns are respected with correct priority
        """
        conn = MongoConnection(mongo_host, mongo_port, w=1, wtimeout=500)

        try:
            with self.mock_gle() as mock:
                yield conn.mydb.mycol.insert({'x': 42})
                mock.assert_called_once_with("mydb", w=1, wtimeout=500)

            db_w0 = Database(conn, "mydb", write_concern=WriteConcern(w=0))
            with self.mock_gle() as mock:
                yield db_w0.mycol.insert({'x': 42})
                self.assertFalse(mock.called)

            coll = Collection(db_w0, "mycol", write_concern=WriteConcern(w=2))
            with self.mock_gle() as mock:
                yield coll.insert({'x': 42})
                mock.assert_called_once_with("mydb", w=2)

            with self.mock_gle() as mock:
                yield coll.insert({'x': 42}, j=True)
                mock.assert_called_once_with("mydb", j=True)

        finally:
            yield conn.mydb.mycol.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Safe(self):
        conn = MongoConnection(mongo_host, mongo_port, w=1, wtimeout=500)
        coll = conn.mydb.mycol

        try:
            with self.mock_gle() as mock:
                yield coll.insert({'x': 42})
                mock.assert_called_once_with("mydb", w=1, wtimeout=500)

            with self.mock_gle() as mock:
                yield coll.insert({'x': 42}, safe=False)
                self.assertFalse(mock.called)

            with self.mock_gle() as mock:
                yield coll.insert({'x': 42}, safe=True)
                mock.assert_called_once_with("mydb", w=1, wtimeout=500)
        finally:
            yield coll.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_SafeWithDefaultW0(self):
        conn = MongoConnection(mongo_host, mongo_port, w=0)
        coll = conn.mydb.mycol

        try:
            with self.mock_gle() as mock:
                yield coll.insert({'x': 42})
                self.assertFalse(mock.called)

            with self.mock_gle() as mock:
                yield coll.insert({'x': 42}, safe=False)
                self.assertFalse(mock.called)

            with self.mock_gle() as mock:
                yield coll.insert({'x': 42}, safe=True)
                mock.assert_called_once_with("mydb")
        finally:
            yield coll.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def __test_operation(self, coll, method, *args):
        with self.mock_gle() as mock:
            yield getattr(coll, method)(*args)
            self.assertFalse(mock.called)

        with self.mock_gle() as mock:
            yield getattr(coll, method)(*args, safe=True)
            mock.assert_called_once_with("mydb")

        with self.mock_gle() as mock:
            yield getattr(coll, method)(*args, safe=False)
            self.assertFalse(mock.called)

        with self.mock_gle() as mock:
            yield getattr(coll, method)(*args, w=1)
            mock.assert_called_once_with("mydb", w=1)

    @defer.inlineCallbacks
    def test_AllOperations(self):
        conn = MongoConnection(mongo_host, mongo_port, w=0)
        coll = conn.mydb.mycol

        try:
            yield self.__test_operation(coll, "insert", {'x': 42})
            yield self.__test_operation(coll, "update", {}, {'x': 42})
            yield self.__test_operation(coll, "save", {'x': 42})
            yield self.__test_operation(coll, "remove", {})
        finally:
            yield coll.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionUrlParams(self):
        conn = ConnectionPool("mongodb://{0}:{1}/?w=2&j=true".format(mongo_host, mongo_port))
        coll = conn.mydb.mycol

        try:
            with self.mock_gle() as mock:
                yield coll.insert({'x': 42})
                mock.assert_called_once_with('mydb', w=2, j=True)
        finally:
            yield coll.drop()
            yield conn.disconnect()
