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

from mock import Mock, patch
from pymongo.errors import ConfigurationError
from twisted.internet import defer
from twisted.trial import unittest
from txmongo.connection import MongoConnection, ConnectionPool
from txmongo.protocol import MongoProtocol
from txmongo.database import Database
from txmongo.collection import Collection
from txmongo.write_concern import WriteConcern

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


class TestWriteConcernClass(unittest.TestCase):

    def test_wtimeout(self):
        wc = WriteConcern(wtimeout=123)
        self.assertEqual(wc.document['wtimeout'], 123)

        self.assertRaises(TypeError, WriteConcern, wtimeout=123.456)

    def test_j(self):
        wc = WriteConcern(j=True)
        self.assertEqual(wc.document['j'], True)

        self.assertRaises(TypeError, WriteConcern, j=1)

    def test_fsync(self):
        wc = WriteConcern(fsync=True)
        self.assertEqual(wc.document['fsync'], True)

        self.assertRaises(TypeError, WriteConcern, fsync=1)
        # Can't set both j and fsync
        self.assertRaises(ConfigurationError, WriteConcern, j=True, fsync=True)

    def test_w(self):
        WriteConcern(w=0)

        # Can't set w=0 with any other options
        self.assertRaises(ConfigurationError, WriteConcern, w=0, j=True)
        self.assertRaises(ConfigurationError, WriteConcern, w=0, wtimeout=100)
        self.assertRaises(ConfigurationError, WriteConcern, w=0, fsync=True)

        self.assertRaises(TypeError, WriteConcern, w=1.5)

    def test_repr(self):
        self.assertEqual(repr(WriteConcern()), "WriteConcern()")
        self.assertEqual(repr(WriteConcern(w=2)), "WriteConcern(w=2)")
        self.assertEqual(repr(WriteConcern(fsync=True)), "WriteConcern(fsync=True)")

        multiopt = repr(WriteConcern(w=2, wtimeout=500, fsync=True))
        self.assertTrue(multiopt.startswith("WriteConcern("))
        self.assertTrue(multiopt.endswith(')'))
        inner = multiopt[13:-1]
        self.assertEqual(set(inner.split(", ")), set(["w=2", "wtimeout=500", "fsync=True"]))

    def test_cmp(self):
        self.assertEqual(WriteConcern(w=2, wtimeout=500), WriteConcern(wtimeout=500, w=2))
        self.assertNotEqual(WriteConcern(w=2, wtimeout=500),
                            WriteConcern(wtimeout=500, w=2, j=True))
