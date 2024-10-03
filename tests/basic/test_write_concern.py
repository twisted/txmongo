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
import copy
from contextlib import contextmanager
from unittest import SkipTest
from unittest.mock import patch

import bson
from pymongo.write_concern import WriteConcern
from twisted.internet import defer
from twisted.trial import unittest

from txmongo import MongoProtocol
from txmongo.collection import Collection
from txmongo.connection import ConnectionPool, MongoConnection
from txmongo.database import Database

mongo_host = "localhost"
mongo_port = 27017


class TestWriteConcern(unittest.TestCase):

    @contextmanager
    def assert_called_with_write_concern(self, write_concern: WriteConcern):
        with patch.object(
            MongoProtocol, "send_msg", side_effect=MongoProtocol.send_msg, autospec=True
        ) as mock:
            yield

            mock.assert_called_once()
            msg = mock.call_args[0][1]
            cmd = bson.decode(msg.body)
            self.assertEqual(cmd["writeConcern"], write_concern.document)

    @defer.inlineCallbacks
    def test_Priority(self):
        """
        Check that connection-level, database-level, collection-level
        and query-level write concerns are respected with correct priority
        """
        conn = MongoConnection(mongo_host, mongo_port, w=1, wtimeout=500)

        try:
            with self.assert_called_with_write_concern(WriteConcern(w=1, wtimeout=500)):
                yield conn.mydb.mycol.insert_one({"x": 42})

            db_w0 = Database(conn, "mydb", write_concern=WriteConcern(w=0))
            with self.assert_called_with_write_concern(WriteConcern(w=0)):
                yield db_w0.mycol.insert_one({"x": 42})

            coll = Collection(
                db_w0, "mycol", write_concern=WriteConcern(w=1, wtimeout=700)
            )
            with self.assert_called_with_write_concern(WriteConcern(w=1, wtimeout=700)):
                yield coll.insert_one({"x": 42})

            with self.assert_called_with_write_concern(WriteConcern(j=True)):
                yield coll.with_options(write_concern=WriteConcern(j=True)).insert_one(
                    {"x": 42}
                )

        finally:
            yield conn.mydb.mycol.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AllOperations(self):
        # Test that all listed methods pass write concern to the server

        conn = MongoConnection(mongo_host, mongo_port, w=1)
        coll = conn.mydb.mycol

        methods = [
            (Collection.insert_one, {"x": 42}),
            (Collection.insert_many, [{"x": 42}]),
            (Collection.update_one, {}, {"$set": {"x": 42}}),
            (Collection.update_many, {}, {"$set": {"x": 42}}),
            (Collection.replace_one, {}, {"x": 42}),
            (Collection.delete_one, {}),
            (Collection.delete_many, {}),
        ]

        try:
            for method, *args in methods:
                with self.assert_called_with_write_concern(WriteConcern(w=1)):
                    yield method(coll, *copy.deepcopy(args))
                with self.assert_called_with_write_concern(WriteConcern(w=1, j=True)):
                    yield method(
                        coll.with_options(write_concern=WriteConcern(w=1, j=True)),
                        *copy.deepcopy(args),
                    )
        finally:
            yield coll.drop()
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionUrlParams(self):
        conn = ConnectionPool(
            "mongodb://{0}:{1}/?w=1&journal=true&wtimeoutms=700".format(
                mongo_host, mongo_port
            )
        )
        coll = conn.mydb.mycol

        try:
            with self.assert_called_with_write_concern(
                WriteConcern(w=1, j=True, wtimeout=700)
            ):
                yield coll.insert_one({"x": 42})
        finally:
            yield coll.drop()
            yield conn.disconnect()
