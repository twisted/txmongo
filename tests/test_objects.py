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

from __future__ import absolute_import, division

import time

from io import BytesIO as StringIO

import os
from bson import objectid, timestamp
from pymongo.write_concern import WriteConcern
import txmongo
from txmongo import database, collection, filter as qf
from txmongo.gridfs import GridFS, GridIn, GridOut, GridOutIterator, errors
from txmongo._gridfs.errors import NoFile
from twisted.trial import unittest
from twisted.internet import defer
from twisted.python.compat import _PY3
from twisted import _version

mongo_host = "127.0.0.1"
mongo_port = 27017


class TestMongoObjects(unittest.TestCase):
    @defer.inlineCallbacks
    def test_MongoObjects(self):
        """ Tests creating mongo objects """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        mydb = conn.mydb
        self.assertEqual(isinstance(mydb, database.Database), True)
        if _PY3:
            self.assertEqual(repr(mydb), "Database(Connection('127.0.0.1', 27017), 'mydb')")
        else:
            self.assertEqual(repr(mydb), "Database(Connection('127.0.0.1', 27017), u'mydb')")
        self.assertEqual(repr(mydb("mydb2")), repr(mydb.__call__("mydb2")))
        mycol = mydb.mycol
        self.assertEqual(isinstance(mycol, collection.Collection), True)
        mycol2 = yield mydb.create_collection("mycol2")
        self.assertEqual(isinstance(mycol2, collection.Collection), True)
        mycol3 = yield mydb.create_collection("mycol3", {"size": 1000})
        self.assertEqual(isinstance(mycol3, collection.Collection), True)
        yield mydb.drop_collection("mycol3")
        yield mydb.drop_collection(mycol3)
        yield self.assertFailure(mydb.drop_collection(None), TypeError)
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Properties(self):
        conn = txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.mydb
        coll = db.mycol

        try:
            # names
            self.assertEqual(db.name, u"mydb")
            self.assertEqual(coll.name, u"mycol")
            self.assertEqual(coll.full_name, u"mydb.mycol")
            self.assertEqual(coll.subcoll.name, u"mycol.subcoll")
            self.assertEqual(coll.subcoll.full_name, u"mydb.mycol.subcoll")

            # database
            self.assertTrue(coll.database is db)

            # Write concern
            w2 = coll.with_options(write_concern=WriteConcern(w=2))
            dbw2 = database.Database(conn, "mydb", write_concern=WriteConcern(w=2))
            self.assertEqual(w2.write_concern, WriteConcern(w=2))
            self.assertEqual(dbw2.write_concern, WriteConcern(w=2))

            # Connection
            self.assertTrue(db.connection is conn)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_MongoOperations(self):
        """ Tests mongo operations """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        test = conn.foo.test

        # insert
        doc = {"foo": "bar", "items": [1, 2, 3]}
        yield test.insert(doc, safe=True)
        result = yield test.find_one(doc)
        self.assertEqual("_id" in result, True)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["items"], [1, 2, 3])

        # insert preserves object id
        doc.update({"_id": objectid.ObjectId()})
        yield test.insert(doc, safe=True)
        result = yield test.find_one(doc)
        self.assertEqual(result.get("_id"), doc.get("_id"))
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["items"], [1, 2, 3])

        # update
        yield test.update({"_id": result["_id"]}, {"$set": {"one": "two"}}, safe=True)
        result = yield test.find_one({"_id": result["_id"]})
        self.assertEqual(result["one"], "two")

        # delete
        yield test.remove(result["_id"], safe=True)

        # disconnect
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Timestamps(self):
        """Tests mongo operations with Timestamps"""
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        test = conn.foo.test_ts

        test.drop()

        # insert with specific timestamp
        doc1 = {"_id": objectid.ObjectId(),
                "ts": timestamp.Timestamp(1, 2)}
        yield test.insert(doc1, safe=True)

        result = yield test.find_one(doc1)
        self.assertEqual(result.get("ts").time, 1)
        self.assertEqual(result.get("ts").inc, 2)

        # insert with specific timestamp
        doc2 = {"_id": objectid.ObjectId(),
                "ts": timestamp.Timestamp(2, 1)}
        yield test.insert(doc2, safe=True)

        # the objects come back sorted by ts correctly.
        # (test that we stored inc/time in the right fields)
        result = yield test.find(filter=qf.sort(qf.ASCENDING("ts")))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["_id"], doc1["_id"])
        self.assertEqual(result[1]["_id"], doc2["_id"])

        # insert with null timestamp
        doc3 = {"_id": objectid.ObjectId(),
                "ts": timestamp.Timestamp(0, 0)}
        yield test.insert(doc3, safe=True)

        # time field loaded correctly
        result = yield test.find_one(doc3["_id"])
        now = time.time()
        self.assertTrue(now - 2 <= result["ts"].time <= now)

        # delete
        yield test.remove(doc1["_id"], safe=True)
        yield test.remove(doc2["_id"], safe=True)
        yield test.remove(doc3["_id"], safe=True)

        # disconnect
        yield conn.disconnect()


class TestGridFsObjects(unittest.TestCase):
    """ Test the GridFS operations from txmongo._gridfs """
    @defer.inlineCallbacks
    def _disconnect(self, conn):
        """ Disconnect the connection """
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_GridFileObjects(self):
        """ Tests gridfs objects """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        db.fs.files.remove({})  # drop all objects there first
        db.fs.chunks.remove({})
        self.assertRaises(TypeError, GridFS, None)
        _ = GridFS(db)  # Default collection
        self.assertRaises(TypeError, GridIn, None)
        with GridIn(db.fs, filename="test_with", contentType="text/plain", chunk_size=1024):
            pass
        grid_in_file = GridIn(db.fs, filename="test_1", contentType="text/plain",
                              content_type="text/plain", chunk_size=65536, length=1048576,
                              upload_date="20150101")
        self.assertFalse(grid_in_file.closed)
        if _version.version.major >= 15:
            with self.assertRaises(TypeError):
                yield grid_in_file.write(1)
            with self.assertRaises(TypeError):
                yield grid_in_file.write(u"0xDEADBEEF")
            with self.assertRaises(AttributeError):
                _ = grid_in_file.test
        grid_in_file.test = 1
        yield grid_in_file.write(b"0xDEADBEEF")
        yield grid_in_file.write(b"0xDEADBEEF"*1048576)
        fake_doc = {"_id": "test_id", "length": 1048576, "filename": "test",
                    "upload_date": "20150101"}
        self.assertRaises(TypeError, GridOut, None, None)
        grid_out_file = GridOut(db.fs, fake_doc)
        if _version.version.major >= 15:
            with self.assertRaises(AttributeError):
                _ = grid_out_file.testing
        self.assertEqual("test", grid_out_file.filename)
        self.assertEqual(0, grid_out_file.tell())
        grid_out_file.seek(1024)
        self.assertEqual(1024, grid_out_file.tell())
        grid_out_file.seek(1024, os.SEEK_CUR)
        self.assertEqual(2048, grid_out_file.tell())
        grid_out_file.seek(0, os.SEEK_END)
        self.assertEqual(1048576, grid_out_file.tell())
        self.assertRaises(IOError, grid_out_file.seek, 0, 4)
        self.assertRaises(IOError, grid_out_file.seek, -1)
        self.assertTrue("'_id': 'test_id'" in repr(grid_out_file))
        self.assertTrue("20150101", grid_in_file.upload_date)
        yield grid_in_file.writelines([b"0xDEADBEEF", b"0xDEADBEAF"])
        yield grid_in_file.close()
        if _version.version.major >= 15:
            with self.assertRaises(AttributeError):
                grid_in_file.length = 1
        self.assertEqual(1, grid_in_file.test)
        if _version.version.major >= 15:
            with self.assertRaises(AttributeError):
                _ = grid_in_file.test_none
        self.assertTrue(grid_in_file.closed)
        if _version.version.major >= 15:
            with self.assertRaises(ValueError):
                yield grid_in_file.write(b"0xDEADBEEF")
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_GridFsObjects(self):
        """ Tests gridfs objects """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        db.fs.files.remove({})  # drop all objects there first
        db.fs.chunks.remove({})
        gfs = GridFS(db)  # Default collection
        yield gfs.delete(u"test")

        _ = gfs.new_file(filename="test_1", contentType="text/plain", chunk_size=65536)
        yield conn.disconnect()
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        db.fs.files.remove({})  # drop all objects there first
        gfs = GridFS(db)  # Default collection
        _ = yield gfs.put(b"0xDEADBEEF", filename="test_2", contentType="text/plain",
                          chunk_size=65536)
        # disconnect
        yield conn.disconnect()

        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        gfs = GridFS(db)  # Default collection
        _ = yield gfs.get("test_3")
        # disconnect
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_GridFsIteration(self):
        """ Tests gridfs iterator """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        db.fs.files.remove({})  # drop all objects there first
        db.fs.chunks.remove({})
        gfs = GridFS(db)  # Default collection
        new_file = gfs.new_file(filename="testName", contentType="text/plain", length=1048576,
                                chunk_size=4096)
        yield new_file.write(b"0xDEADBEEF"*4096*2)
        yield new_file.close()
        fake_doc = {"_id": new_file._id, "name": "testName", "length": 4096*2, "chunkSize": 4096,
                    "contentType": "text/plain"}
        grid_out_file = GridOut(db.fs, fake_doc)
        iterator = GridOutIterator(grid_out_file, db.fs.chunks)
        next_it = yield next(iterator)
        self.assertEqual(len(next_it), 4096)
        _ = yield next(iterator)
        next_it = yield next(iterator)
        self.assertEqual(next_it, None)

        fake_bad_doc = {"_id": "bad_id", "name": "testName", "length": 4096*2,
                        "chunkSize": 4096, "contentType": "text/plain"}
        grid_bad_out_file = GridOut(db.fs, fake_bad_doc)
        bad_iterator = GridOutIterator(grid_bad_out_file, db.fs.chunks)
        if _version.version.major >= 15:
            with self.assertRaises(errors.CorruptGridFile):
                next_it = yield bad_iterator.next()

        # disconnect
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_GridFsOperations(self):
        """ Tests gridfs operations """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        db.fs.files.remove({})  # Drop files first TODO: iterate through files and delete them

        # Don't forget to disconnect
        self.addCleanup(self._disconnect, conn)
        try:
            in_file = StringIO(b"Test input string")
            out_file = StringIO()
        except Exception as e:
            self.fail("Failed to create memory files for testing: %s" % e)

        g_out = None

        try:
            # Tests writing to a new gridfs file
            gfs = GridFS(db)  # Default collection
            if _version.version.major >= 15:
                with self.assertRaises(NoFile):
                    yield gfs.get_last_version("optest")

            g_in = gfs.new_file(filename="optest", contentType="text/plain",
                                chunk_size=65536)  # non-default chunk size used
            # yielding to ensure writes complete before we close and close before we try to read
            yield g_in.write(in_file.read())
            yield g_in.close()

            # Tests reading from an existing gridfs file
            g_out = yield gfs.get_last_version("optest")
            data = yield g_out.read()
            out_file.write(data)
            _id = g_out._id
        except Exception as e:
            self.fail("Failed to communicate with the GridFS. " +
                      "Is MongoDB running? %s" % e)
        else:
            self.assertEqual(in_file.getvalue(), out_file.getvalue(),
                             "Could not read the value from writing an input")
        finally:
            in_file.close()
            out_file.close()
            if g_out:
                g_out.close()

        listed_files = yield gfs.list()
        self.assertEqual(["optest"], listed_files,
                         "`optest` is the only expected file and we received %s" % listed_files)

        yield gfs.delete(_id)
