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
import time
from StringIO import StringIO

from bson import objectid, timestamp
import txmongo
from txmongo import database
from txmongo import collection
from txmongo import gridfs
from txmongo import filter as qf
from txmongo._gridfs import GridIn
from twisted.trial import unittest
from twisted.internet import base, defer

mongo_host = "localhost"
mongo_port = 27017
base.DelayedCall.debug = True


class TestMongoObjects(unittest.TestCase):
    @defer.inlineCallbacks
    def test_MongoObjects(self):
        """ Tests creating mongo objects """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        mydb = conn.mydb
        self.assertEqual(isinstance(mydb, database.Database), True)
        mycol = mydb.mycol
        self.assertEqual(isinstance(mycol, collection.Collection), True)
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
    def test_GridFsObjects(self):
        """ Tests gridfs objects """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        
        gfs = gridfs.GridFS(db)  # Default collection
        
        _ = GridIn(db.fs, filename="test", contentType="text/plain",
                   chunk_size=2**2**2**2)
        _ = gfs.new_file(filename="test2", contentType="text/plain",
                         chunk_size=2**2**2**2)
        
        # disconnect
        yield conn.disconnect()
        
    @defer.inlineCallbacks
    def test_GridFsOperations(self):
        """ Tests gridfs operations """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        
        # Don't forget to disconnect
        self.addCleanup(self._disconnect, conn)
        try:
            in_file = StringIO("Test input string")
            out_file = StringIO()
        except Exception as e:
            self.fail("Failed to create memory files for testing: %s" % e)

        g_out = None
        
        try:
            # Tests writing to a new gridfs file
            gfs = gridfs.GridFS(db)  # Default collection
            g_in = gfs.new_file(filename="optest", contentType="text/plain",
                                chunk_size=2**2**2**2)  # non-default chunk size used
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
