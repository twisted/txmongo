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
from StringIO import StringIO

import txmongo
from txmongo import database
from txmongo import collection
from txmongo import gridfs
from txmongo._pymongo import objectid
from txmongo._gridfs import GridIn, GridOut
from twisted.trial import unittest
from twisted.trial import runner
from twisted.internet import base, defer, reactor

mongo_host="localhost"
mongo_port=27017
base.DelayedCall.debug = False

class TestMongoQueries(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_SingleCursorIteration(self):
        yield self.coll.insert([{'v':i} for i in xrange(10)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 10)

    @defer.inlineCallbacks
    def test_MultipleCursorIterations(self):
        yield self.coll.insert([{'v':i} for i in xrange(200)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 200)
        print len(res)

    @defer.inlineCallbacks
    def test_LargeData(self):
        yield self.coll.insert([{'v':' '*(2**19)} for i in xrange(4)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 4)
        print len(res)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


class TestMongoQueriesEdgeCases(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.coll = self.conn.mydb.mycol

    @defer.inlineCallbacks
    def test_BelowBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(100)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 100)

    @defer.inlineCallbacks
    def test_EqualToBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(101)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 101)

    @defer.inlineCallbacks
    def test_AboveBatchThreshold(self):
        yield self.coll.insert([{'v':i} for i in xrange(102)], safe=True)
        res = yield self.coll.find()
        self.assertEqual(len(res), 102)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()


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
        doc = {"foo":"bar", "items":[1, 2, 3]}
        yield test.insert(doc, safe=True)
        result = yield test.find_one(doc)
        self.assertEqual(result.has_key("_id"), True)
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["items"], [1, 2, 3])
        
        # insert preserves object id
        doc.update({'_id': objectid.ObjectId()})
        yield test.insert(doc, safe=True)
        result = yield test.find_one(doc)
        self.assertEqual(result.get('_id'), doc.get('_id'))
        self.assertEqual(result["foo"], "bar")
        self.assertEqual(result["items"], [1, 2, 3])

        # update
        yield test.update({"_id":result["_id"]}, {"$set":{"one":"two"}}, safe=True)
        result = yield test.find_one({"_id":result["_id"]})
        self.assertEqual(result["one"], "two")

        # delete
        yield test.remove(result["_id"], safe=True)

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
        collection = db.fs
        
        gfs = gridfs.GridFS(db) # Default collection
        
        gridin = GridIn(collection, filename='test', contentType="text/plain",
                        chunk_size=2**2**2**2)
        new_file = gfs.new_file(filename='test2', contentType="text/plain",
                        chunk_size=2**2**2**2)
        
        # disconnect
        yield conn.disconnect()
        
    @defer.inlineCallbacks
    def test_GridFsOperations(self):
        """ Tests gridfs operations """
        conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        db = conn.test
        collection = db.fs
        
        # Don't forget to disconnect
        self.addCleanup(self._disconnect, conn)
        try:
            in_file = StringIO("Test input string")
            out_file = StringIO()
        except Exception, e:
            self.fail("Failed to create memory files for testing: %s" % e)
        
        try:
            # Tests writing to a new gridfs file
            gfs = gridfs.GridFS(db) # Default collection
            g_in = gfs.new_file(filename='optest', contentType="text/plain",
                            chunk_size=2**2**2**2) # non-default chunk size used
            # yielding to ensure writes complete before we close and close before we try to read
            yield g_in.write(in_file.read())
            yield g_in.close()
            
            # Tests reading from an existing gridfs file
            g_out = yield gfs.get_last_version('optest')
            data = yield g_out.read()
            out_file.write(data)
            _id = g_out._id
        except Exception,e:
            self.fail("Failed to communicate with the GridFS. " +
                      "Is MongoDB running? %s" % e)
        else:
            self.assertEqual(in_file.getvalue(), out_file.getvalue(),
                         "Could not read the value from writing an input")        
        finally:
            in_file.close()
            out_file.close()
            g_out.close()

        
        listed_files = yield gfs.list()
        self.assertEqual(['optest'], listed_files,
                         "'optest' is the only expected file and we received %s" % listed_files)
        
        yield gfs.delete(_id)

if __name__ == "__main__":
    suite = runner.TrialSuite((TestMongoObjects, TestGridFsObjects))
    suite.run()
