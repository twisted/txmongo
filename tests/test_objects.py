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

from twisted.trial import unittest
from twisted.internet import reactor, defer, base

import pymonga
from pymonga._pymongo.objectid import ObjectId

# mongoDB connection configuration.
# Change this accordingly.
DB_HOST = 'localhost'
DB_PORT = 27017
DB_NAME = 'pymonga_test'

dummy_doc = {'dummy_key': u'01'*100}

class TestPYMONGA(unittest.TestCase):
    """Test the mongoDB asynchronous python driver."""

    @defer.inlineCallbacks
    def testConnection(self):
        db = yield pymonga.Connection(host=DB_HOST, port=DB_PORT, reconnect=False) 
        print db 

        # db and collection 
        pymonga_db = db.pymonga_db 
        test = pymonga_db.test_collection

        # test insert doc and its ObjectId instance
        yield test.insert(dummy_doc, safe=True) 
        doc = yield test.find_one() 
        self.assertEqual(doc.get('dummy_key'), dummy_doc.get('dummy_key'))
        assert isinstance(doc['_id'], ObjectId)

        # test remove doc
        yield test.remove(doc['_id'], safe=True) 

        # test drop collection
        yield test.drop(safe=True) 

        # test drop db
        #db.drop_database(pymonga_db)	

        yield db.disconnect() 
        print db

    @defer.inlineCallbacks
    def testConnectionPool(self):
        db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        print db

        # db and collection 
        pymonga_db = db[DB_NAME] 
        test = pymonga_db.test_collection
        self.assertEqual(repr(test), "<mongodb Collection: %s.test_collection>" % DB_NAME)

        ld = []
        for i in xrange(1000):
            ld.append(test.insert({"x": i}, safe=True ))

        yield defer.DeferredList(ld)

        total = yield test.count()
        self.assertEqual(total, 1000)

        yield test.drop(safe=True) 
        #db.drop_database(pymonga_db)	

        yield db.disconnect()	
        print db
