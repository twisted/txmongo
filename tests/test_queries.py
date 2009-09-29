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

#base.DelayedCall.debug = True 

class TestPYMONGAQueries(unittest.TestCase):
    """Test querying for the mongoDB asynchronous python driver."""

    @defer.inlineCallbacks
    def testQuerySafe(self):
        db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        pymonga_db = db[DB_NAME]
        test = pymonga_db.test_collection
        yield test.insert(dummy_doc, safe=True)
        test.drop(safe=True)
        yield db.disconnect() 

    @defer.inlineCallbacks
    def testQueryNonSafe(self):
        db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        pymonga_db = db[DB_NAME]
        test = pymonga_db.test_collection
        yield test.insert(dummy_doc, safe=False)
        test.drop(safe=True)
        yield db.disconnect() 

    @defer.inlineCallbacks
    def testQueryInPoolSafe(self):
        db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        pymonga_db = db[DB_NAME]
        test = pymonga_db.test_collection
        yield test.insert(dummy_doc, safe=True)
        test.drop(safe=True)
        yield db.disconnect() 

    @defer.inlineCallbacks
    def testQueryInPoolNonSafe(self):
        db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        pymonga_db = db[DB_NAME]
        test = pymonga_db.test_collection
        yield test.insert(dummy_doc, safe=False)
        test.drop(safe=True)
        yield db.disconnect() 
