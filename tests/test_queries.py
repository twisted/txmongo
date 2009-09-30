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

base.DelayedCall.debug = True 

def Query(inpool = True, safe = True, cmd = 'insert'):
    def testWrapper(q):
        if inpool:
            db = yield pymonga.ConnectionPool(host=DB_HOST, port=DB_PORT, reconnect=False)
        else:
            db = yield pymonga.Connection(host=DB_HOST, port=DB_PORT, reconnect=False)
            
        pymonga_db = db[DB_NAME]
        test = pymonga_db.test_collection

        if cmd == 'insert':
            yield test.insert(dummy_doc, safe=safe)
        elif cmd == 'update':
            yield test.update({'dummy_key': u'0'}, dummy_doc, safe=safe)
        elif cmd == 'save':
            yield test.save(dummy_doc, safe=safe)
        
        test.drop(safe=True)
        db.disconnect()
 
    return testWrapper       

class TestPYMONGAQueries(unittest.TestCase):
    """Test querying for the mongoDB asynchronous python driver."""

    testInsertSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'insert', safe = True))
    testInsertNonSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'insert', safe = False))
    testInsertInPoolSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'insert', safe = True))
    testInsertInPoolNonSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'insert', safe = False))

    testUpdateSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'update', safe = True))
    testUpdateNonSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'update', safe = False))
    testUpdateInPoolSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'update', safe = True))
    testUpdateInPoolNonSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'update', safe = False))

    testSaveSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'save', safe = True))
    testSaveNonSafe = defer.inlineCallbacks(Query(inpool = False, cmd = 'save', safe = False))
    testSaveInPoolSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'save', safe = True))
    testSaveInPoolNonSafe = defer.inlineCallbacks(Query(inpool = True, cmd = 'save', safe = False))
