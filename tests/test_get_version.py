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
from mock import patch
from time import time
from twisted.trial import unittest
from twisted.internet import defer
from txmongo import connection, gridfs
from txmongo.utils import check_deadline
from txmongo.errors import TimeExceeded
from txmongo._gridfs.errors import NoFile

class TestGFS(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        self.db = self.conn['dbname']
        self.gfs = gridfs.GridFS(self.db)
        for n in xrange(0,10):
            yield self.gfs.put("Hello" + str(n), filename="world")

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.command('dropDatabase')
        yield self.conn.disconnect()

    @defer.inlineCallbacks
    def test_GFSVersion_strip(self):
        docs = yield self.gfs.list()
        self.assertEqual(docs, ['world'])
        index = 9
        while True:
            try:
                doc = yield self.gfs.get_last_version('world')
                text = yield doc.read()
                self.assertEqual(text, 'Hello' + str(index))
                index -= 1
                yield self.gfs.delete(doc._id)
            except NoFile:
                break
        try:
            doc = yield self.gfs.get_last_version('world')
            self.assertEqual(doc, None)
        except NoFile:
            return

    @defer.inlineCallbacks
    def test_GFSVersion_last(self):
        doc = yield self.gfs.get_last_version("world")
        text = yield doc.read()
        self.assertEqual(text, 'Hello9')

    @defer.inlineCallbacks
    def test_GFSVersion_get_last(self):
        doc = yield self.gfs.get_version("world",-1)
        text = yield doc.read()
        self.assertEqual(text, 'Hello9')
            
    @defer.inlineCallbacks
    def test_GFSVersion_first(self):
        doc = yield self.gfs.get_version("world",0)
        text = yield doc.read()
        self.assertEqual(text, 'Hello0')

    @defer.inlineCallbacks
    def test_GFSVersion_second(self):
        doc = yield self.gfs.get_version("world",1)
        text = yield doc.read()
        self.assertEqual(text, 'Hello1')

    @defer.inlineCallbacks
    def test_GFSVersion_last_but_1(self):
        doc = yield self.gfs.get_version("world",-2)
        text = yield doc.read()
        self.assertEqual(text, 'Hello8')
