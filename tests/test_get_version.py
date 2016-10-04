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

class TestGetVersion(unittest.TestCase):

    def setUp(self):
        self.conn = connection.ConnectionPool("mongodb://127.0.0.1/dbname")
        self.gfs = gridfs.GridFS(self.conn)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.conn.disconnect()

    def test_GetDefaultDatabase(self):
        self.assertEqual(self.named_conn.get_default_database().name,
                         self.named_conn["dbname"].name)

    @defer.inlineCallbacks
    def test_GFSVersion(self):
        for n in xrange(0,10):
            yield self.gfs.put("Hello" + str(n), filename="world")

        doc = yield self.gfs.get_last_version("world")
        text = yield doc.read()
        self.assertEqual(text, 'Hello10')

