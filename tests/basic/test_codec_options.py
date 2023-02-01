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

from __future__ import absolute_import, division

import datetime
from bson import SON
from bson.codec_options import CodecOptions
from twisted.trial import unittest
from twisted.internet import defer
from txmongo.connection import MongoConnection
from txmongo.database import Database

mongo_host = "localhost"
mongo_port = 27017


class TestCodecOptions(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.db
        self.coll = self.db.coll
        yield self.coll.insert_one({'x': 42, 'y': datetime.datetime.now()})

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()

    @defer.inlineCallbacks
    def test_Levels(self):
        as_son = CodecOptions(document_class=SON)

        doc = yield self.coll.find_one()
        self.assertIsInstance(doc, dict)

        try:
            conn = MongoConnection(mongo_host, mongo_port, codec_options=as_son)
            doc = yield conn.db.coll.find_one()
            self.assertIsInstance(doc, SON)
        finally:
            yield conn.disconnect()

        doc = yield Database(self.conn, "db", codec_options=as_son).coll.find_one()
        self.assertIsInstance(doc, SON)

        doc = yield self.coll.with_options(codec_options=as_son).find_one()
        self.assertIsInstance(doc, SON)

    @defer.inlineCallbacks
    def test_TzAware(self):
        doc = yield self.coll.find_one()
        self.assertIsNone(doc['y'].tzinfo, None)

        tz_aware = CodecOptions(tz_aware=True)
        doc = yield self.coll.with_options(codec_options=tz_aware).find_one()
        self.assertIsNotNone(doc['y'].tzinfo, None)
