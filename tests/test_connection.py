# coding: utf-8
# Copyright 2010 Mark L.
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
from twisted.internet import base, defer
import txmongo

mongo_host = "localhost"
mongo_port = 27017
base.DelayedCall.debug = True


class TestMongoConnection(unittest.TestCase):

    def setUp(self):
        self.named_conn = txmongo.connection.ConnectionPool("mongodb://localhost/dbname")
        self.unnamed_conn = txmongo.connection.ConnectionPool("mongodb://localhost/")

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.named_conn.disconnect()
        yield self.unnamed_conn.disconnect()

    def test_GetDefaultDatabase(self):
        self.assertEqual(self.named_conn.get_default_database().name,
                         self.named_conn["dbname"].name)
        self.assertEqual(self.unnamed_conn.get_default_database(), None)
