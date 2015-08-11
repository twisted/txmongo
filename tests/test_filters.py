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

from __future__ import absolute_import, division

from twisted.trial import unittest
from twisted.internet import defer
import txmongo
import txmongo.filter as qf
from pymongo.errors import OperationFailure

mongo_host = "localhost"
mongo_port = 27017


class TestMongoFilters(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol
        yield self.coll.insert({'x': 42})

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.db.system.profile.drop()
        yield self.conn.disconnect()


    @defer.inlineCallbacks
    def test_Hint(self):
        # find() should fail with 'bad hint' if hint specifier works correctly
        self.assertFailure(self.coll.find({}, filter=qf.hint([('x', 1)])), OperationFailure)

        # create index and test it is honoured
        yield self.coll.create_index(qf.sort(qf.ASCENDING("x")), name="test_index")
        found_1 = yield self.coll.find({}, filter=qf.hint([('x', 1)]))
        found_2 = yield self.coll.find({}, filter=qf.hint(qf.ASCENDING("x")))
        found_3 = yield self.coll.find({}, filter=qf.hint("test_index"))
        self.assertTrue(found_1 == found_2 == found_3)

        # find() should fail with 'bad hint' if hint specifier works correctly
        self.assertFailure(self.coll.find({}, filter=qf.hint(["test_index", 1])), OperationFailure)
        self.assertFailure(self.coll.find({}, filter=qf.hint(qf.ASCENDING("test_index"))),
                           OperationFailure)

    def test_SortAscendingMultipleFields(self):
        self.assertEqual(qf.sort(qf.ASCENDING(['x', 'y'])), qf.sort(qf.ASCENDING('x') +
                                                                    qf.ASCENDING('y')))

    def test_SortOneLevelList(self):
        self.assertEqual(qf.sort([('x', 1)]), qf.sort(('x', 1)))

    def test_SortInvalidKey(self):
        self.assertRaises(TypeError, qf.sort, [(1, 2)])
        self.assertRaises(TypeError, qf.sort, [('x', 3)])

    def test_SortGeoIndexes(self):
        self.assertEqual(qf.sort(qf.GEO2D('x')), qf.sort([('x', "2d")]))
        self.assertEqual(qf.sort(qf.GEO2DSPHERE('x')), qf.sort([('x', "2dsphere")]))
        self.assertEqual(qf.sort(qf.GEOHAYSTACK('x')), qf.sort([('x', "geoHaystack")]))

    @defer.inlineCallbacks
    def __test_simple_filter(self, filter, optionname, optionvalue):
        # Checking that `optionname` appears in profiler log with specified value

        yield self.db.command("profile", 2)
        yield self.coll.find({}, filter=filter)
        yield self.db.command("profile", 0)

        cnt = yield self.db.system.profile.count({"query." + optionname: optionvalue})
        self.assertEqual(cnt, 1)

    @defer.inlineCallbacks
    def test_Comment(self):
        comment = "hello world"

        yield self.__test_simple_filter(qf.comment(comment), "$comment", comment)

    @defer.inlineCallbacks
    def test_Snapshot(self):
        yield self.__test_simple_filter(qf.snapshot(), "$snapshot", True)

    @defer.inlineCallbacks
    def test_Explain(self):
        result = yield self.coll.find({}, filter=qf.explain())
        self.assertTrue("executionStats" in result[0] or "nscanned" in result[0])

    @defer.inlineCallbacks
    def test_FilterMerge(self):
        self.assertEqual(qf.sort(qf.ASCENDING('x') + qf.DESCENDING('y')),
                         qf.sort(qf.ASCENDING('x')) + qf.sort(qf.DESCENDING('y')))

        comment = "hello world"

        yield self.db.command("profile", 2)
        yield self.coll.find({}, filter=qf.sort(qf.ASCENDING('x')) + qf.comment(comment))
        yield self.db.command("profile", 0)

        cnt = yield self.db.system.profile.count({"query.$orderby.x": 1,
                                                  "query.$comment": comment})
        self.assertEqual(cnt, 1)

    def test_Repr(self):
        self.assertEqual(repr(qf.sort(qf.ASCENDING('x'))),
                         "<mongodb QueryFilter: {'orderby': (('x', 1),)}>")
