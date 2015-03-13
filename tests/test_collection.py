# Copyright 2012-2015 TxMongo Developers
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

"""Test the collection module.
Based on pymongo driver's test_collection.py
"""

from bson.son import SON
from pymongo import errors

from twisted.internet import defer
from twisted.trial import unittest

import txmongo

from txmongo import filter
from txmongo.collection import Collection

mongo_host = "localhost"
mongo_port = 27017


class TestIndexInfo(unittest.TestCase):
    
    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()

    @defer.inlineCallbacks
    def test_collection(self):
        self.assertRaises(TypeError, Collection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(errors.InvalidName, make_col, self.db, "")
        self.assertRaises(errors.InvalidName, make_col, self.db, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes\x00t")
        self.assertRaises(TypeError, self.coll.save, "test")
        self.assertRaises(ValueError, self.coll.filemd5, "test")
        self.assertFailure(self.db.test.find(spec="test"), TypeError)
        self.assertFailure(self.db.test.find(fields="test"), TypeError)
        self.assertFailure(self.db.test.find(skip="test"), TypeError)
        self.assertFailure(self.db.test.find(limit="test"), TypeError)
        self.assertFailure(self.db.test.insert([1]), TypeError)
        self.assertFailure(self.db.test.insert(1), TypeError)
        self.assertFailure(self.db.test.update(1, 1), TypeError)
        self.assertFailure(self.db.test.update({}, 1), TypeError)
        self.assertFailure(self.db.test.update({}, {}, 'a'), TypeError)

        self.assert_(isinstance(self.db.test, Collection))
        self.assertEqual(NotImplemented, self.db.test.__cmp__(7))
        self.assertNotEqual(cmp(self.db.test, 7), 0)
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])
        self.assertEqual(repr(self.db.test), "Collection(mydb, test)")
        self.assertEqual(self.db.test.test, self.db.test("test"))

        options = yield self.db.test.options()
        self.assertIsInstance(options, dict)

        yield self.db.drop_collection("test")
        collection_names = yield self.db.collection_names()
        self.assertFalse("test" in collection_names)

    @defer.inlineCallbacks
    def test_create_index(self):
        db = self.db
        coll = self.coll

        self.assertRaises(TypeError, coll.create_index, 5)
        self.assertRaises(TypeError, coll.create_index, {"hello": 1})

        yield coll.insert({'c': 1})  # make sure collection exists.

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 1)

        yield coll.create_index(filter.sort(filter.ASCENDING("hello")))
        yield coll.create_index(filter.sort(filter.ASCENDING("hello") +
                                filter.DESCENDING("world")))

        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 3)

        yield coll.drop_indexes()
        ix = yield coll.create_index(filter.sort(filter.ASCENDING("hello") +
                                     filter.DESCENDING("world")), name="hello_world")
        self.assertEquals(ix, "hello_world")

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 1)
        
        yield coll.create_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield db.system.indexes.find({"ns": u"mydb.mycol"}) 
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 1)

        ix = yield coll.create_index(filter.sort(filter.ASCENDING("hello") +
                                     filter.DESCENDING("world")))
        self.assertEquals(ix, "hello_1_world_-1")
    
    @defer.inlineCallbacks
    def test_create_index_nodup(self):
        coll = self.coll

        yield coll.drop()
        yield coll.insert({'b': 1})
        yield coll.insert({'b': 1})

        ix = coll.create_index(filter.sort(filter.ASCENDING("b")), unique=True)
        yield self.assertFailure(ix, errors.DuplicateKeyError)

    @defer.inlineCallbacks
    def test_ensure_index(self):
        db = self.db
        coll = self.coll
        
        yield coll.ensure_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield db.system.indexes.find({"ns": u"mydb.mycol"}) 
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield coll.drop_indexes()

    @defer.inlineCallbacks
    def test_index_info(self):
        db = self.db

        yield db.test.drop_indexes()
        yield db.test.remove({})

        db.test.save({})  # create collection
        ix_info = yield db.test.index_information()
        self.assertEqual(len(ix_info), 1)
        self.assertEqual(ix_info["_id_"]["name"], "_id_")

        yield db.test.create_index(filter.sort(filter.ASCENDING("hello")))
        ix_info = yield db.test.index_information()
        self.assertEqual(len(ix_info), 2)
        self.assertEqual(ix_info["hello_1"]["name"], "hello_1")

        yield db.test.create_index(filter.sort(filter.DESCENDING("hello") +
                                               filter.ASCENDING("world")),
                                   unique=True, sparse=True)
        ix_info = yield db.test.index_information()
        self.assertEqual(ix_info["hello_1"]["name"], "hello_1")
        self.assertEqual(len(ix_info), 3)
        self.assertEqual({"hello": -1, "world": 1}, ix_info["hello_-1_world_1"]["key"])
        self.assertEqual(True, ix_info["hello_-1_world_1"]["unique"])
        self.assertEqual(True, ix_info["hello_-1_world_1"]["sparse"])

        yield db.test.drop_indexes()
        yield db.test.remove({})

    @defer.inlineCallbacks
    def test_index_geo2d(self):
        coll = self.coll 
        yield coll.drop_indexes()
        geo_ix = yield coll.create_index(filter.sort(filter.GEO2D("loc")))

        self.assertEqual("loc_2d", geo_ix)

        index_info = yield coll.index_information()
        self.assertEqual({"loc": "2d"}, index_info["loc_2d"]["key"])

    @defer.inlineCallbacks
    def test_index_geo2dsphere(self):
        coll = self.coll 
        yield coll.drop_indexes()
        geo_ix = yield coll.create_index(filter.sort(filter.GEO2DSPHERE("loc")))

        self.assertEqual("loc_2dsphere", geo_ix)
        index_info = yield coll.index_information()

        self.assertEqual(index_info["loc_2dsphere"]["key"], {"loc": "2dsphere"})

    @defer.inlineCallbacks
    def test_index_haystack(self):
        db = self.db
        coll = self.coll
        yield coll.drop_indexes()

        _id = yield coll.insert({
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        })
        yield coll.insert({
            "pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"
        })
        yield coll.insert({
            "pos": {"long": 59.1, "lat": 87.2}, "type": "office"
        })

        yield coll.create_index(filter.sort(filter.GEOHAYSTACK("pos") +
                                            filter.ASCENDING("type")), **{"bucket_size": 1})

        # TODO: A db.command method has not been implemented yet.
        # Sending command directly
        command = SON([
            ("geoSearch", "mycol"),
            ("near", [33, 33]),
            ("maxDistance", 6),
            ("search", {"type": "restaurant"}),
            ("limit", 30),
        ])
           
        results = yield db["$cmd"].find_one(command)
        self.assertEqual(2, len(results["results"]))
        self.assertEqual({
            "_id": _id,
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }, results["results"][0])
