# -*- coding: utf-8 -*-

# Copyright 2012 Renzo S.
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

from twisted.trial import unittest

from bson.dbref import DBRef
from bson.son import SON
from bson.objectid import ObjectId


class TestDBRef(unittest.TestCase):

    def test_dbref(self):
        self.assertRaises(TypeError, DBRef, 5, "test_id")
        self.assertRaises(TypeError, DBRef, "test", "test_id", 5)
        oid = ObjectId()
        ref = DBRef("testc", oid, "testdb")
        self.assertEqual(ref.collection, "testc")
        self.assertEqual(ref.id, oid)
        self.assertEqual(ref.database, "testdb")
        ref = DBRef(u"testcoll", oid)
        self.assertEqual(ref.collection, "testcoll")
        ref_son = SON([("$ref", "testcoll"), ("$id", oid)])
        self.assertEqual(ref.as_doc(), ref_son)
        self.assertEqual(repr(ref), "DBRef(u'testcoll', %r)" % oid)

        ref = DBRef(u"testcoll", oid, u"testdb")
        ref_son = SON([("$ref", "testcoll"), ("$id", oid), ("$db", "testdb")])
        self.assertEqual(ref.as_doc(), ref_son)
        self.assertEqual(repr(ref), "DBRef(u'testcoll', %r, u'testdb')" % oid)

        ref1 = DBRef('a', oid)
        ref2 = DBRef('a', oid)

        self.assertEqual(hash(ref1), hash(ref2))
