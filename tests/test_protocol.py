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

from bson import BSON
from twisted.trial import unittest
from twisted.python.compat import unicode

from txmongo.protocol import MongoClientProtocol, MongoDecoder, Insert, Query, \
    KillCursors, Getmore, Update, Delete, UPDATE_MULTI, UPDATE_UPSERT, \
    DELETE_SINGLE_REMOVE


class _FakeTransport(object):
    """Catches all content that MongoClientProtocol wants to send over the wire"""

    def __init__(self):
        self.data = []

    def write(self, data):
        self.data.append(data)

    def get_content(self):
        return b''.join(self.data)


class TestMongoProtocol(unittest.TestCase):

    def __test_encode_decode(self, request):
        proto = MongoClientProtocol()
        proto.transport = _FakeTransport()

        proto.send(request)

        decoder = MongoDecoder()
        decoder.feed(proto.transport.get_content())
        decoded = next(decoder)

        for field, dec_value, req_value in zip(request._fields, decoded, request):
            # len and request_id are not filled in request object
            if field not in ("len", "request_id"):
                if isinstance(dec_value, bytes) and \
                   isinstance(req_value, unicode):
                    dec_value = dec_value.decode()

                self.assertEqual(dec_value, req_value)

    def test_EncodeDecodeQuery(self):
        request = Query(collection="coll", n_to_skip=123, n_to_return=456,
                        query=BSON.encode({'x': 42}),
                        fields=BSON.encode({'y': 1}))
        self.__test_encode_decode(request)

    def test_EncodeDecodeKillCursors(self):
        request = KillCursors(cursors=[0x12345678, 0x87654321])
        self.__test_encode_decode(request)

    def test_EncodeDecodeGetmore(self):
        request = Getmore(collection="coll", cursor_id=0x12345678, n_to_return=5)
        self.__test_encode_decode(request)

    def test_EncodeDecodeInsert(self):
        request = Insert(collection="coll", documents=[BSON.encode({'x': 42})])
        self.__test_encode_decode(request)

    def test_EncodeDecodeUpdate(self):
        request = Update(flags=UPDATE_MULTI | UPDATE_UPSERT, collection="coll",
                         selector=BSON.encode({'x': 42}),
                         update=BSON.encode({"$set": {'y': 123}}))
        self.__test_encode_decode(request)

    def test_EncodeDecodeDelete(self):
        request = Delete(flags=DELETE_SINGLE_REMOVE, collection="coll",
                         selector=BSON.encode({'x': 42}))
        self.__test_encode_decode(request)
