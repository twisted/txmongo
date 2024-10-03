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

import bson
from bson import ObjectId
from twisted.trial import unittest

from txmongo.protocol import (
    OP_MSG_MORE_TO_COME,
    MongoDecoder,
    MongoSenderProtocol,
    Msg,
    Query,
    Reply,
)


class _FakeTransport:
    """Catches all content that MongoClientProtocol wants to send over the wire"""

    def __init__(self):
        self.data = []

    def write(self, data):
        self.data.append(data)

    def get_content(self):
        return b"".join(self.data)


class TestMongoProtocol(unittest.TestCase):
    def _encode_decode(self, request):
        proto = MongoSenderProtocol()
        proto.transport = _FakeTransport()

        proto._send(request)

        decoder = MongoDecoder()
        decoder.feed(proto.transport.get_content())
        return next(decoder)

    def test_EncodeDecodeQuery(self):
        request = Query(
            collection="coll",
            n_to_skip=123,
            n_to_return=456,
            query=bson.encode({"x": 42}),
            fields=bson.encode({"y": 1}),
        )

        decoded = self._encode_decode(request)

        self.assertEqual(decoded.response_to, request.response_to)
        self.assertEqual(decoded.flags, request.flags)
        self.assertEqual(decoded.collection, request.collection)
        self.assertEqual(decoded.n_to_skip, request.n_to_skip)
        self.assertEqual(decoded.n_to_return, request.n_to_return)
        self.assertEqual(decoded.query, request.query)
        self.assertEqual(decoded.fields, request.fields)

    def test_EncodeDecodeReply(self):
        request = Reply(
            response_flags=123,
            cursor_id=456,
            starting_from=789,
            documents=[bson.encode({"a": 1}), bson.encode({"b": 2})],
        )

        decoded = self._encode_decode(request)

        self.assertEqual(decoded.response_to, request.response_to)
        self.assertEqual(decoded.response_flags, request.response_flags)
        self.assertEqual(decoded.cursor_id, request.cursor_id)
        self.assertEqual(decoded.starting_from, request.starting_from)
        self.assertEqual(decoded.documents, request.documents)

    def test_EncodeDecodeMsg(self):
        request = Msg(
            response_to=123,
            flag_bits=OP_MSG_MORE_TO_COME,
            body=bson.encode({"a": 1, "$db": "dbname"}),
            payload={
                "documents": [
                    bson.encode({"a": 1}),
                    bson.encode({"a": 2}),
                ],
                "updates": [
                    bson.encode({"$set": {"z": 1}}),
                    bson.encode({"$set": {"z": 2}}),
                ],
                "deletes": [
                    bson.encode({"_id": ObjectId()}),
                    bson.encode({"_id": ObjectId()}),
                ],
            },
        )

        decoded = self._encode_decode(request)

        self.assertEqual(decoded.response_to, request.response_to)
        self.assertEqual(decoded.opcode(), request.opcode())
        self.assertEqual(decoded.flag_bits, request.flag_bits)
        self.assertEqual(decoded.body, request.body)
        self.assertEqual(decoded.payload, request.payload)
