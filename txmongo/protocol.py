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

import struct
from txmongo._pymongo import bson
from txmongo.database import Database
from twisted.internet import defer, protocol

_ONE = "\x01\x00\x00\x00"
_ZERO = "\x00\x00\x00\x00"

"""Low level connection to Mongo."""


class _MongoQuery(object):
    def __init__(self, id, collection, limit):
        self.id = id
        self.limit = limit
        self.collection = collection
        self.documents = []
        self.deferred = defer.Deferred()


class MongoProtocol(protocol.Protocol):
    def __init__(self):
        self.__id = 0
        self.__queries = {}
        self.__buffer = ""
        self.__datalen = None
        self.__response = 0
        self.__waiting_header = True

    def connectionMade(self):
        self.factory.append(self)

    def connectionLost(self, reason):
        self.connected = 0
        self.factory.remove(self)
        protocol.Protocol.connectionLost(self, reason)

    def dataReceived(self, data):
        while self.__waiting_header:
            self.__buffer += data
            if len(self.__buffer) < 16:
                break

            # got full header, 16 bytes (or more)
            header, extra = self.__buffer[:16], self.__buffer[16:]
            self.__buffer = ""
            self.__waiting_header = False
            datalen, request, response, operation = struct.unpack("<iiii", header)
            self.__datalen = datalen - 16
            self.__response = response
            if extra:
                self.dataReceived(extra)
            break
        else:
            if self.__datalen is not None:
                data, extra = data[:self.__datalen], data[self.__datalen:]
                self.__datalen -= len(data)
            else:
                extra = ""

            self.__buffer += data
            if self.__datalen == 0:
                self.messageReceived(self.__response, self.__buffer)
                self.__datalen = None
                self.__waiting_header = True
                self.__buffer = ""
                if extra:
                    self.dataReceived(extra)

    def messageReceived(self, request_id, packet):
        # Response Flags:
        #   bit 0:    Cursor Not Found
        #   bit 1:    Query Failure
        #   bit 2:    Shard Config Stale
        #   bit 3:    Await Capable
        #   bit 4-31: Reserved
        QUERY_FAILURE = 1 << 1
        response_flag, cursor_id, start, length = struct.unpack("<iqii", packet[:20])
        if response_flag == QUERY_FAILURE:
            self.queryFailure(request_id, cursor_id, response_flag, bson._to_dicts(packet[20:]))
            return
        self.querySuccess(request_id, cursor_id, bson._to_dicts(packet[20:]))

    def sendMessage(self, operation, collection, message, query_opts=_ZERO):
        #print "sending %d to %s" % (operation, self)
        fullname = collection and bson._make_c_string(collection) or ""
        message = query_opts + fullname + message
        header = struct.pack("<iiii", 16 + len(message), self.__id, 0, operation)
        self.transport.write(header + message)
        self.__id += 1

    def OP_INSERT(self, collection, docs):
        docs = [bson.BSON.from_dict(doc) for doc in docs]
        self.sendMessage(2002, collection, "".join(docs))

    def OP_UPDATE(self, collection, spec, document, upsert=False, multi=False):
        options = 0
        if upsert:
            options += 1
        if multi:
            options += 2

        message = struct.pack("<i", options) + \
            bson.BSON.from_dict(spec) + bson.BSON.from_dict(document)
        self.sendMessage(2001, collection, message)

    def OP_DELETE(self, collection, spec):
        self.sendMessage(2006, collection, _ZERO + bson.BSON.from_dict(spec))

    def OP_KILL_CURSORS(self, cursors):
        message = struct.pack("<i", len(cursors))
        for cursor_id in cursors:
            message += struct.pack("<q", cursor_id)
        self.sendMessage(2007, None, message)

    def OP_GET_MORE(self, collection, limit, cursor_id):
        message = struct.pack("<iq", limit, cursor_id)
        self.sendMessage(2005, collection, message)

    def OP_QUERY(self, collection, spec, skip, limit, fields=None):
        message = struct.pack("<ii", skip, limit) + bson.BSON.from_dict(spec)
        if fields:
            message += bson.BSON.from_dict(fields)

        queryObj = _MongoQuery(self.__id, collection, limit)
        self.__queries[self.__id] = queryObj
        self.sendMessage(2004, collection, message)
        return queryObj.deferred

    def queryFailure(self, request_id, cursor_id, response, raw_error):
        queryObj = self.__queries.pop(request_id, None)
        if queryObj:
            queryObj.deferred.errback(ValueError("mongo error=%s" % repr(raw_error)))
            del(queryObj)

    def querySuccess(self, request_id, cursor_id, documents):
        try:
            queryObj = self.__queries.pop(request_id)
        except KeyError:
            return
        queryObj.documents += documents
        if cursor_id:
            queryObj.id = self.__id
            next_batch = 0
            if queryObj.limit:
                next_batch = queryObj.limit - len(queryObj.documents)
                # Assert, because according to the protocol spec and my observations
                # there should be no problems with this, but who knows? At least it will
                # be noticed, if something unexpected happens. And it is definitely
                # better, than silently returning a wrong number of documents
                assert next_batch >= 0, "Unexpected number of documents received!"
                if not next_batch:
                    self.OP_KILL_CURSORS([cursor_id])
                    queryObj.deferred.callback(queryObj.documents)
                    return
            self.__queries[self.__id] = queryObj
            self.OP_GET_MORE(queryObj.collection, next_batch, cursor_id)
        else:
            queryObj.deferred.callback(queryObj.documents)
