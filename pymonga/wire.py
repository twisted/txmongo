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
from pymonga._pymongo import bson
from pymonga.objects import Database
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


class _MongoWire(protocol.Protocol):
    def __init__(self):
        self.__id = 0
        self.__queries = {}
        self.__buffer = ""
        self.__datalen = None
        self.__response = 0
        self.__waiting_header = True

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
        response_flag, cursor_id, start, length = struct.unpack("<iqii", packet[:20])
        if response_flag:
            self._queryFailure(request_id, cursor_id, response_flag, packet[20:])
            return
        self._querySuccess(request_id, cursor_id, bson._to_dicts(packet[20:]))
	    
    def _send(self, operation, collection, message, query_opts=_ZERO):
        #print "sending %d to %s" % (operation, self)
        fullname = collection and bson._make_c_string(collection) or ""
        message = query_opts + fullname + message
        header = struct.pack("<iiii", 16+len(message), self.__id, 0, operation)
        self.transport.write(header+message)
        self.__id += 1

    def _OP_INSERT(self, collection, docs):
        self._send(2002, collection,
            "".join([bson.BSON.from_dict(doc) for doc in docs]))

    def _OP_UPDATE(self, collection, spec, document, upsert=False):
        message = (upsert and _ONE or _ZERO) + \
            bson.BSON.from_dict(spec) + bson.BSON.from_dict(document)
        self._send(2001, collection, message)

    def _OP_DELETE(self, collection, spec):
        self._send(2006, collection, _ZERO + bson.BSON.from_dict(spec))

    def _OP_KILL_CURSORS(self, cursors):
        message = struct.pack("<i", len(cursors))
        for cursor_id in cursors:
            message += struct.pack("<q", cursor_id)
        self._send(2007, None, message)

    def _OP_GET_MORE(self, collection, limit, cursor_id):
        message = struct.pack("<iq", limit, cursor_id)
        self._send(2005, collection, message)

    def _OP_QUERY(self, collection, spec, skip, limit, fields=None):
        message = struct.pack("<ii", skip, limit) + bson.BSON.from_dict(spec)
        if fields:
            message += bson.BSON.from_dict(fields)

        queryObj = _MongoQuery(self.__id, collection, limit)
        self.__queries[self.__id] = queryObj
        self._send(2004, collection, message)
        return queryObj.deferred

    def _queryFailure(self, request_id, cursor_id, response, raw_error):
        queryObj = self.__queries.pop(request_id, None)
        if queryObj:
            queryObj.deferred.errback((response, raw_error))
            del queryObj

    def _querySuccess(self, request_id, cursor_id, documents):
        queryObj = self.__queries.pop(request_id, None)
        if queryObj:
            ndocs = len(documents)
            queryObj.documents += documents

            if cursor_id and ndocs < queryObj.limit:
                queryObj.limit -= ndocs
                queryObj.id = self.__id
                self.__queries[self.__id] = queryObj
                self._OP_GET_MORE(queryObj.collection, 0, cursor_id)
            else:
                if cursor_id:
                    self._OP_KILL_CURSORS([cursor_id])
                queryObj.deferred.callback(queryObj.documents)
                del queryObj


class MongoProtocol(_MongoWire):
    def __init__(self):
        self.__pool = None
        _MongoWire.__init__(self)

    def _set_pool(self, pool, size):
        self.__pool = pool
        self.__poolidx = 0
        self.__poolsize = size

    def _get_conn(self):
        if self.__pool:
            conn = self.__pool[self.__poolidx]
            self.__poolidx = (self.__poolidx + 1) % self.__poolsize
            return conn
        else:
            return self

    def __str__(self):
        addr = self.transport.getHost()
        return "<mongodb Connection: %s:%s>" % (addr.host, addr.port)

    def __getitem__(self, database_name):
        return Database(self, database_name)

    def __getattr__(self, database_name):
        return Database(self, database_name)
