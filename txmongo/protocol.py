# coding: utf-8
# Copyright 2009 Alexandre Fiori
# Copyright 2012 Christian Hergert
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

"""
Low level connection to Mongo.

This module contains the wire protocol implementation for txmongo.
The various constants from the protocl are available as constants.

This implementation requires pymongo so that as much of the
implementation can be shared. This includes BSON encoding and
decoding as well as Exception types, when applicable.
"""

import bson
from   collections      import namedtuple
from   pymongo          import errors
import struct
from   twisted.internet import defer, protocol
from   twisted.python   import failure, log

INT_MAX = 2147483647

OP_REPLY        = 1
OP_MSG          = 1000
OP_UPDATE       = 2001
OP_INSERT       = 2002
OP_QUERY        = 2004
OP_GETMORE      = 2005
OP_DELETE       = 2006
OP_KILL_CURSORS = 2007

OP_NAMES = {
    OP_REPLY:        'REPLY',
    OP_MSG:          'MSG',
    OP_UPDATE:       'UPDATE',
    OP_INSERT:       'INSERT',
    OP_QUERY:        'QUERY',
    OP_GETMORE:      'GETMORE',
    OP_DELETE:       'DELETE',
    OP_KILL_CURSORS: 'KILL_CURSORS'
}

DELETE_SINGLE_REMOVE = 1 << 0

QUERY_TAILABLE_CURSOR   = 1 << 1
QUERY_SLAVE_OK          = 1 << 2
QUERY_OPLOG_REPLAY      = 1 << 3
QUERY_NO_CURSOR_TIMEOUT = 1 << 4
QUERY_AWAIT_DATA        = 1 << 5
QUERY_EXHAUST           = 1 << 6
QUERY_PARTIAL           = 1 << 7

REPLY_CURSOR_NOT_FOUND   = 1 << 0
REPLY_QUERY_FAILURE      = 1 << 1
REPLY_SHARD_CONFIG_STALE = 1 << 2
REPLY_AWAIT_CAPABLE      = 1 << 3

UPDATE_UPSERT = 1 << 0
UPDATE_MULTI  = 1 << 1

Msg = namedtuple('Msg', ['len', 'request_id', 'response_to', 'opcode', 'message'])
KillCursors = namedtuple('KillCursors', ['len', 'request_id', 'response_to', 'opcode', 'zero', 'n_cursors', 'cursors'])

class Delete(namedtuple('Delete', ['len', 'request_id', 'response_to', 'opcode', 'zero', 'collection', 'flags', 'selector'])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_DELETE,
                zero=0, collection='', flags=0, selector=None):
        return super(Delete, cls).__new__(cls, len, request_id, response_to,
                                           opcode, zero, collection,
                                           flags, selector)

class Getmore(namedtuple('Getmore', ['len', 'request_id', 'response_to',
                                     'opcode', 'zero', 'collection',
                                     'n_to_return', 'cursor_id'])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_GETMORE,
                zero=0, collection='', n_to_return=-1, cursor_id=-1):
        return super(Getmore, cls).__new__(cls, len, request_id, response_to,
                                           opcode, zero, collection,
                                           n_to_return, cursor_id)

class Insert(namedtuple('Insert', ['len', 'request_id', 'response_to',
                                   'opcode', 'flags', 'collection',
                                   'documents'])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_INSERT,
                flags=0, collection='', documents=None):
        return super(Insert, cls).__new__(cls, len, request_id, response_to,
                                          opcode, flags, collection, documents)

class Reply(namedtuple('Reply', ['len', 'request_id', 'response_to', 'opcode',
                                 'response_flags', 'cursor_id',
                                 'starting_from', 'n_returned', 'documents'])):
    def __new__(cls, _len=0, request_id=0, response_to=0, opcode=OP_REPLY,
                response_flags=0, cursor_id=0, starting_from=0,
                n_returned=None, documents=None):
        if documents is None:
            documents = []
        if n_returned is None:
            n_returned = len(documents)
        documents = [b if isinstance(b, bson.BSON) else bson.BSON.encode(b) for b in documents]
        return super(Reply, cls).__new__(cls, _len, request_id, response_to,
                                         opcode, response_flags, cursor_id,
                                         starting_from, n_returned,
                                         documents)

class Query(namedtuple('Query', ['len', 'request_id', 'response_to', 'opcode',
                                 'flags', 'collection', 'n_to_skip',
                                 'n_to_return', 'query', 'fields'])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_QUERY,
                flags=0, collection='', n_to_skip=0, n_to_return=-1,
                query=None, fields=None):
        if query is None:
            query = {}
        if not isinstance(query, bson.BSON):
            query = bson.BSON.encode(query)
        if fields is not None and not isinstance(fields, bson.BSON):
            fields = bson.BSON.encode(fields)
        return super(Query, cls).__new__(cls, len, request_id, response_to,
                                         opcode, flags, collection, n_to_skip,
                                         n_to_return, query, fields)

class Update(namedtuple('Update', ['len', 'request_id', 'response_to',
                                   'opcode', 'zero', 'collection', 'flags',
                                   'selector', 'update'])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_UPDATE,
                zero=0, collection='', flags=0, selector=None, update=None):
        return super(Update, cls).__new__(cls, len, request_id, response_to,
                                          opcode, zero, collection, flags,
                                          selector, update)

class MongoClientProtocol(protocol.Protocol):
    __request_id = 1

    def getrequestid(self):
        return self.__request_id

    def _send(self, iovec):
        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        if self.__request_id >= INT_MAX:
            self.__request_id = 1
        datalen = sum([len(chunk) for chunk in iovec]) + 8
        datareq = struct.pack('<ii', datalen, request_id)
        iovec.insert(0, datareq)
        self.transport.write(''.join(iovec))
        return request_id

    def send(self, request):
        opname = OP_NAMES[request.opcode]
        sender = getattr(self, 'send_%s' % opname, None)
        if callable(sender):
            return sender(request)
        else:
            log.msg("No sender for opcode: %d" % request.opcode)

    def send_REPLY(self, request):
        iovec = [struct.pack('<iiiqii', *request[2:8])]
        iovec.extend(request.documents)
        self._send(iovec)

    def send_MSG(self, request):
        iovec = [struct.pack('<ii', *request[2:4]), request.message, '\x00']
        return self._send(iovec)

    def send_UPDATE(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00',
                 struct.pack('<i', request.flags),
                 request.selector,
                 request.update]
        return self._send(iovec)

    def send_INSERT(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00']
        iovec.extend(request.documents)
        return self._send(iovec)

    def send_QUERY(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00',
                 struct.pack('<ii', request.n_to_skip, request.n_to_return),
                 request.query,
                 (request.fields or '')]
        return self._send(iovec)

    def send_GETMORE(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00',
                 struct.pack('<iq', request.n_to_return, request.cursor_id)]
        return self._send(iovec)

    def send_DELETE(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00',
                 struct.pack('<i', request.flags),
                 request.selector]
        return self._send(iovec)

    def send_KILL_CURSORS(self, request):
        iovec = [struct.pack('<iii', *request[2:5]),
                 request.collection.encode('ascii'), '\x00',
                 struct.pack('<i', len(request.cursors))]
        for cursor in request.cursors:
            iovec.append(struct.pack('<q', cursor))
        return self._send(iovec)

class MongoServerProtocol(protocol.Protocol):
    __decoder = None

    def __init__(self):
        self.__decoder = MongoDecoder()

    def dataReceived(self, data):
        self.__decoder.feed(data)

        try:
            request = self.__decoder.next()
            while request:
                self.handle(request)
                request = self.__decoder.next()
        except Exception, ex:
            self.fail(reason=failure.Failure(ex))

    def handle(self, request):
        opname = OP_NAMES[request.opcode]
        handler = getattr(self, 'handle_%s' % opname, None)
        if callable(handler):
            handler(request)
        else:
            log.msg("No handler found for opcode: %d" % request.opcode)

    def handle_REPLY(self, request):
        pass

    def handle_MSG(self, request):
        pass

    def handle_UPDATE(self, request):
        pass

    def handle_INSERT(self, request):
        pass

    def handle_QUERY(self, request):
        pass

    def handle_GETMORE(self, request):
        pass

    def handle_DELETE(self, request):
        pass

    def handle_KILL_CURSORS(self, request):
        pass

class MongoProtocol(MongoServerProtocol, MongoClientProtocol):
    __connection_ready = None
    __deferreds = None

    def __init__(self):
        MongoServerProtocol.__init__(self)
        self.__connection_ready = []
        self.__deferreds = {}

    def inflight(self):
        return len(self.__deferreds)

    def connectionMade(self):
        deferreds, self.__connection_ready = self.__connection_ready, []
        if deferreds:
            for df in deferreds:
                df.callback(self)

    def connectionLost(self, reason):
        if self.__deferreds:
            deferreds, self.__deferreds = self.__deferreds, {}
            for df in deferreds.itervalues():
                df.errback(reason)
        deferreds, self.__connection_ready = self.__connection_ready, []
        if deferreds:
            for df in deferreds:
                df.errback(reason)
        protocol.Protocol.connectionLost(self, reason)

    def connectionReady(self):
        if self.transport:
            return defer.succeed(None)
        if not self.__connection_ready:
            self.__connection_ready = []
        df = defer.Deferred()
        self.__connection_ready.append(df)
        return df

    def send_GETMORE(self, request):
        request_id = MongoClientProtocol.send_GETMORE(self, request)
        df = defer.Deferred()
        self.__deferreds[request_id] = df
        return df

    def send_QUERY(self, request):
        request_id = MongoClientProtocol.send_QUERY(self, request)
        df = defer.Deferred()
        self.__deferreds[request_id] = df
        return df

    def handle_REPLY(self, request):
        if request.response_to in self.__deferreds:
            df = self.__deferreds.pop(request.response_to)
            if request.response_flags & REPLY_QUERY_FAILURE:
                doc = request.documents[0].decode()
                code = doc.get('code')
                msg = doc.get('$err', 'Unknown error')
                fail_conn = False
                if code == 13435:
                    err = errors.AutoReconnect(msg)
                    fail_conn = True
                else:
                    err = errors.OperationFailure(msg, code)
                df.errback(err)
                if fail_conn:
                    self.transport.loseConnection()
            else:
                df.callback(request)

    def fail(self, reason):
        if not isinstance(reason, failure.Failure):
            reason = failure.Failure(reason)
        log.err(reason)
        self.transport.loseConnection()

    @defer.inlineCallbacks
    def getlasterror(self, db):
        command = {'getlasterror': 1}
        db = '%s.$cmd' % db.split('.', 1)[0]
        uri = self.factory.uri
        if 'w' in uri['options']:
            command['w'] = int(uri['options']['w'])
        if 'wtimeoutms' in uri['options']:
            command['wtimeout'] = int(uri['options']['wtimeoutms'])
        if 'fsync' in uri['options']:
            command['fsync'] = bool(uri['options']['fsync'])
        if 'journal' in uri['options']:
            command['journal'] = bool(uri['options']['journal'])

        query = Query(collection=db, query=command)
        reply = yield self.send_QUERY(query)

        assert len(reply.documents) == 1

        document = reply.documents[0].decode()
        err = document.get('err', None)
        code = document.get('code', None)

        if err is not None:
            if code == 11000:
                raise errors.DuplicateKeyError(err, code=code)
            else:
                raise errors.OperationFailure(err, code=code)

        defer.returnValue(document)

class MongoDecoder:
    dataBuffer = None

    def __init__(self):
        self.dataBuffer = ''

    def feed(self, data):
        self.dataBuffer += data

    def next(self):
        if len(self.dataBuffer) < 16:
            return None
        msglen, = struct.unpack('<i', self.dataBuffer[:4])
        if len(self.dataBuffer) < msglen:
            return None
        if msglen < 16:
            raise errors.ConnectionFailure()
        msgdata = self.dataBuffer[:msglen]
        self.dataBuffer = self.dataBuffer[msglen:]
        return self.decode(msgdata)

    def decode(self, msgdata):
        msglen = len(msgdata)
        header = struct.unpack('<iiii', msgdata[:16])
        opcode = header[3]
        if opcode == OP_UPDATE:
            zero, = struct.unpack('<i', msgdata[16:20])
            if zero != 0:
                raise errors.ConnectionFailure()
            name = msgdata[20:].split('\x00', 1)[0]
            offset = 20 + len(name) + 1
            flags, = struct.unpack('<i', msgdata[offset:offset+4])
            offset += 4
            selectorlen, = struct.unpack('<i', msgdata[offset:offset+4])
            selector = bson.BSON(msgdata[offset:offset+selectorlen])
            offset += selectorlen
            updatelen, = struct.unpack('<i', msgdata[offset:offset+4])
            update = bson.BSON(msgdata[offset:offset+updatelen])
            return Update(*(header + (zero, name, flags, selector, update)))
        elif opcode == OP_INSERT:
            flags, = struct.unpack('<i', msgdata[16:20])
            name = msgdata[20:].split('\x00', 1)[0]
            offset = 20 + len(name) + 1
            docs = []
            while offset < len(msgdata):
                doclen, = struct.unpack('<i', msgdata[offset:offset+4])
                docdata = msgdata[offset:offset+doclen]
                doc = bson.BSON(docdata)
                docs.append(doc)
                offset += doclen
            return Insert(*(header + (flags, name, docs)))
        elif opcode == OP_QUERY:
            flags, = struct.unpack('<i', msgdata[16:20])
            name = msgdata[20:].split('\x00', 1)[0]
            offset = 20 + len(name) + 1
            ntoskip, ntoreturn = struct.unpack('<ii', msgdata[offset:offset+8])
            offset += 8
            querylen, = struct.unpack('<i', msgdata[offset:offset+4])
            querydata = msgdata[offset:offset+querylen]
            query = bson.BSON(querydata)
            offset += querylen
            fields = None
            if msglen > offset:
                fieldslen, = struct.unpack('<i', msgdata[offset:offset+4])
                fields = bson.BSON(msgdata[offset:offset+fieldslen])
            return Query(*(header + (flags, name, ntoskip, ntoreturn, query, fields)))
        elif opcode == OP_GETMORE:
            zero, = struct.unpack('<i', msgdata[16:20])
            if zero != 0:
                raise errors.ConnectionFailure()
            name = msgdata[20:].split('\x00', 1)[0]
            offset = 20 + len(name) + 1
            ntoreturn, cursorid = struct.unpack('<iq', msgdata[offset:offset+12])
            return Getmore(*(header + (zero, name, ntoreturn, cursorid)))
        elif opcode == OP_DELETE:
            zero, = struct.unpack('<i', msgdata[16:20])
            if zero != 0:
                raise errors.ConnectionFailure()
            name = msgdata[20:].split('\x00', 1)[0]
            offset = 20 + len(name) + 1
            flags, = struct.unpack('<i', msgdata[offset:offset+4])
            offset += 4
            selector = bson.BSON(msgdata[offset:])
            return Delete(*(header + (zero, name, flags, selector)))
        elif opcode == OP_KILL_CURSORS:
            cursors = struct.unpack('<ii', msgdata[16:24])
            if cursors[0] != 0:
                raise errors.ConnectionFailure()
            offset = 24
            cursor_list = []
            for i in xrange(cursors[1]):
                cursor, = struct.unpack('<q', msgdata[offset:offset+8])
                cursor_list.append(cursor)
                offset += 8
            return KillCursors(*(header + cursors + (cursor_list,)))
        elif opcode == OP_MSG:
            if msgdata[-1] != '\x00':
                raise errors.ConnectionFailure()
            return Msg(*(header + (msgdata[16:-1].decode('ascii'),)))
        elif opcode == OP_REPLY:
            reply = struct.unpack('<iqii', msgdata[16:36])
            docs = []
            offset = 36
            for i in xrange(reply[3]):
                doclen, = struct.unpack('<i', msgdata[offset:offset+4])
                if doclen > (msglen - offset):
                    raise errors.ConnectionFailure()
                docdata = msgdata[offset:offset+doclen]
                doc = bson.BSON(docdata)
                docs.append(doc)
                offset += doclen
            return Reply(*(header + reply + (docs,)))
        else:
            raise errors.ConnectionFailure()
        return header
