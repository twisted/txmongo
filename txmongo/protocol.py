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
from   collections      import namedtuple, OrderedDict
import pymongo
from   pymongo          import errors
import random
import struct
from   twisted.internet import defer, protocol
from   twisted.python   import failure, log
import types


#
# Constants
#

OP_REPLY        = 1
OP_MSG          = 1000
OP_UPDATE       = 2001
OP_INSERT       = 2002
OP_QUERY        = 2004
OP_GETMORE      = 2005
OP_DELETE       = 2006
OP_KILL_CURSORS = 2007


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


#
# Structures
#

MongoHeaderFields = ['message_length',
                     'request_id',
                     'response_to',
                     'op_code']
MongoHeader = namedtuple('MongoHeader', MongoHeaderFields)

MongoReplyFields = ['header',
                    'response_flags',
                    'cursor',
                    'starting_offset',
                    'count',
                    'documents']
MongoReply = namedtuple('MongoReply', MongoReplyFields)


#
# Errors
#

class MongoProtocolError(Exception):
    message = 'The communication contract was broken.'

class InsufficientData(Exception):
    pass


#
# Classes
#

class MongoProtocol(protocol.Protocol):
    __buffer = ''
    __connection_ready = None
    __deferreds = None
    __request_id = None
    as_class = dict
    max_bson_size = 16777216

    def __init__(self):
        self.__buffer = ''
        self.__deferreds = {}
        self.__request_id = random.randint(1, 65535)
        self.__connection_ready = []

    def connectionMade(self):
        # Log our connection status.
        addr = '%s:%s' % self.transport.addr
        log.msg('Connected to %s.' % addr, system='txmongo')
        deferreds, self.__connection_ready = self.__connection_ready, []
        if deferreds:
            for df in deferreds:
                df.callback(self)

    def connectionLost(self, reason):
        if self.__deferreds:
            deferreds, self.__deferreds = self.__deferreds, {}
            for df in deferreds:
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

    def OP_QUERY(self,
                 db_and_collection,
                 flags,
                 skip,
                 limit,
                 query,
                 field_selector):
        """
        Perform a Mongo query against @db_and_collection.

        Parameters:
            @db_and_collection -- The db and collection name "db.collection".
            @flags             -- Mongo query flags.
            @skip              -- Number of documents to skip.
            @field_selector    -- What fields to select.
        """
        #print 'OP_QUERY', self

        assert isinstance(db_and_collection, basestring)
        assert isinstance(flags, int)
        assert isinstance(skip, int)
        assert isinstance(limit, int)

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        if query is None:
            query = {}
        if not isinstance(query, bson.BSON):
            query = bson.BSON.encode(query)

        data = struct.pack('<iiii', request_id, 0, OP_QUERY, flags)
        data += db_and_collection.encode('ascii') + '\0'
        data += struct.pack('<ii', skip, limit)
        data += query
        if field_selector is not None:
            if not isinstance(field_selector, bson.BSON):
                field_selector = bson.BSON.encode(field_selector)
            if len(field_selector) > self.max_bson_size:
                raise pymongo.connection.ConfigurationError('BSON document size is too large')
            data += field_selector
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.__deferreds[request_id] = df = defer.Deferred()
        self.transport.write(data)

        return df

    def OP_UPDATE(self,
                  db_and_collection,
                  flags,
                  selector,
                  update):
        """
        Perform a Mongo update against @db_and_collection.

        Parameters:
            @db_and_collection -- The db and collection name "db.collection".
            @flags             -- Mongo update flags.
            @selector          -- The selector to query.
            @update            -- The update to perform.
        """
        #print 'OP_UPDATE', self

        assert isinstance(db_and_collection, basestring)
        assert isinstance(flags, int)
        assert update

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        if selector is None:
            selector = {}
        if not isinstance(selector, bson.BSON):
            selector = bson.BSON.encode(selector)
        if not isinstance(update, bson.BSON):
            update = bson.BSON.encode(update)
        data = struct.pack('<iiii', request_id, 0, OP_UPDATE, 0)
        data += db_and_collection.encode('ascii') + '\0'
        data += struct.pack('<i', flags)
        if len(selector) > self.max_bson_size:
            raise pymongo.connection.ConfigurationError('BSON document size is too large')
        data += selector
        if len(update) > self.max_bson_size:
            raise pymongo.connection.ConfigurationError('BSON document size is too large')
        data += update
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.transport.write(data)

    def OP_INSERT(self,
                  db_and_collection,
                  flags,
                  documents):
        """
        Performs a Mongo insertion against @db_and_collection.

        Parameters:
            @db_and_collection -- The db and collection name "db.collection".
            @flags             -- Mongo insertion flags.
            @documents         -- A document or list of documents to insert.
        """
        #print 'OP_INSERT', self

        assert isinstance(db_and_collection, basestring)
        assert isinstance(flags, int)
        assert documents

        if not isinstance(documents, list):
            documents = [documents]

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        data = struct.pack('<iiii', request_id, 0, OP_INSERT, flags)
        data += db_and_collection.encode('ascii') + '\0'
        for document in documents:
            if not isinstance(document, bson.BSON):
                document = bson.BSON.encode(document)
            if len(document) > self.max_bson_size:
                raise pymongo.connection.ConfigurationError('BSON document size is too large')
            data += document
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.transport.write(data)

    def OP_GETMORE(self,
                   db_and_collection,
                   limit,
                   cursor_id):
        """
        Performs a Mongo getmore operation against @db_and_collection.

        Parameters:
            @db_and_collection -- The db and collection name "db.collection".
            @limit             -- The maximum number of documents.
            @cursor_id         -- The Mongo cursor we are iterating.
        """
        #print 'OP_GETMORE', self

        assert isinstance(db_and_collection, basestring)
        assert isinstance(cursor_id, (int, long))

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        data = struct.pack('<iiii', request_id, 0, OP_GETMORE, 0)
        data += db_and_collection.encode('ascii') + '\0'
        data += struct.pack('<iq', limit, cursor_id)
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.__deferreds[request_id] = df = defer.Deferred()
        self.transport.write(data)

        return df

    def OP_DELETE(self,
                  db_and_collection,
                  flags,
                  selector):
        """
        Performs a Mongo delete operation against @db_and_collection.

        Parameters:
            @db_and_collection -- The db and collection name "db.collection".
            @flags             -- Mongo delete flags.
            @selector          -- Selector for matching documents.
        """
        #print 'OP_DELETE', self

        assert isinstance(db_and_collection, basestring)
        assert isinstance(flags, int)
        assert isinstance(selector, (bson.ObjectId, bson.BSON, types.DictType))

        if not isinstance(selector, bson.BSON):
            selector = bson.BSON.encode(selector)

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        data = struct.pack('<iiii', request_id, 0, OP_DELETE, 0)
        data += db_and_collection.encode('ascii') + '\0'
        data += struct.pack('<i', flags)
        if len(selector) > self.max_bson_size:
            raise pymongo.connection.ConfigurationError('BSON document size is too large')
        data += selector
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.transport.write(data)

    def OP_KILL_CURSORS(self, cursors):
        """
        Performs a Mongo killcursors operation.

        Parameters:
            cursors -- A list of cursor_ids to kill.
        """
        #print 'OP_KILL_CURSORS', self

        assert isinstance(cursors, (list, int, long))

        if not isinstance(cursors, list):
            cursors = [cursors]

        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        data = struct.pack('<iiii', request_id, 0, OP_KILL_CURSORS, 0)
        data += struct.pack('<i', len(cursors))
        data += struct.pack(('<%dq' % len(cursors)), *cursors)
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.transport.write(data)

    def OP_MSG(self, message):
        """
        Performs a Mongo msg operation.

        Parameters:
            @msg -- The message to deliver.
        """
        #print 'OP_MSG', self

        assert isinstance(message, basestring)
        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        data = struct.pack('<iii', request_id, 0, OP_MSG)
        data += message.encode('ascii') + '\0'
        data_len = len(data) + 4
        data = struct.pack('<i', data_len) + data
        self.transport.write(data)

    def dataReceived(self, data):
        """
        Handle incoming bytes from the Mongo server. Dispatch any pending
        responses.
        """
        #print 'Got', data.encode('hex')
        self.__buffer += data
        self._process()

    pcount = 0

    def _process(self):
        """
        Process the current receive buffer and dispatch any deferreds.
        """
        try:
            #print 'XXXXXXXXX process'
            self.pcount += 1
            #print '>>>> %d <<<< _process(%r)' % (self.pcount, self.__buffer)
            while self.__buffer:
                try:
                    reply = self._readNextOp()
                    if reply.header.response_to in self.__deferreds:
                        if reply.header.response_to not in self.__deferreds:
                            f = MongoProtocolError('Mongo replied with invalid '
                                                   'response_to set.')
                            self.fail(f)
                            return
                        df = self.__deferreds.pop(reply.header.response_to)
                        if reply.response_flags & REPLY_QUERY_FAILURE:
                            doc = reply.documents[0]
                            code = doc.get('code')
                            msg = doc.get('$err', 'Unknown error')
                            err = errors.OperationFailure(msg, code)
                            df.errback(err)
                        else:
                            df.callback(reply)
                except MongoProtocolError, ex:
                    #print 'Protocol Error', repr(ex)
                    self.fail(ex)
                    return
                except InsufficientData, ex:
                    #print 'Insufficient data', repr(ex)
                    return
            #print 'Done processing ======', self.pcount
        except Exception, ex:
            print 'caught exception !!!!!!!!!!!!!!!!!!!!', repr(ex)

    def _readNextOp(self):
        data = self.__buffer
        if len(data) < 16:
            raise InsufficientData()
        header = MongoHeader(*struct.unpack('<iiii', data[:16]))
        if header.message_length > len(data):
            raise InsufficientData()
        data = data[16:]
        if header.op_code != OP_REPLY:
            raise MongoProtocolError('Unknown message type %s' % header.op_code)
        reply_fields = struct.unpack('<iqii', data[:20])
        data = data[20:]
        docs = []
        for i in xrange(reply_fields[3]):
            if len(data) < 4:
                raise InsufficientData()
            doc_len, = struct.unpack('<i', data[:4])
            if len(data) < doc_len:
                raise InsufficientData()
            doc = bson.BSON(data[:doc_len]).decode(as_class=self.as_class)
            docs.append(doc)
            data = data[doc_len:]
        self.__buffer = data
        if len(docs) != reply_fields[3]:
            raise MongoProtocolError('document count did not match number '
                                     'of received documents.')
        return MongoReply(header,
                          *reply_fields,
                          documents=docs)

    def fail(self, reason):
        #print 'THIS SHIT IS GOING DOWN'
        if not isinstance(reason, failure.Failure):
            reason = failure.Failure(reason)
        log.err(reason)
        self.transport.loseConnection()

    @defer.inlineCallbacks
    def getlasterror(self, db):
        command = {'getlasterror': 1}
        db = '%s.$cmd' % db.split('.', 1)[0]
        #print 'getlasterror', db
        uri = self.factory.uri
        if 'w' in uri['options']:
            command['w'] = int(uri['options']['w'])
        if 'wtimeoutms' in uri['options']:
            command['wtimeout'] = int(uri['options']['wtimeoutms'])
        if 'fsync' in uri['options']:
            command['fsync'] = bool(uri['options']['fsync'])
        if 'journal' in uri['options']:
            command['journal'] = bool(uri['options']['journal'])

        #print command
        #print 'OP_QUERY()', db, 0, 0, 1, command, None
        try:
            df = self.OP_QUERY(db, 0, 0, 1, command, None)
            #print df
            #print self.__deferreds
            reply = yield df
            #print 'got reply', reply
        except Exception, ex:
            #print 'EXCEPTION!!!', repr(ex)
            raise ex

        #print reply.count
        #print reply.documents

        try:
            assert reply.count == 1
            assert len(reply.documents) == 1
        except Exception, ex:
            #print 'HOLY FUCK WTF'
            raise ex

        document = reply.documents[0]
        err = document.get('err', None)
        code = document.get('code', None)

        if err is not None:
            if code == 11000:
                raise errors.DuplicateKeyError(err, code=code)
            else:
                raise errors.OperationFailure(err, code=code)

        defer.returnValue(document)
