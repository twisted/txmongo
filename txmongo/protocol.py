# Copyright 2009-2014 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

"""
Low level connection to Mongo.

This module contains the wire protocol implementation for txmongo.
The various constants from the protocol are available as constants.

This implementation requires pymongo so that as much of the
implementation can be shared. This includes BSON encoding and
decoding as well as Exception types, when applicable.
"""

from collections import namedtuple
import struct

import base64
import hmac
from hashlib import sha1
from random import SystemRandom
from bson import BSON, SON, Binary
from pymongo import auth
from pymongo.errors import AutoReconnect, ConnectionFailure, DuplicateKeyError, OperationFailure
from twisted.internet import defer, protocol, error
from twisted.python import failure, log


INT_MAX = 2147483647

OP_REPLY = 1
OP_MSG = 1000
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GETMORE = 2005
OP_DELETE = 2006
OP_KILL_CURSORS = 2007

OP_NAMES = {
    OP_REPLY: "REPLY",
    OP_MSG: "MSG",
    OP_UPDATE: "UPDATE",
    OP_INSERT: "INSERT",
    OP_QUERY: "QUERY",
    OP_GETMORE: "GETMORE",
    OP_DELETE: "DELETE",
    OP_KILL_CURSORS: "KILL_CURSORS"
}

DELETE_SINGLE_REMOVE = 1 << 0

QUERY_TAILABLE_CURSOR = 1 << 1
QUERY_SLAVE_OK = 1 << 2
QUERY_OPLOG_REPLAY = 1 << 3
QUERY_NO_CURSOR_TIMEOUT = 1 << 4
QUERY_AWAIT_DATA = 1 << 5
QUERY_EXHAUST = 1 << 6
QUERY_PARTIAL = 1 << 7

REPLY_CURSOR_NOT_FOUND = 1 << 0
REPLY_QUERY_FAILURE = 1 << 1
REPLY_SHARD_CONFIG_STALE = 1 << 2
REPLY_AWAIT_CAPABLE = 1 << 3

UPDATE_UPSERT = 1 << 0
UPDATE_MULTI = 1 << 1

Msg = namedtuple("Msg", ["len", "request_id", "response_to", "opcode", "message"])

class KillCursors(namedtuple("KillCursors", ["len", "request_id", "response_to", "opcode",
                                             "zero", "n_cursors", "cursors"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_KILL_CURSORS,
                zero=0, n_cursors=0, cursors=None):
        n_cursors = __builtins__["len"](cursors)
        return super(KillCursors, cls).__new__(cls, len, request_id, response_to,
                                               opcode, zero, n_cursors, cursors)


class Delete(namedtuple("Delete",
                        ["len", "request_id", "response_to", "opcode", "zero", "collection",
                         "flags", "selector"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_DELETE,
                zero=0, collection='', flags=0, selector=None):
        return super(Delete, cls).__new__(cls, len, request_id, response_to,
                                          opcode, zero, collection,
                                          flags, selector)


class Getmore(namedtuple("Getmore", ["len", "request_id", "response_to",
                                     "opcode", "zero", "collection",
                                     "n_to_return", "cursor_id"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_GETMORE,
                zero=0, collection='', n_to_return=-1, cursor_id=-1):
        return super(Getmore, cls).__new__(cls, len, request_id, response_to,
                                           opcode, zero, collection,
                                           n_to_return, cursor_id)


class Insert(namedtuple("Insert", ["len", "request_id", "response_to",
                                   "opcode", "flags", "collection",
                                   "documents"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_INSERT,
                flags=0, collection='', documents=None):
        return super(Insert, cls).__new__(cls, len, request_id, response_to,
                                          opcode, flags, collection, documents)


class Reply(namedtuple("Reply", ["len", "request_id", "response_to", "opcode",
                                 "response_flags", "cursor_id",
                                 "starting_from", "n_returned", "documents"])):
    def __new__(cls, _len=0, request_id=0, response_to=0, opcode=OP_REPLY,
                response_flags=0, cursor_id=0, starting_from=0,
                n_returned=None, documents=None):
        if documents is None:
            documents = []
        if n_returned is None:
            n_returned = len(documents)
        documents = [b if isinstance(b, BSON) else BSON.encode(b) for b in documents]
        return super(Reply, cls).__new__(cls, _len, request_id, response_to,
                                         opcode, response_flags, cursor_id,
                                         starting_from, n_returned,
                                         documents)


class Query(namedtuple("Query", ["len", "request_id", "response_to", "opcode",
                                 "flags", "collection", "n_to_skip",
                                 "n_to_return", "query", "fields"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_QUERY,
                flags=0, collection='', n_to_skip=0, n_to_return=-1,
                query=None, fields=None):
        if query is None:
            query = {}
        if not isinstance(query, BSON):
            query = BSON.encode(query)
        if fields is not None and not isinstance(fields, BSON):
            fields = BSON.encode(fields)
        return super(Query, cls).__new__(cls, len, request_id, response_to,
                                         opcode, flags, collection, n_to_skip,
                                         n_to_return, query, fields)


class Update(namedtuple("Update", ["len", "request_id", "response_to",
                                   "opcode", "zero", "collection", "flags",
                                   "selector", "update"])):
    def __new__(cls, len=0, request_id=0, response_to=0, opcode=OP_UPDATE,
                zero=0, collection='', flags=0, selector=None, update=None):
        return super(Update, cls).__new__(cls, len, request_id, response_to,
                                          opcode, zero, collection, flags,
                                          selector, update)


class MongoClientProtocol(protocol.Protocol):
    __request_id = 1

    def get_request_id(self):
        return self.__request_id

    def _send(self, io_vector):
        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        if self.__request_id >= INT_MAX:
            self.__request_id = 1
        data_length = sum([len(chunk) for chunk in io_vector]) + 8
        data_req = struct.pack("<ii", data_length, request_id)
        io_vector.insert(0, data_req)
        self.transport.write(''.join(io_vector))
        return request_id

    def send(self, request):
        opname = OP_NAMES[request.opcode]
        sender = getattr(self, "send_%s" % opname, None)
        if callable(sender):
            return sender(request)
        else:
            log.msg("No sender for opcode: %d" % request.opcode)

    def send_REPLY(self, request):
        iovec = [struct.pack("<iiiqii", *request[2:8])]
        iovec.extend(request.documents)
        self._send(iovec)

    def send_MSG(self, request):
        iovec = [struct.pack("<ii", *request[2:4]), request.message, '\x00']
        return self._send(iovec)

    def send_UPDATE(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 request.collection.encode("ascii"), '\x00',
                 struct.pack("<i", request.flags),
                 request.selector,
                 request.update]
        return self._send(iovec)

    def send_INSERT(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 request.collection.encode("ascii"), '\x00']
        iovec.extend(request.documents)
        return self._send(iovec)

    def send_QUERY(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 request.collection.encode("ascii"), '\x00',
                 struct.pack("<ii", request.n_to_skip, request.n_to_return),
                 request.query,
                 (request.fields or '')]
        return self._send(iovec)

    def send_GETMORE(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 request.collection.encode("ascii"), '\x00',
                 struct.pack("<iq", request.n_to_return, request.cursor_id)]
        return self._send(iovec)

    def send_DELETE(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 request.collection.encode("ascii"), '\x00',
                 struct.pack("<i", request.flags),
                 request.selector]
        return self._send(iovec)

    def send_KILL_CURSORS(self, request):
        iovec = [struct.pack("<iii", *request[2:5]),
                 struct.pack("<i", len(request.cursors))]

        for cursor in request.cursors:
            iovec.append(struct.pack("<q", cursor))
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
        handler = getattr(self, "handle_%s" % opname, None)
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

connectionDone = failure.Failure(error.ConnectionDone())
connectionDone.cleanFailure()


class MongoAuthenticationError(Exception): pass


class MongoProtocol(MongoServerProtocol, MongoClientProtocol):
    __connection_ready = None
    __deferreds = None

    min_wire_version = None
    max_wire_version = None

    def __init__(self):
        MongoServerProtocol.__init__(self)
        self.__connection_ready = []
        self.__deferreds = {}

        self.__auth_lock = defer.DeferredLock()

    def inflight(self):
        return len(self.__deferreds)

    def connectionMade(self):
        deferreds, self.__connection_ready = self.__connection_ready, []
        if deferreds:
            for df in deferreds:
                df.callback(self)

    def connectionLost(self, reason=connectionDone):
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
                code = doc.get("code")
                msg = doc.get("$err", "Unknown error")
                fail_conn = False
                if code == 13435:
                    err = AutoReconnect(msg)
                    fail_conn = True
                else:
                    err = OperationFailure(msg, code)
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
    def get_last_error(self, db):
        command = {"getlasterror": 1}
        db = "%s.$cmd" % db.split('.', 1)[0]
        uri = self.factory.uri
        if 'w' in uri["options"]:
            command['w'] = int(uri["options"]['w'])
        if "wtimeoutms" in uri["options"]:
            command["wtimeout"] = int(uri["options"]["wtimeoutms"])
        if "fsync" in uri["options"]:
            command["fsync"] = bool(uri["options"]["fsync"])
        if "journal" in uri["options"]:
            command["journal"] = bool(uri["options"]["journal"])

        query = Query(collection=db, query=command)
        reply = yield self.send_QUERY(query)

        assert len(reply.documents) == 1

        document = reply.documents[0].decode()
        err = document.get("err", None)
        code = document.get("code", None)

        if err is not None:
            if code == 11000:
                raise DuplicateKeyError(err, code=code)
            else:
                raise OperationFailure(err, code=code)

        defer.returnValue(document)

    def set_wire_versions(self, min_wire_version, max_wire_version):
        self.min_wire_version = min_wire_version
        self.max_wire_version = max_wire_version

    @defer.inlineCallbacks
    def __run_command(self, database, query):
        cmd_collection = str(database) + ".$cmd"
        response = yield self.send_QUERY(Query(collection=cmd_collection, query=query))
        defer.returnValue(response.documents[0].decode())

    @defer.inlineCallbacks
    def authenticate_mongo_cr(self, database_name, username, password):
        result = yield self.__run_command(database_name, {"getnonce": 1})

        if not result["ok"]:
            raise MongoAuthenticationError(result["errmsg"])

        nonce = result["nonce"]

        auth_cmd = SON(authenticate=1)
        auth_cmd["user"] = unicode(username)
        auth_cmd["nonce"] = nonce
        auth_cmd["key"] = auth._auth_key(nonce, username, password)

        result = yield self.__run_command(database_name, auth_cmd)

        if not result["ok"]:
            raise MongoAuthenticationError(result["errmsg"])

        defer.returnValue(result)

    @defer.inlineCallbacks
    def authenticate_scram_sha1(self, database_name, username, password):
        # Totally stolen from pymongo.auth

        user = username.encode("utf-8").replace('=', "=3D").replace(',', "=2C")
        nonce = base64.standard_b64encode(str(SystemRandom().random())[2:].encode("utf-8"))
        first_bare = "n={0},r={1}".format(user, nonce)

        cmd = SON([("saslStart", 1),
                        ("mechanism", "SCRAM-SHA-1"),
                        ("autoAuthorize", 1),
                        ("payload", Binary("n,," + first_bare))])
        result = yield self.__run_command(database_name, cmd)

        server_first = result["payload"]
        parsed = auth._parse_scram_response(server_first)
        iterations = int(parsed['i'])
        salt = parsed['s']
        rnonce = parsed['r']
        if not rnonce.startswith(nonce):
            raise MongoAuthenticationError("Server returned an invalid nonce.")

        without_proof = "c=biws,r=" + rnonce
        salted_pass = auth._hi(auth._password_digest(username, password).encode("utf-8"),
                               base64.standard_b64decode(salt),
                               iterations)
        client_key = hmac.HMAC(salted_pass, "Client Key", sha1).digest()
        stored_key = sha1(client_key).digest()
        auth_msg = ','.join((first_bare, server_first, without_proof))
        client_sig = hmac.HMAC(stored_key, auth_msg, sha1).digest()
        client_proof = "p=" + base64.standard_b64encode(auth._xor(client_key, client_sig))
        client_final = ','.join((without_proof, client_proof))

        server_key = hmac.HMAC(salted_pass, "Server Key", sha1).digest()
        server_sig = base64.standard_b64encode(
            hmac.HMAC(server_key, auth_msg, sha1).digest())

        cmd = SON([("saslContinue", 1),
                        ("conversationId", result["conversationId"]),
                        ("payload", Binary(client_final))])
        result = yield self.__run_command(database_name, cmd)

        if not result["ok"]:
            raise MongoAuthenticationError("Authentication failed")

        parsed = auth._parse_scram_response(result["payload"])
        if parsed['v'] != server_sig:
            raise MongoAuthenticationError("Server returned an invalid signature.")

        # Depending on how it's configured, Cyrus SASL (which the server uses)
        # requires a third empty challenge.
        if not result["done"]:
            cmd = SON([("saslContinue", 1),
                            ("conversationId", result["conversationId"]),
                            ("payload", Binary(''))])
            result = yield self.__run_command(database_name, cmd)
            if not result["done"]:
                raise MongoAuthenticationError("SASL conversation failed to complete.")


    @defer.inlineCallbacks
    def authenticate(self, database_name, username, password, mechanism):
        database_name = str(database_name)
        username = unicode(username)
        password = unicode(password)

        yield self.__auth_lock.acquire()

        try:
            if mechanism == "MONGODB-CR":
                auth_func = self.authenticate_mongo_cr
            elif mechanism == "SCRAM-SHA-1":
                auth_func = self.authenticate_scram_sha1
            elif mechanism == "DEFAULT":
                if self.max_wire_version >= 3:
                    auth_func = self.authenticate_scram_sha1
                else:
                    auth_func = self.authenticate_mongo_cr
            else:
                raise MongoAuthenticationError(
                    "Unknown authentication mechanism: {0}".format(mechanism))

            result = yield auth_func(database_name, username, password)
            defer.returnValue(result)
        finally:
            self.__auth_lock.release()


class MongoDecoder:
    dataBuffer = None

    def __init__(self):
        self.dataBuffer = ''

    def feed(self, data):
        self.dataBuffer += data

    def next(self):
        if len(self.dataBuffer) < 16:
            return None
        message_length, = struct.unpack("<i", self.dataBuffer[:4])
        if len(self.dataBuffer) < message_length:
            return None
        if message_length < 16:
            raise ConnectionFailure()
        message_data = self.dataBuffer[:message_length]
        self.dataBuffer = self.dataBuffer[message_length:]
        return self.decode(message_data)

    @staticmethod
    def decode(message_data):
        message_length = len(message_data)
        header = struct.unpack("<iiii", message_data[:16])
        opcode = header[3]
        if opcode == OP_UPDATE:
            zero, = struct.unpack("<i", message_data[16:20])
            if zero != 0:
                raise ConnectionFailure()
            name = message_data[20:].split("\x00", 1)[0]
            offset = 20 + len(name) + 1
            flags, = struct.unpack("<i", message_data[offset:offset + 4])
            offset += 4
            selector_length, = struct.unpack("<i", message_data[offset:offset + 4])
            selector = BSON(message_data[offset:offset + selector_length])
            offset += selector_length
            update_length, = struct.unpack("<i", message_data[offset:offset + 4])
            update = BSON(message_data[offset:offset + update_length])
            return Update(*(header + (zero, name, flags, selector, update)))
        elif opcode == OP_INSERT:
            flags, = struct.unpack("<i", message_data[16:20])
            name = message_data[20:].split("\x00", 1)[0]
            offset = 20 + len(name) + 1
            docs = []
            while offset < len(message_data):
                document_length, = struct.unpack("<i", message_data[offset:offset + 4])
                docdata = message_data[offset:offset + document_length]
                doc = BSON(docdata)
                docs.append(doc)
                offset += document_length
            return Insert(*(header + (flags, name, docs)))
        elif opcode == OP_QUERY:
            flags, = struct.unpack("<i", message_data[16:20])
            name = message_data[20:].split("\x00", 1)[0]
            offset = 20 + len(name) + 1
            number_to_skip, number_to_return = struct.unpack("<ii", message_data[offset:offset + 8])
            offset += 8
            query_length, = struct.unpack("<i", message_data[offset:offset + 4])
            query_data = message_data[offset:offset + query_length]
            query = BSON(query_data)
            offset += query_length
            fields = None
            if message_length > offset:
                fields_length, = struct.unpack("<i", message_data[offset:offset + 4])
                fields = BSON(message_data[offset:offset + fields_length])
            return Query(*(header + (flags, name, number_to_skip, number_to_return, query, fields)))
        elif opcode == OP_GETMORE:
            zero, = struct.unpack("<i", message_data[16:20])
            if zero != 0:
                raise ConnectionFailure()
            name = message_data[20:].split("\x00", 1)[0]
            offset = 20 + len(name) + 1
            number_to_return, cursorid = struct.unpack("<iq", message_data[offset:offset + 12])
            return Getmore(*(header + (zero, name, number_to_return, cursorid)))
        elif opcode == OP_DELETE:
            zero, = struct.unpack("<i", message_data[16:20])
            if zero != 0:
                raise ConnectionFailure()
            name = message_data[20:].split("\x00", 1)[0]
            offset = 20 + len(name) + 1
            flags, = struct.unpack("<i", message_data[offset:offset + 4])
            offset += 4
            selector = BSON(message_data[offset:])
            return Delete(*(header + (zero, name, flags, selector)))
        elif opcode == OP_KILL_CURSORS:
            cursors = struct.unpack("<ii", message_data[16:24])
            if cursors[0] != 0:
                raise ConnectionFailure()
            offset = 24
            cursor_list = []
            for i in range(cursors[1]):
                cursor, = struct.unpack("<q", message_data[offset:offset + 8])
                cursor_list.append(cursor)
                offset += 8
            return KillCursors(*(header + cursors + (cursor_list,)))
        elif opcode == OP_MSG:
            if message_data[-1] != "\x00":
                raise ConnectionFailure()
            return Msg(*(header + (message_data[16:-1].decode("ascii"),)))
        elif opcode == OP_REPLY:
            reply = struct.unpack("<iqii", message_data[16:36])
            docs = []
            offset = 36
            for i in range(reply[3]):
                document_length, = struct.unpack("<i", message_data[offset:offset + 4])
                if document_length > (message_length - offset):
                    raise ConnectionFailure()
                docdata = message_data[offset:offset + document_length]
                doc = BSON(docdata)
                docs.append(doc)
                offset += document_length
            return Reply(*(header + reply + (docs,)))
        else:
            raise ConnectionFailure()
