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

import base64
import hashlib
import hmac
import logging
import struct
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from hashlib import sha1
from random import SystemRandom
from typing import Dict, List

import bson
from bson import SON, Binary, CodecOptions
from pymongo import auth
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    CursorNotFound,
    NotPrimaryError,
    OperationFailure,
)
from twisted.internet import defer, error, protocol
from twisted.python import failure, log

try:
    from hashlib import pbkdf2_hmac as _hi
except ImportError:

    def _hi(hash_name, data, salt, iterations):
        """A simple implementation of PBKDF2-HMAC."""
        mac = hmac.HMAC(data, None, getattr(hashlib, hash_name))

        def _digest(msg, mac=mac):
            """Get a digest for msg."""
            _mac = mac.copy()
            _mac.update(msg)
            return _mac.digest()

        from_bytes = int.from_bytes
        to_bytes = int.to_bytes

        _u1 = _digest(salt + b"\x00\x00\x00\x01")
        _ui = from_bytes(_u1, "big")
        for _ in range(iterations - 1):
            _u1 = _digest(_u1)
            _ui ^= from_bytes(_u1, "big")
        return to_bytes(_ui, mac.digest_size, "big")


INT_MAX = 2147483647

OP_REPLY = 1
OP_QUERY = 2004
OP_COMPRESSED = 2012
OP_MSG = 2013


@dataclass
class BaseMessage:
    request_id: int = 0
    response_to: int = 0

    @classmethod
    @abstractmethod
    def opcode(cls) -> int: ...

    def encode(self, request_id: int) -> bytes:
        payload = self._payload()
        return b"".join(
            [
                struct.pack(
                    "<iiii",
                    sum(map(len, payload)) + 4 * 4,
                    request_id,
                    self.response_to,
                    self.opcode(),
                ),
                *payload,
            ]
        )

    @abstractmethod
    def _payload(self) -> List[bytes]: ...

    @classmethod
    @abstractmethod
    def decode(
        cls, request_id: int, response_to: int, opcode: int, message_data: bytes
    ) -> "BaseMessage": ...


QUERY_TAILABLE_CURSOR = 1 << 1
QUERY_SLAVE_OK = 1 << 2
QUERY_OPLOG_REPLAY = 1 << 3
QUERY_NO_CURSOR_TIMEOUT = 1 << 4
QUERY_AWAIT_DATA = 1 << 5
QUERY_EXHAUST = 1 << 6
QUERY_PARTIAL = 1 << 7


@dataclass
class Query(BaseMessage):
    flags: int = 0
    collection: str = ""
    n_to_skip: int = 0
    n_to_return: int = -1
    query: bytes = bson.encode({})
    fields: bytes = None

    @classmethod
    def opcode(cls):
        return OP_QUERY

    def _payload(self) -> List[bytes]:
        return [
            struct.pack("<i", self.flags),
            self.collection.encode("ascii"),
            b"\x00",
            struct.pack("<ii", self.n_to_skip, self.n_to_return),
            self.query,
            self.fields or b"",
        ]

    @classmethod
    def decode(
        cls, request_id: int, response_to: int, opcode: int, message_data: bytes
    ) -> "Query":
        (flags,) = struct.unpack("<i", message_data[16:20])
        name = message_data[20:].split(b"\x00", 1)[0]
        offset = 20 + len(name) + 1
        number_to_skip, number_to_return = struct.unpack(
            "<ii", message_data[offset : offset + 8]
        )
        offset += 8
        (query_length,) = struct.unpack("<i", message_data[offset : offset + 4])
        query_data = message_data[offset : offset + query_length]
        offset += query_length
        fields = None
        if len(message_data) > offset:
            (fields_length,) = struct.unpack("<i", message_data[offset : offset + 4])
            fields = message_data[offset : offset + fields_length]
        return Query(
            request_id=request_id,
            response_to=response_to,
            flags=flags,
            collection=name.decode(),
            n_to_skip=number_to_skip,
            n_to_return=number_to_return,
            query=query_data,
            fields=fields,
        )


REPLY_CURSOR_NOT_FOUND = 1 << 0
REPLY_QUERY_FAILURE = 1 << 1
REPLY_SHARD_CONFIG_STALE = 1 << 2
REPLY_AWAIT_CAPABLE = 1 << 3


@dataclass
class Reply(BaseMessage):
    response_flags: int = 0
    cursor_id: int = 0
    starting_from: int = 0
    documents: List[bytes] = None

    @classmethod
    def opcode(cls):
        return OP_REPLY

    def _payload(self) -> List[bytes]:
        return [
            struct.pack(
                "<iqii",
                self.response_flags,
                self.cursor_id,
                self.starting_from,
                len(self.documents),
            ),
            *self.documents,
        ]

    @classmethod
    def decode(
        cls, request_id: int, response_to: int, opcode: int, message_data: bytes
    ) -> "Reply":
        msg_len = len(message_data)
        (response_flags, cursor_id, starting_from, n_returned) = struct.unpack(
            "<iqii", message_data[16:36]
        )
        docs = []
        offset = 36
        for i in range(n_returned):
            (document_length,) = struct.unpack("<i", message_data[offset : offset + 4])
            if document_length > (msg_len - offset):
                raise ConnectionFailure()
            doc = message_data[offset : offset + document_length]
            docs.append(doc)
            offset += document_length

        return Reply(
            request_id=request_id,
            response_to=response_to,
            response_flags=response_flags,
            cursor_id=cursor_id,
            starting_from=starting_from,
            documents=docs,
        )


OP_MSG_CHECKSUM_PRESENT = 1 << 0
OP_MSG_MORE_TO_COME = 1 << 1
OP_MSG_EXHAUST_ALLOWED = 1 << 16


@dataclass
class Msg(BaseMessage):
    body: bytes = bson.encode({})
    flag_bits: int = 0
    payload: Dict[str, List[bytes]] = field(default_factory=dict)

    @classmethod
    def opcode(cls):
        return OP_MSG

    @classmethod
    def create_flag_bits(cls, not_more_to_come: bool) -> int:
        return 0 if not_more_to_come else OP_MSG_MORE_TO_COME

    def size_in_bytes(self) -> int:
        """return estimated overall message length including messageLength and requestID"""
        # checksum is not added since we don't support it for now
        return (
            4 * 4  # header
            + 4  # flatBits
            + 1  # payloadType
            + len(self.body)  # body length
            + sum(
                (
                    1  # payloadType
                    + len(key.encode("ascii"))  # identifier
                    + 1  # closing NULL of identifier
                    + sum(len(doc) for doc in docs)  # payload
                )
                for key, docs in self.payload.items()
            )
        )

    def _payload(self) -> List[bytes]:
        """return list of bytes objects to be sent over the wire,
        excluding leading messageLength and requestID"""
        output = [
            struct.pack("<i", self.flag_bits),
            # section with payloadType=0
            b"\x00",
            self.body,
        ]
        for arg_name, docs in self.payload.items():
            # section with payloadType=1
            payload = [arg_name.encode("ascii"), b"\x00", *docs]
            output.extend(
                [
                    b"\x01",
                    struct.pack("<i", 4 + sum(len(x) for x in payload)),
                    *payload,
                ]
            )
        return output

    @classmethod
    def decode(
        cls, request_id: int, response_to: int, opcode: int, message_data: bytes
    ) -> "Msg":
        msg_length = len(message_data)
        body = None
        payload: Dict[str, List[bytes]] = {}
        offset = 20
        while offset < msg_length:
            payload_type = message_data[offset]
            offset += 1

            if payload_type == 0:
                bson_size = struct.unpack(
                    "<i",
                    message_data[offset : offset + 4],
                )[0]
                body = message_data[offset : offset + bson_size]
                offset += bson_size

            elif payload_type == 1:
                size = struct.unpack("<i", message_data[offset : offset + 4])[0]
                null_pos = message_data.index(0, offset + 4)
                arg_name = message_data[offset + 4 : null_pos].decode("ascii")
                bsons = []
                bson_offset = null_pos + 1
                while bson_offset - offset < size:
                    bson_size = struct.unpack(
                        "<i", message_data[bson_offset : bson_offset + 4]
                    )[0]
                    bsons.append(message_data[bson_offset : bson_offset + bson_size])
                    bson_offset += bson_size
                payload[arg_name] = bsons
                offset = bson_offset

        return cls(
            request_id=request_id,
            response_to=response_to,
            flag_bits=struct.unpack("<i", message_data[16:20])[0],
            body=body,
            payload=payload,
        )


class MongoDecoder:
    # All BaseMessage subclasses must be defined above this line
    OPCODE_TO_CLASS = {
        subclass.opcode(): subclass for subclass in BaseMessage.__subclasses__()
    }

    dataBuffer = None

    def __init__(self):
        self.dataBuffer = b""

    def feed(self, data):
        self.dataBuffer += data

    def __next__(self):
        if len(self.dataBuffer) < 16:
            return None
        (message_length,) = struct.unpack("<i", self.dataBuffer[:4])
        if len(self.dataBuffer) < message_length:
            return None
        if message_length < 16:
            raise ConnectionFailure()
        message_data = self.dataBuffer[:message_length]
        self.dataBuffer = self.dataBuffer[message_length:]
        return self.decode(message_data)

    next = __next__

    @classmethod
    def decode(cls, message_data) -> BaseMessage:
        (request_id, response_to, opcode) = struct.unpack("<iii", message_data[4:16])
        try:
            msg_class = cls.OPCODE_TO_CLASS[opcode]
        except KeyError:
            log.msg(f"TxMongo: unknown message {opcode=}", logLevel=logging.ERROR)
            raise ConnectionFailure()
        return msg_class.decode(request_id, response_to, opcode, message_data)


class MongoSenderProtocol(protocol.Protocol):
    __request_id = 1

    def get_request_id(self):
        return self.__request_id

    def _send(self, request: BaseMessage):
        request_id, self.__request_id = self.__request_id, self.__request_id + 1
        if self.__request_id >= INT_MAX:
            self.__request_id = 1
        self.transport.write(request.encode(request_id))
        return request_id


class MongoReceiverProtocol(protocol.Protocol, metaclass=ABCMeta):
    __decoder: MongoDecoder

    def __init__(self):
        self.__decoder = MongoDecoder()

    def dataReceived(self, data):
        self.__decoder.feed(data)

        try:
            request = next(self.__decoder)
            while request:
                self.handle(request)
                request = next(self.__decoder)
        except Exception as e:
            self.fail(e)

    def fail(self, reason):
        log.err(str(reason))
        self.transport.loseConnection()

    @abstractmethod
    def handle(self, msg: BaseMessage): ...


connectionDone = failure.Failure(error.ConnectionDone())
connectionDone.cleanFailure()


class MongoAuthenticationError(Exception):
    pass


class MongoProtocol(MongoReceiverProtocol, MongoSenderProtocol):
    __connection_ready = None
    __deferreds = None

    min_wire_version = None
    max_wire_version = None

    max_bson_size = None
    max_write_batch_size = None
    max_message_size = None

    def __init__(self):
        super().__init__()
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
        # We need to clear factory.instance before failing deferreds
        # because client code might immediately re-issue query when
        # it catches AutoReconnect, so we must invalidate current
        # connection before. Factory.clientConnectionFailed() is called
        # too late.
        self.factory.setInstance(None, reason)

        if self.__deferreds:
            deferreds, self.__deferreds = self.__deferreds, {}
            for df in deferreds.values():
                df.errback(AutoReconnect("TxMongo lost connection to MongoDB."))
        deferreds, self.__connection_ready = self.__connection_ready, []
        if deferreds:
            for df in deferreds:
                df.errback(AutoReconnect("TxMongo lost connection to MongoDB."))

        protocol.Protocol.connectionLost(self, reason)

    def connectionReady(self):
        if self.transport:
            log.msg(
                "TxMongo: connection to MongoDB established.", logLevel=logging.INFO
            )
            return defer.succeed(None)
        if not self.__connection_ready:
            self.__connection_ready = []

        def on_cancel(d):
            self.__connection_ready.remove(d)

        df = defer.Deferred(on_cancel)
        self.__connection_ready.append(df)
        return df

    def __wait_for_reply_to(self, request_id):
        def on_cancel(_):
            del self.__deferreds[request_id]

        df = defer.Deferred(on_cancel)
        self.__deferreds[request_id] = df
        return df

    def send_query(self, request):
        request_id = self._send(request)
        return self.__wait_for_reply_to(request_id)

    def send_msg(self, msg: Msg) -> defer.Deferred[Msg]:
        """Send Msg (OP_MSG) and return deferred.

        If OP_MSG has OP_MSG_MORE_TO_COME flag set, returns already fired deferred with None as a result.
        """
        request_id = self._send(msg)
        if msg.flag_bits & OP_MSG_MORE_TO_COME:
            return defer.succeed(None)
        return self.__wait_for_reply_to(request_id)

    def send_simple_msg(
        self, body: dict, codec_options: CodecOptions
    ) -> defer.Deferred[dict]:
        """Send simple OP_MSG without extracted payload and return parsed response."""

        def on_response(response: Msg):
            reply = bson.decode(response.body, codec_options)
            for key, bin_docs in msg.payload.items():
                reply[key] = [bson.decode(doc, codec_options) for doc in bin_docs]
            return reply

        msg = Msg(body=bson.encode(body, codec_options=codec_options))
        return self.send_msg(msg).addCallback(on_response)

    def handle(self, request: BaseMessage):
        if isinstance(request, Reply):
            self.handle_reply(request)
        elif isinstance(request, Msg):
            self.handle_msg(request)
        else:
            log.msg(
                "TxMongo: no handler found for opcode '%d'" % request.opcode(),
                logLevel=logging.WARNING,
            )

    def handle_reply(self, request):
        if request.response_to in self.__deferreds:
            df = self.__deferreds.pop(request.response_to)
            if request.response_flags & REPLY_QUERY_FAILURE:
                doc = bson.decode(request.documents[0])
                code = doc.get("code")
                msg = "TxMongo: " + doc.get("$err", "Unknown error")
                fail_conn = False
                if code == 13435:
                    err = NotPrimaryError(msg)
                    fail_conn = True
                else:
                    err = OperationFailure(msg, code)
                df.errback(err)
                if fail_conn:
                    self.transport.loseConnection()
            elif request.response_flags & REPLY_CURSOR_NOT_FOUND:
                # Inspired by pymongo handling
                msg = "Cursor not found, cursor id: %d" % (request.cursor_id,)
                errobj = {"ok": 0, "errmsg": msg, "code": 43}
                df.errback(CursorNotFound(msg, 43, errobj))
            else:
                df.callback(request)

    def handle_msg(self, request: Msg):
        if dfr := self.__deferreds.pop(request.response_to, None):
            dfr.callback(request)

    def set_wire_versions(self, min_wire_version, max_wire_version):
        self.min_wire_version = min_wire_version
        self.max_wire_version = max_wire_version

    def send_op_query_command(self, database, query):
        cmd_collection = str(database) + ".$cmd"
        return self.send_query(
            Query(collection=cmd_collection, query=bson.encode(query))
        ).addCallback(lambda response: bson.decode(response.documents[0]))

    @defer.inlineCallbacks
    def authenticate_scram_sha1(self, database_name, username, password):
        # Totally stolen from pymongo.auth
        user = username.replace("=", "=3D").replace(",", "=2C")
        nonce = base64.standard_b64encode(str(SystemRandom().random()).encode("ascii"))[
            2:
        ]
        first_bare = "n={0},r={1}".format(user, nonce.decode()).encode("ascii")

        cmd = SON(
            [
                ("saslStart", 1),
                ("mechanism", "SCRAM-SHA-1"),
                ("autoAuthorize", 1),
                ("payload", Binary(b"n,," + first_bare)),
            ]
        )
        result = yield self.send_op_query_command(database_name, cmd)

        server_first = result["payload"]
        parsed = auth._parse_scram_response(server_first)
        iterations = int(parsed[b"i"])
        salt = parsed[b"s"]
        rnonce = parsed[b"r"]
        if not rnonce.startswith(nonce):
            raise MongoAuthenticationError("TxMongo: server returned an invalid nonce.")

        without_proof = b"c=biws,r=" + rnonce
        salted_pass = _hi(
            "sha1",
            auth._password_digest(username, password).encode("utf-8"),
            base64.standard_b64decode(salt),
            iterations,
        )
        client_key = hmac.HMAC(salted_pass, b"Client Key", sha1).digest()
        stored_key = sha1(client_key).digest()
        auth_msg = b",".join((first_bare, server_first, without_proof))
        client_sig = hmac.HMAC(stored_key, auth_msg, sha1).digest()
        client_proof = b"p=" + base64.standard_b64encode(
            auth._xor(client_key, client_sig)
        )
        client_final = b",".join((without_proof, client_proof))

        server_key = hmac.HMAC(salted_pass, b"Server Key", sha1).digest()
        server_sig = base64.standard_b64encode(
            hmac.HMAC(server_key, auth_msg, sha1).digest()
        )

        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", result["conversationId"]),
                ("payload", Binary(client_final)),
            ]
        )
        result = yield self.send_op_query_command(database_name, cmd)

        if not result["ok"]:
            raise MongoAuthenticationError("TxMongo: authentication failed.")

        parsed = auth._parse_scram_response(result["payload"])
        if parsed[b"v"] != server_sig:
            raise MongoAuthenticationError(
                "TxMongo: server returned an invalid signature."
            )

        # Depending on how it's configured, Cyrus SASL (which the server uses)
        # requires a third empty challenge.
        if not result["done"]:
            cmd = SON(
                [
                    ("saslContinue", 1),
                    ("conversationId", result["conversationId"]),
                    ("payload", Binary(b"")),
                ]
            )
            result = yield self.send_op_query_command(database_name, cmd)
            if not result["done"]:
                raise MongoAuthenticationError(
                    "TxMongo: SASL conversation failed to complete."
                )

    @defer.inlineCallbacks
    def authenticate_mongo_x509(self, database_name, username, password):
        query = SON(
            [("authenticate", 1), ("mechanism", "MONGODB-X509"), ("user", username)]
        )
        result = yield self.send_op_query_command("$external", query)
        if not result["ok"]:
            raise MongoAuthenticationError(result["errmsg"])
        return result

    @defer.inlineCallbacks
    def authenticate(self, database_name, username, password, mechanism):
        database_name = str(database_name)
        username = str(username)
        password = str(password)

        yield self.__auth_lock.acquire()

        try:
            if mechanism == "SCRAM-SHA-1":
                auth_func = self.authenticate_scram_sha1
            elif mechanism == "MONGODB-X509":
                auth_func = self.authenticate_mongo_x509
            elif mechanism == "DEFAULT":
                auth_func = self.authenticate_scram_sha1
            else:
                raise MongoAuthenticationError(
                    "TxMongo: Unknown authentication mechanism: {0}".format(mechanism)
                )

            result = yield auth_func(database_name, username, password)
            return result
        finally:
            self.__auth_lock.release()
