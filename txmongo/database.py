# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import annotations

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from txmongo.collection import Collection
from txmongo.protocol import Msg
from txmongo.sessions import ClientSession
from txmongo.utils import check_deadline, timeout


class Database:
    __factory = None

    def __init__(self, factory, database_name, write_concern=None, codec_options=None):
        self.__factory = factory
        self.__write_concern = write_concern
        self.__codec_options = codec_options
        self._database_name = str(database_name)

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "Database(%r, %r)" % (
            self.__factory,
            self._database_name,
        )

    def __call__(self, database_name):
        return Database(
            self.__factory, database_name, self.__write_concern, self.__codec_options
        )

    def __getitem__(self, collection_name):
        return Collection(self, collection_name)

    def __getattr__(self, collection_name):
        return self[collection_name]

    @property
    def name(self):
        return self._database_name

    @property
    def connection(self):
        return self.__factory

    @property
    def write_concern(self):
        return self.__write_concern or self.__factory.write_concern

    @property
    def codec_options(self):
        return self.__codec_options or self.__factory.codec_options

    @timeout
    @defer.inlineCallbacks
    def command(
        self,
        command,
        value=1,
        check=True,
        allowable_errors=None,
        codec_options=None,
        *,
        session: ClientSession = None,
        _deadline=None,
        **kwargs,
    ):
        """command(command, value=1, check=True, allowable_errors=None, codec_options=None, *, session: ClientSession=None)"""
        if isinstance(command, (bytes, str)):
            command = {command: value}
        if codec_options is None:
            codec_options = self.codec_options
        command.update(kwargs.copy())
        command["$db"] = self.name

        with self.connection._using_session(session, self.write_concern) as session:
            command.update(self.connection._get_session_command_fields(session))

            proto = yield self.connection.getprotocol()
            check_deadline(_deadline)

            errmsg = "TxMongo: command {0} on namespace {1} failed with '%s'".format(
                repr(command), self
            )
            reply = yield proto.send_msg(
                Msg.create(command, codec_options=codec_options),
                codec_options,
                check=check,
                errmsg=errmsg,
                allowable_errors=allowable_errors,
            )
            self.connection._advance_cluster_time(session, reply)
            return reply

    @timeout
    def create_collection(
        self, name, options=None, write_concern=None, codec_options=None, **kwargs
    ):
        collection = Collection(
            self, name, write_concern=write_concern, codec_options=codec_options
        )

        if options is None and kwargs:
            options = kwargs
            kwargs = {}

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            options.update(kwargs)
            return self.command("create", name, **options).addCallback(
                lambda _: collection
            )

        return collection

    @timeout
    def drop_collection(self, name_or_collection, _deadline=None):
        """drop_collection(name_or_collection)"""
        if isinstance(name_or_collection, Collection):
            name = name_or_collection._collection_name
        elif isinstance(name_or_collection, (bytes, str)):
            name = name_or_collection
        else:
            raise TypeError(
                "TxMongo: name must be an instance of basestring or txmongo.Collection"
            )

        return self.command(
            "drop", str(name), allowable_errors=["ns not found"], _deadline=_deadline
        )

    @timeout
    @inlineCallbacks
    def collection_names(self, *, batch_size=0, _deadline=None):
        """collection_names()"""

        cmd = {
            "listCollections": 1,
            "nameOnly": True,
            "authorizedCollections": True,
        }
        if batch_size:
            # "cursor" parameter is undocumented, but working in 4.0-8.0 and
            # is useful for testing purposes
            cmd["cursor"] = {"batchSize": batch_size}
        response = yield self.command(
            cmd,
            _deadline=_deadline,
        )
        names = []
        cursor_id = response["cursor"]["id"]
        names.extend(coll["name"] for coll in response["cursor"]["firstBatch"])
        while cursor_id:
            response = yield self.command(
                {
                    "getMore": cursor_id,
                    "$db": self.name,
                    "collection": "$cmd.listCollections",
                    "batchSize": batch_size,
                },
                _deadline=_deadline,
            )
            cursor_id = response["cursor"]["id"]
            names.extend(coll["name"] for coll in response["cursor"]["nextBatch"])
        return names

    def authenticate(self, name, password, mechanism="DEFAULT"):
        """
        Send an authentication command for this database.
        mostly stolen from pymongo
        """
        if not isinstance(name, (bytes, str)):
            raise TypeError("TxMongo: name must be an instance of basestring.")
        if not isinstance(password, (bytes, str)):
            raise TypeError("TxMongo: password must be an instance of basestring.")

        """
        Authenticating
        """
        return self.connection.authenticate(self, name, password, mechanism)
