# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.
import bson
from twisted.internet import defer

from txmongo.collection import Collection
from txmongo.protocol import Msg
from txmongo.pymongo_internals import _check_command_response
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
        _deadline=None,
        **kwargs,
    ):
        """command(command, value=1, check=True, allowable_errors=None, codec_options=None)"""
        if isinstance(command, (bytes, str)):
            command = {command: value}
        if codec_options is None:
            codec_options = self.codec_options
        options = kwargs.copy()
        command.update(options)
        command["$db"] = self.name

        proto = yield self.connection.getprotocol()
        check_deadline(_deadline)

        response = yield proto.send_MSG(
            Msg(body=bson.encode(command, codec_options=codec_options))
        )
        reply = bson.decode(response.body, codec_options=codec_options)
        if check:
            msg = "TxMongo: command {0} on namespace {1} failed with '%s'".format(
                repr(command), self
            )
            _check_command_response(reply, msg, allowable_errors)
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
    def collection_names(self, _deadline=None):
        """collection_names()"""

        def ok(results):
            names = [r["name"] for r in results]
            names = [
                n[len(str(self)) + 1 :] for n in names if n.startswith(str(self) + ".")
            ]
            names = [n for n in names if "$" not in n]
            return names

        return self["system.namespaces"].find(_deadline=_deadline).addCallback(ok)

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
