# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from bson.son import SON
from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions
from pymongo.helpers import _check_command_response
from twisted.python.compat import unicode
from txmongo.collection import Collection
from txmongo.utils import timeout, check_deadline
from txmongo.protocol import Query


class Database(object):
    __factory = None

    def __init__(self, factory, database_name, write_concern=None,
                 codec_options=None):
        self.__factory = factory
        self.__write_concern = write_concern
        self.__codec_options = codec_options
        self._database_name = unicode(database_name)

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "Database(%r, %r)" % (self.__factory, self._database_name,)

    def __call__(self, database_name):
        return Database(self.__factory, database_name, self.__write_concern,
                        self.__codec_options)

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
    def command(self, command, value=1, check=True, allowable_errors=None,
                codec_options=DEFAULT_CODEC_OPTIONS, _deadline=None, write_concern=None, **kwargs):
        """command(command, value=1, check=True, allowable_errors=None, codec_options=DEFAULT_CODEC_OPTIONS)"""
        if isinstance(command, (bytes, unicode)):
            command = SON([(command, value)])
        options = kwargs.copy()
        command.update(options)

        def on_proto(protocol):
            check_deadline(_deadline)
            return self._send_command_to_proto(protocol, command, check=check, allowable_errors=allowable_errors, codec_options=codec_options, write_concern=write_concern, _deadline=_deadline)

        proto = self.connection.getprotocol()
        return proto.addCallback(on_proto)

    def _send_command_to_proto(self, proto, command, check=True, allowable_errors=None, codec_options=None, write_concern=None, flags=0, _deadline=None):
        def on_reply(raw_reply):
            check_deadline(_deadline)
            response = raw_reply.documents[0].decode(codec_options=codec_options or self.codec_options)

            if check:
                msg = "TxMongo: command {0} on namespace {1} failed with '%s'".format(repr(command), self)
                _check_command_response(response, msg, allowable_errors)

            return response

        query = Query(collection=str(self) + '.$cmd', query=command, flags=flags)
        return proto.send_QUERY(query).addCallback(on_reply)


    @timeout
    def create_collection(self, name, options=None, write_concern=None,
                          codec_options=None, **kwargs):
        collection = Collection(self, name, write_concern=write_concern,
                                codec_options=codec_options)

        if options is None and kwargs:
            options = kwargs
            kwargs = {}

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            options.update(kwargs)
            return self.command("create", name, **options)\
                .addCallback(lambda _: collection)

        return collection

    @timeout
    def drop_collection(self, name_or_collection, _deadline=None):
        """drop_collection(name_or_collection)"""
        if isinstance(name_or_collection, Collection):
            name = name_or_collection._collection_name
        elif isinstance(name_or_collection, (bytes, unicode)):
            name = name_or_collection
        else:
            raise TypeError("TxMongo: name must be an instance of basestring or txmongo.Collection")

        return self.command("drop", unicode(name), allowable_errors=["ns not found"],
                            _deadline=_deadline)

    @timeout
    def collection_names(self, _deadline=None):
        """collection_names()"""
        def ok(results):
            names = [r["name"] for r in results]
            names = [n[len(str(self)) + 1:] for n in names
                     if n.startswith(str(self) + ".")]
            names = [n for n in names if "$" not in n]
            return names
        return self["system.namespaces"].find(_deadline=_deadline).addCallback(ok)


    def authenticate(self, name, password, mechanism="DEFAULT"):
        """
        Send an authentication command for this database.
        mostly stolen from pymongo
        """
        if not isinstance(name, (bytes, unicode)):
            raise TypeError("TxMongo: name must be an instance of basestring.")
        if not isinstance(password, (bytes, unicode)):
            raise TypeError("TxMongo: password must be an instance of basestring.")

        """
        Authenticating
        """
        return self.connection.authenticate(self, name, password, mechanism)
