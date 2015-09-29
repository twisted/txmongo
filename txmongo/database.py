# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from bson.son import SON
from pymongo.helpers import _check_command_response
from twisted.internet import defer
from twisted.python.compat import unicode
from txmongo.collection import Collection
from txmongo.utils import timeout


class Database(object):
    __factory = None

    def __init__(self, factory, database_name, write_concern=None):
        self.__factory = factory
        self.__write_concern = write_concern
        self._database_name = unicode(database_name)

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "Database(%r, %r)" % (self.__factory, self._database_name,)

    def __call__(self, database_name):
        return Database(self.__factory, database_name, self.__write_concern)

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

    @timeout
    @defer.inlineCallbacks
    def command(self, command, value=1, check=True, allowable_errors=None, **kwargs):
        if isinstance(command, (bytes, unicode)):
            command = SON([(command, value)])
        options = kwargs.copy()
        options.pop("_deadline", None)
        command.update(options)

        ns = self["$cmd"]
        response = yield ns.find_one(command, **kwargs)

        if check:
            msg = "command {0} on namespace {1} failed: %s".format(repr(command), ns)
            _check_command_response(response, msg, allowable_errors)

        defer.returnValue(response)

    @timeout
    @defer.inlineCallbacks
    def create_collection(self, name, options=None, **kwargs):
        collection = Collection(self, name)

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            options.update(kwargs)
            yield self.command("create", name, **options)

        defer.returnValue(collection)

    @timeout
    @defer.inlineCallbacks
    def drop_collection(self, name_or_collection, **kwargs):
        if isinstance(name_or_collection, Collection):
            name = name_or_collection._collection_name
        elif isinstance(name_or_collection, (bytes, unicode)):
            name = name_or_collection
        else:
            raise TypeError("name must be an instance of basestring or txmongo.Collection")

        yield self.command("drop", unicode(name), allowable_errors=["ns not found"], **kwargs)

    @timeout
    @defer.inlineCallbacks
    def collection_names(self, **kwargs):
        results = yield self["system.namespaces"].find(**kwargs)

        names = [r["name"] for r in results]
        names = [n[len(str(self)) + 1:] for n in names
                 if n.startswith(str(self) + ".")]
        names = [n for n in names if "$" not in n]
        defer.returnValue(names)

    @defer.inlineCallbacks
    def authenticate(self, name, password, mechanism="DEFAULT"):
        """
        Send an authentication command for this database.
        mostly stolen from pymongo
        """
        if not isinstance(name, (bytes, unicode)):
            raise TypeError("name must be an instance of basestring")
        if not isinstance(password, (bytes, unicode)):
            raise TypeError("password must be an instance of basestring")

        """
        Authenticating
        """
        yield self.connection.authenticate(self, name, password, mechanism)
