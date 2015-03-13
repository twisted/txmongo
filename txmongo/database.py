# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from bson.son import SON
from twisted.internet import defer
from txmongo.collection import Collection


class Database(object):
    __factory = None

    def __init__(self, factory, database_name):
        self.__factory = factory
        self._database_name = unicode(database_name)

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "Database(%r, %r)" % (self.__factory, self._database_name,)

    def __call__(self, database_name):
        return Database(self._factory, database_name)

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

    def create_collection(self, name, options=None):
        def wrapper(result, deferred, collection):
            if result.get("ok", 0.0):
                deferred.callback(collection)
            else:
                deferred.errback(RuntimeError(result.get("errmsg", "unknown error")))

        deferred = defer.Deferred()
        collection = Collection(self, name)

        if options:
            if "size" in options:
                options["size"] = float(options["size"])

            command = SON({"create": name})
            command.update(options)
            d = self["$cmd"].find_one(command)
            d.addCallback(wrapper, deferred, collection)
        else:
            deferred.callback(collection)

        return deferred

    def drop_collection(self, name_or_collection):
        if isinstance(name_or_collection, Collection):
            name = name_or_collection._collection_name
        elif isinstance(name_or_collection, basestring):
            name = name_or_collection
        else:
            raise TypeError("name must be an instance of basestring or txmongo.Collection")

        return self["$cmd"].find_one({"drop": unicode(name)})

    def collection_names(self):
        def wrapper(results):
            names = [r["name"] for r in results]
            names = [n[len(str(self)) + 1:] for n in names
                     if n.startswith(str(self) + ".")]
            names = [n for n in names if "$" not in n]
            return names

        d = self["system.namespaces"].find()
        d.addCallback(wrapper)
        return d

    @defer.inlineCallbacks
    def authenticate(self, name, password):
        """
        Send an authentication command for this database.
        mostly stolen from pymongo
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")
        if not isinstance(password, basestring):
            raise TypeError("password must be an instance of basestring")

        """
        Authenticating
        """
        yield self.connection.authenticate(self, name, password)
