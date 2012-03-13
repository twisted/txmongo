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

from twisted.internet import defer
from txmongo._pymongo.son import SON
from txmongo._pymongo import helpers
from txmongo.collection import Collection


class Database(object):
    def __init__(self, factory, database_name):
        self.__factory = factory
        self._database_name = database_name

    @property
    def _connection(self):
        return self.__factory.connection()

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "<mongodb Database: %s>" % self._database_name

    def __call__(self, database_name):
        return Database(self._factory, database_name)

    def __getitem__(self, collection_name):
        return Collection(self, collection_name)

    def __getattr__(self, collection_name):
        return self[collection_name]

    def create_collection(self, name, options={}):
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

    def authenticate(self, name, password):
        """
        Send an authentication command for this database.
        mostly stolen from pymongo
        """
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")
        if not isinstance(password, basestring):
            raise TypeError("password must be an instance of basestring")

        d = defer.Deferred()
        # First get the nonce
        self["$cmd"].find_one({"getnonce": 1}
                ).addCallback(self.authenticate_with_nonce, name, password, d
                ).addErrback(d.errback)


        return d

    def authenticate_with_nonce(self, result, name, password, d):
        nonce = result['nonce']
        key = helpers._auth_key(nonce, name, password)

        # hacky because order matters
        auth_command = SON(authenticate=1)
        auth_command['user'] = unicode(name)
        auth_command['nonce'] = nonce
        auth_command['key'] = key

        # Now actually authenticate
        self["$cmd"].find_one(auth_command
                ).addCallback(self.authenticated, d
                ).addErrback(d.errback)

    def authenticated(self, result, d):
        """might want to just call callback with 0.0 instead of errback"""
        ok = result['ok']
        if ok:
            d.callback(ok)
        else:
            d.errback(result['errmsg'])

