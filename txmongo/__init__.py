# coding: utf-8
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

from bson import ObjectId, SON
from pymongo.uri_parser import parse_uri
from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ReconnectingClientFactory
from txmongo.database import Database
from txmongo.protocol import MongoProtocol

class _Connection(ReconnectingClientFactory):
    __notify_ready = None
    __pool = None
    __uri = None
    instance = None
    protocol = MongoProtocol

    def __init__(self, pool, uri):
        self.__pool = pool
        self.__uri = uri

    def notifyReady(self):
        if self.instance:
            return defer.succeed(self)
        if self.__notify_ready is None:
            self.__notify_ready = []
        df = defer.Deferred()
        self.__notify_ready.append(df)
        return df

    def setinstance(self, instance=None, reason=None):
        self.instance = instance
        deferreds, self.__notify_ready = self.__notify_ready, []
        for df in deferreds:
            if self.instance:
                df.callback(self)
            else:
                df.errback(reason)

    @property
    def uri(self):
        return self.__uri

class ConnectionPool(object):
    __index = 0
    __pool = None
    __pool_size = None
    __uri = None

    def __init__(self, uri='mongodb://127.0.0.1:27017', pool_size=1):
        assert isinstance(uri, basestring)
        assert isinstance(pool_size, int)
        assert pool_size >= 1

        if not uri.startswith('mongodb://'):
            uri = 'mongodb://' + uri

        self.__uri = parse_uri(uri)
        self.__pool_size = pool_size
        self.__pool = [_Connection(self, self.__uri) for i in xrange(pool_size)]

        host, port = self.__uri['nodelist'][0]
        for factory in self.__pool:
            factory.connector = reactor.connectTCP(host, port, factory)

    def __getitem__(self, name):
        return Database(self, name)

    def __getattr__(self, name):
        return self[name]

    def __repr__(self):
        if self.uri['nodelist']:
            return 'Connection(%r, %r)' % self.uri['nodelist'][0]
        return 'Connection()'

    def disconnect(self):
        for factory in self.__pool:
            factory.stopTrying()
            factory.stopFactory()
            if factory.instance and factory.instance.transport:
                factory.instance.transport.loseConnection()
            if factory.connector:
                factory.connector.disconnect()
        # Wait for the next iteration of the loop for resolvers
        # to potentially cleanup.
        df = defer.Deferred()
        reactor.callLater(0, df.callback, None)
        return df

    def getprotocol(self):
        # Get the next protocol available for communication in the pool.
        connection = self.__pool[self.__index]
        self.__index = (self.__index + 1) % self.__pool_size

        # If the connection is already connected, just return it.
        if connection.instance:
            return defer.succeed(connection.instance)

        # Wait for the connection to connection.
        return connection.notifyReady().addCallback(lambda c: c.instance)

    @property
    def uri(self):
        return self.__uri

class Connection(ConnectionPool):
    pass

# FOR LEGACY REASONS
class MongoConnection(Connection):
    def __init__(self, host, port, pool_size=1):
        uri = 'mongodb://%s:%d/' % (host, port)
        Connection.__init__(self, uri, pool_size=pool_size)

lazyMongoConnectionPool = MongoConnection
lazyMongoConnection = MongoConnection
MongoConnectionPool = MongoConnection

if __name__ == '__main__':
    import sys
    from twisted.python import log

    log.startLogging(sys.stdout)
    connection = Connection()
    reactor.run()
