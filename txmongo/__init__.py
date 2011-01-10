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

from txmongo.database import Database
from txmongo.protocol import MongoProtocol
from txmongo._pymongo.objectid import ObjectId
from twisted.internet import task, defer, reactor, protocol


DISCONNECT_INTERVAL = .5


class _offline(object):
    def OP_INSERT(self, *args, **kwargs):
        pass

    def OP_UPDATE(self, *args, **kwargs):
        pass

    def OP_DELETE(self, *args, **kwargs):
        pass

    def OP_QUERY(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred


class MongoAPI(object):
    def __init__(self, factory):
        self.__factory = factory
        self._connected = factory.deferred

    def __connection_lost(self, deferred):
        if self.__factory.size == 0:
            self.__task.stop()
            deferred.callback(True)

    def disconnect(self):
        self.__factory.continueTrying = 0
        for conn in self.__factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        self.__task = task.LoopingCall(self.__connection_lost, d)
        self.__task.start(DISCONNECT_INTERVAL, True)
        return d

    def __repr__(self):
        try:
            cli = self.__factory.pool[0].transport.getPeer()
        except:
            info = "not connected"
        else:
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self.__factory.size)
        return "<Mongodb: %s>" % info

    def __getitem__(self, database_name):
        return Database(self.__factory, database_name)

    def __getattr__(self, database_name):
        return self[database_name]


class _MongoFactory(protocol.ReconnectingClientFactory):
    maxDelay = 10
    protocol = MongoProtocol

    def __init__(self, pool_size):
        self.idx = 0
        self.size = 0
        self.pool = []
        self.pool_size = pool_size
        self.deferred = defer.Deferred()
        self.API = MongoAPI(self)

    def append(self, conn):
        self.pool.append(conn)
        self.size += 1
        if self.deferred and self.size == self.pool_size:
            self.deferred.callback(self.API)
            self.deferred = None

    def remove(self, conn):
        try:
            self.pool.remove(conn)
        except:
            pass
        self.size = len(self.pool)

    def connection(self):
        try:
            assert self.size
            conn = self.pool[self.idx % self.size]
            self.idx += 1
        except:
            return _offline()
        else:
            return conn


def _Connection(host, port, reconnect, pool_size, lazy):
    factory = _MongoFactory(pool_size)
    factory.continueTrying = reconnect
    for x in xrange(pool_size):
        reactor.connectTCP(host, port, factory)
    return (lazy is True) and factory.API or factory.deferred


def MongoConnection(host="localhost", port=27017, reconnect=True):
    return _Connection(host, port, reconnect, pool_size=1, lazy=False)


def lazyMongoConnection(host="localhost", port=27017, reconnect=True):
    return _Connection(host, port, reconnect, pool_size=1, lazy=True)


def MongoConnectionPool(host="localhost", port=27017, reconnect=True, pool_size=5):
    return _Connection(host, port, reconnect, pool_size, lazy=False)


def lazyMongoConnectionPool(host="localhost", port=27017, reconnect=True, pool_size=5):
    return _Connection(host, port, reconnect, pool_size, lazy=True)
