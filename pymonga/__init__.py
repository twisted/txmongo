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

from pymonga import wire
from pymonga.tracker import Tracker
from pymonga._pymongo.objectid import ObjectId
from twisted.internet import defer, reactor, protocol

"""An asynchronous Mongo driver for Python."""

class MongoFactory(protocol.ReconnectingClientFactory):
    protocol = wire.MongoProtocol

    def __init__(self, deferred):
        self.tracker = Tracker()
        self.deferred = deferred

    def buildProtocol(self, addr):
        p = self.protocol()
        self.tracker.append(p)
        if self.deferred is not None:
            reactor.callLater(0, self.deferred.callback, self.tracker)
            self.deferred = None
        return p

    def clientConnectionLost(self, connector, error):
        self.resetDelay()
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, error)

    def clientConnectionFailed(self, connector, error):
        self.resetDelay()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, error)


def Connection(host="localhost", port=27017, reconnect=True):
    d = defer.Deferred()
    reactor.connectTCP(host, port, MongoFactory(d))
    return d

def ConnectionPool(host="localhost", port=27017, reconnect=True, size=5):
    d = defer.Deferred()
    factory = MongoFactory(d)
    for x in xrange(size):
        reactor.connectTCP(host, port, factory)
    return d
