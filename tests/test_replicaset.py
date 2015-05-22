# coding: utf-8
# Copyright 2010 Mark L.
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

from bson.son import SON
from pymongo.errors import OperationFailure, AutoReconnect
from twisted.trial import unittest
from twisted.internet import defer, base, reactor
from txmongo.connection import MongoConnection, ConnectionPool, _Connection
from txmongo.protocol import QUERY_SLAVE_OK

from mongod import Mongod

# base.DelayedCall.debug = True


class TestReplicaSet(unittest.TestCase):

    ports = [37017, 37018, 37019]
    rsname = "rs1"

    rsconfig = {
        "_id": rsname,
        "members": [
            {"_id": i, "host": "localhost:{0}".format(port) }
            for i, port in enumerate(ports)
        ]
    }
    # We assume first member to be master
    rsconfig["members"][0]["priority"] = 2

    def __sleep(self, delay):
        d = defer.Deferred()
        reactor.callLater(delay, d.callback, None)
        return d

    @defer.inlineCallbacks
    def setUp(self):
        self.__mongod = [Mongod(port=p, replset=self.rsname) for p in self.ports]
        yield defer.gatherResults([mongo.start() for mongo in self.__mongod])

        master_uri = "mongodb://localhost:{0}/?readPreference=secondaryPreferred".format(self.ports[0])
        master = ConnectionPool(master_uri)
        yield master.admin["$cmd"].find_one({"replSetInitiate": self.rsconfig})

        ready = False
        for i in xrange(120):
            yield self.__sleep(0.5)

            # My practice shows that we need to query both ismaster and replSetGetStatus
            # to be sure that replica set is up and running, primary is elected and all
            # secondaries are in sync and ready to became new primary

            ismaster_req = master.admin["$cmd"].find_one({"ismaster": 1})
            replstatus_req = master.admin["$cmd"].find_one({"replSetGetStatus": 1})
            ismaster, replstatus = yield defer.gatherResults([ismaster_req, replstatus_req])

            initialized = replstatus["ok"]
            startup = any(m["stateStr"].startswith("STARTUP") for m in replstatus.get("members", []))
            ready = initialized and ismaster["ismaster"] and not startup

            if ready:
                break

        if not ready:
            yield self.tearDown()
            raise Exception("ReplicaSet initialization took more than 60s")

        yield master.disconnect()


    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.gatherResults([mongo.stop() for mongo in self.__mongod])


    @defer.inlineCallbacks
    def test_WriteToMaster(self):
        conn = MongoConnection("localhost", self.ports[0])
        try:
            coll = conn.db.coll
            yield coll.insert({'x': 42}, safe=True)
            result = yield coll.find_one()
            self.assertEqual(result['x'], 42)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_SlaveOk(self):
        uri = "mongodb://localhost:{0}/?readPreference=secondaryPreferred".format(self.ports[1])
        conn = ConnectionPool(uri)
        try:
            empty = yield conn.db.coll.find(flags=QUERY_SLAVE_OK)
            self.assertEqual(empty, [])

            yield self.assertFailure(conn.db.coll.insert({'x': 42}), OperationFailure)
        finally:
            yield conn.disconnect()


    @defer.inlineCallbacks
    def test_SwitchToMasterOnConnect(self):
        # Reverse hosts order
        try:
            conn = MongoConnection("localhost", self.ports[1])
            result = yield conn.db.coll.find({'x': 42})
            self.assertEqual(result, [])
        finally:
            yield conn.disconnect()

        # txmongo will do log.err() for AutoReconnects
        self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_AutoReconnect(self):
        self.patch(_Connection, 'maxDelay', 5)

        try:
            uri = "mongodb://localhost:{0}/?w={1}".format(self.ports[0], len(self.ports))
            conn = ConnectionPool(uri)

            yield conn.db.coll.insert({'x': 42}, safe = True)

            yield self.__mongod[0].stop()

            try:
                result = yield conn.db.coll.find_one()
            except AutoReconnect:
                result = yield conn.db.coll.find_one()

            self.assertEqual(result['x'], 42)
        finally:
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)
