# coding: utf-8
# Copyright 2015 Ilya Skriblovsky <ilyaskriblovsky@gmail.com>
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

from __future__ import absolute_import, division

from bson import SON
from pymongo.errors import OperationFailure, AutoReconnect, ConfigurationError
from time import time
from twisted.trial import unittest
from twisted.internet import defer, reactor
from txmongo.connection import MongoConnection, ConnectionPool, _Connection
from txmongo.errors import TimeExceeded
from txmongo.protocol import QUERY_SLAVE_OK, MongoProtocol

from .mongod import Mongod


class TestReplicaSet(unittest.TestCase):

    ports = [37017, 37018, 37019]
    rsname = "rs1"

    rsconfig = {
        "_id": rsname,
        "members": [
            {"_id": i, "host": "localhost:{0}".format(port)}
            for i, port in enumerate(ports)
        ]
    }
    # We assume first member to be master
    rsconfig["members"][0]["priority"] = 2

    __init_timeout = 60
    __ping_interval = 0.5

    def __sleep(self, delay):
        d = defer.Deferred()
        reactor.callLater(delay, d.callback, None)
        return d

    @defer.inlineCallbacks
    def __check_reachable(self, port):
        uri = "mongodb://localhost:{0}/?readPreference=secondaryPreferred".format(port)
        conn = ConnectionPool(uri)
        yield conn.admin.command("ismaster", check=False)
        yield conn.disconnect()

    @defer.inlineCallbacks
    def setUp(self):
        self.__mongod = [Mongod(port=p, replset=self.rsname) for p in self.ports]
        yield defer.gatherResults([mongo.start() for mongo in self.__mongod])

        yield defer.gatherResults([self.__check_reachable(port) for port in self.ports])

        master_uri = "mongodb://localhost:{0}/?readPreference=secondaryPreferred".format(self.ports[0])
        master = ConnectionPool(master_uri)
        yield master.admin.command("replSetInitiate", self.rsconfig)

        ready = False
        n_tries = int(self.__init_timeout / self.__ping_interval)
        for i in range(n_tries):
            yield self.__sleep(self.__ping_interval)

            # My practice shows that we need to query both ismaster and replSetGetStatus
            # to be sure that replica set is up and running, primary is elected and all
            # secondaries are in sync and ready to became new primary

            ismaster_req = master.admin.command("ismaster", check=False)
            replstatus_req = master.admin.command("replSetGetStatus", check=False)
            ismaster, replstatus = yield defer.gatherResults([ismaster_req, replstatus_req])

            initialized = replstatus["ok"]
            ok_states = {"PRIMARY", "SECONDARY"}
            states_ready = all(m["stateStr"] in ok_states for m in replstatus.get("members", []))
            ready = initialized and ismaster["ismaster"] and states_ready

            if ready:
                break

        if not ready:
            yield self.tearDown()
            raise Exception("ReplicaSet initialization took more than {0}s".format(
                self.__init_timeout))

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

            yield conn.db.coll.insert({'x': 42}, safe=True)

            yield self.__mongod[0].stop()

            while True:
                try:
                    result = yield conn.db.coll.find_one()
                    self.assertEqual(result['x'], 42)
                    break
                except AutoReconnect:
                    pass

        finally:
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_AutoReconnect_from_primary_step_down(self):
        self.patch(_Connection, 'maxDelay', 5)
        uri = "mongodb://localhost:{0}/?w={1}".format(self.ports[0], len(self.ports))
        conn = ConnectionPool(uri)

        # this will force primary to step down, triggering an AutoReconnect that bubbles up
        # through the connection pool to the client
        command = conn.admin.command(SON([('replSetStepDown', 86400), ('force', 1)]))
        self.assertFailure(command, AutoReconnect)

        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_find_with_timeout(self):
        self.patch(_Connection, 'maxDelay', 5)

        try:
            uri = "mongodb://localhost:{0}/?w={1}".format(self.ports[0], len(self.ports))
            conn = ConnectionPool(uri, initial_delay=3)

            yield conn.db.coll.insert({'x': 42}, safe=True)

            yield self.__mongod[0].stop()

            while True:
                try:
                    yield conn.db.coll.find_one(timeout=2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_find_with_deadline(self):
        self.patch(_Connection, 'maxDelay', 5)

        try:
            uri = "mongodb://localhost:{0}/?w={1}".format(self.ports[0], len(self.ports))
            conn = ConnectionPool(uri, initial_delay=3)

            yield conn.db.coll.insert({'x': 42}, safe=True)

            yield self.__mongod[0].stop()

            while True:
                try:
                    yield conn.db.coll.find_one(deadline=time()+2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_TimeExceeded_insert(self):
        self.patch(_Connection, 'maxDelay', 5)

        try:
            uri = "mongodb://localhost:{0}/?w={1}".format(self.ports[0], len(self.ports))
            conn = ConnectionPool(uri, initial_delay=3)

            yield conn.db.coll.insert({'x': 42}, safe=True)

            yield self.__mongod[0].stop()

            while True:
                try:
                    yield conn.db.coll.insert({'y': 42}, safe=True, timeout=2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_InvalidRSName(self):
        uri = "mongodb://localhost:{0}/?replicaSet={1}_X".format(self.ports[0], self.rsname)

        ok = defer.Deferred()

        def proto_fail(self, exception):
            conn.disconnect()

            if type(exception) == ConfigurationError:
                ok.callback(None)
            else:
                ok.errback(exception)

        self.patch(MongoProtocol, "fail", proto_fail)

        conn = ConnectionPool(uri)

        @defer.inlineCallbacks
        def do_query():
            yield conn.db.coll.insert({'x': 42})
            raise Exception("You shall not pass!")

        yield defer.DeferredList([ok, do_query()],
                                 fireOnOneCallback=True,
                                 fireOnOneErrback=True)
        self.flushLoggedErrors(AutoReconnect)
