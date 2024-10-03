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

import signal
from time import time

from bson import SON
from pymongo.errors import AutoReconnect, ConfigurationError, OperationFailure
from twisted.internet import defer, reactor
from twisted.trial import unittest

from tests.conf import MongoConf
from tests.mongod import create_mongod
from txmongo.connection import ConnectionPool
from txmongo.errors import TimeExceeded
from txmongo.protocol import QUERY_SLAVE_OK, MongoProtocol


class TestReplicaSet(unittest.TestCase):

    conf = MongoConf()

    @property
    def ports(self) -> list:
        return self.conf.replicase_test_ports

    rs_name = "rs1"

    @property
    def rs_config(self):
        return {
            "_id": self.rs_name,
            "members": [
                {
                    "_id": i,
                    "host": (
                        f"{mongo.container_name}:27017"
                        if self.conf.run_in_docker
                        else f"localhost:{mongo.port}"
                    ),
                    # We assume first member to be master
                    "priority": 2 if i == 0 else 1,
                }
                for i, mongo in enumerate(self.__mongod)
            ],
        }

    __init_timeout = 60
    __ping_interval = 0.5

    def __sleep(self, delay):
        d = defer.Deferred()
        reactor.callLater(delay, d.callback, None)
        return d

    @defer.inlineCallbacks
    def __check_reachable(self, port):
        uri = f"mongodb://localhost:{port}/?readPreference=secondaryPreferred"
        conn = ConnectionPool(uri)
        yield conn.admin.command("ismaster", check=False)
        yield conn.disconnect()

    @property
    def master_uri(self) -> str:
        return f"mongodb://localhost:{self.ports[0]}"

    @property
    def master_uri_with_secondary(self) -> str:
        return f"{self.master_uri}/?readPreference=secondaryPreferred"

    @property
    def master_with_guaranteed_write(self) -> str:
        """
        success write, when every node wrote data
        """
        return f"{self.master_uri}/?w={len(self.ports)}"

    @property
    def secondary_first_schema(self) -> str:
        """
        for docker need schema because in rs_config we have internal ports,
        but python process would connect to external.
        """
        return (
            f"mongodb://localhost:{self.ports[1]},"
            f"localhost:{self.ports[0]},"
            f"localhost:{self.ports[2]}"
        )

    @defer.inlineCallbacks
    def setUp(self):
        self.__mongod = [
            create_mongod(port=p, replset=self.rs_name) for p in self.ports
        ]
        yield defer.gatherResults([mongo.start() for mongo in self.__mongod])

        yield defer.gatherResults([self.__check_reachable(port) for port in self.ports])
        master = ConnectionPool(self.master_uri_with_secondary)
        yield master.admin.command("replSetInitiate", self.rs_config)

        ready = False
        n_tries = int(self.__init_timeout / self.__ping_interval)
        for i in range(n_tries):
            yield self.__sleep(self.__ping_interval)

            # My practice shows that we need to query both ismaster and replSetGetStatus
            # to be sure that replica set is up and running, primary is elected and all
            # secondaries are in sync and ready to became new primary

            ismaster_req = master.admin.command("ismaster", check=False)
            replstatus_req = master.admin.command("replSetGetStatus", check=False)
            ismaster, replstatus = yield defer.gatherResults(
                [ismaster_req, replstatus_req]
            )

            initialized = replstatus["ok"]
            ok_states = {"PRIMARY", "SECONDARY"}
            states_ready = all(
                m["stateStr"] in ok_states for m in replstatus.get("members", [])
            )
            ready = initialized and ismaster["ismaster"] and states_ready

            if ready:
                break

        if not ready:
            yield self.tearDown()
            raise Exception(
                f"ReplicaSet initialization took more than {self.__init_timeout}s"
            )

        yield master.disconnect()

    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.gatherResults([mongo.stop() for mongo in self.__mongod])

    @defer.inlineCallbacks
    def test_WriteToMaster(self):
        conn = ConnectionPool(self.master_uri)
        try:
            coll = conn.db.coll
            yield coll.insert_one({"x": 42})
            result = yield coll.find_one()
            self.assertEqual(result["x"], 42)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_SlaveOk(self):
        conn = ConnectionPool(
            f"mongodb://localhost:{self.ports[1]}/?readPreference=secondaryPreferred"
        )
        try:
            empty = yield conn.db.coll.find(flags=QUERY_SLAVE_OK)
            self.assertEqual(empty, [])

            server_status = yield conn.admin.command("serverStatus")
            _version = [int(part) for part in server_status["version"].split(".")]

            yield self.assertFailure(conn.db.coll.insert_one({"x": 42}), AutoReconnect)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_SwitchToMasterOnConnect(self):
        # Reverse hosts order
        try:
            conn = ConnectionPool(self.secondary_first_schema)
            result = yield conn.db.coll.find({"x": 42})
            self.assertEqual(result, [])
        finally:
            yield conn.disconnect()

        # txmongo will do log.err() for AutoReconnects
        self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_AutoReconnect(self):
        try:
            conn = ConnectionPool(self.master_with_guaranteed_write, max_delay=5)

            yield conn.db.coll.insert_one({"x": 42})

            self.__mongod[0].kill(signal.SIGSTOP)

            while True:
                try:
                    result = yield conn.db.coll.find_one()
                    self.assertEqual(result["x"], 42)
                    break
                except AutoReconnect:
                    pass

        finally:
            self.__mongod[0].kill(signal.SIGCONT)
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_AutoReconnect_from_primary_step_down(self):
        conn = ConnectionPool(self.master_with_guaranteed_write, max_delay=5)

        # this will force primary to step down, triggering an AutoReconnect that bubbles up
        # through the connection pool to the client
        command = conn.admin.command(SON([("replSetStepDown", 86400), ("force", 1)]))
        self.assertFailure(command, AutoReconnect)

        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_find_with_timeout(self):
        try:
            conn = ConnectionPool(
                self.master_with_guaranteed_write, retry_delay=3, max_delay=5
            )

            yield conn.db.coll.insert_one({"x": 42})

            yield self.__mongod[0].kill(signal.SIGSTOP)
            yield self.__sleep(0.2)

            while True:
                try:
                    yield conn.db.coll.find_one(timeout=2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield self.__mongod[0].kill(signal.SIGCONT)
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_find_with_deadline(self):
        try:
            conn = ConnectionPool(
                self.master_with_guaranteed_write, retry_delay=3, max_delay=5
            )

            yield conn.db.coll.insert_one({"x": 42})

            yield self.__mongod[0].kill(signal.SIGSTOP)
            yield self.__sleep(0.2)

            while True:
                try:
                    yield conn.db.coll.find_one(deadline=time() + 2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield self.__mongod[0].kill(signal.SIGCONT)
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_TimeExceeded_insert(self):
        try:
            conn = ConnectionPool(
                self.master_with_guaranteed_write, retry_delay=3, max_delay=5
            )

            yield conn.db.coll.insert_one({"x": 42})

            yield self.__mongod[0].kill(signal.SIGSTOP)

            while True:
                try:
                    yield conn.db.coll.insert_one({"y": 42}, timeout=2)
                    self.fail("TimeExceeded not raised!")
                except TimeExceeded:
                    break  # this is what we should have returned
                except AutoReconnect:
                    pass

        finally:
            yield self.__mongod[0].kill(signal.SIGCONT)
            yield conn.disconnect()
            self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_InvalidRSName(self):
        ok = defer.Deferred()

        def proto_fail(self, exception):
            conn.disconnect()

            if type(exception) == ConfigurationError:
                ok.callback(None)
            else:
                ok.errback(exception)

        self.patch(MongoProtocol, "fail", proto_fail)

        conn = ConnectionPool(self.master_uri + f"/?replicaSet={self.rs_name}_X")

        @defer.inlineCallbacks
        def do_query():
            yield conn.db.coll.insert_one({"x": 42})
            raise Exception("You shall not pass!")

        yield defer.DeferredList(
            [ok, do_query()], fireOnOneCallback=True, fireOnOneErrback=True
        )
        self.flushLoggedErrors(AutoReconnect)

    @defer.inlineCallbacks
    def test_StaleConnection(self):
        conn = ConnectionPool(
            self.secondary_first_schema,
            ping_interval=5,
            ping_timeout=5,
        )
        try:
            yield conn.db.coll.count()
            # check that 5s pingers won't break connection if it is healthy
            yield self.__sleep(6)
            yield conn.db.coll.count()
            yield self.__mongod[0].kill(signal.SIGSTOP)
            yield self.__sleep(0.2)
            while True:
                try:
                    yield conn.db.coll.count()
                    break
                except AutoReconnect:
                    pass
        finally:
            self.__mongod[0].kill(signal.SIGCONT)
            yield conn.disconnect()
