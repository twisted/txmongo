# Copyright 2009-2015 The TxMongo Developers. All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Tuple

from bson.binary import UuidRepresentation
from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions
from pymongo.common import validate_is_mapping
from pymongo.errors import AutoReconnect, ConfigurationError, OperationFailure
from pymongo.read_preferences import ReadPreference
from pymongo.uri_parser import parse_uri
from pymongo.write_concern import WriteConcern
from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.python import log

from txmongo.database import Database
from txmongo.protocol import MongoProtocol, Msg
from txmongo.sessions import ClientSession, ServerSession, SessionOptions
from txmongo.types import Document
from txmongo.utils import check_deadline, get_err, timeout

_PRIMARY_READ_PREFERENCES = {
    ReadPreference.PRIMARY.mode,
    ReadPreference.PRIMARY_PREFERRED.mode,
}


class _Connection(ReconnectingClientFactory):
    __notify_ready = None
    __allnodes = None
    __index = -1
    __uri = None

    instance = None
    protocol = MongoProtocol

    def __init__(self, pool, uri, connection_id, initial_delay, max_delay):
        self.__allnodes = list(uri["nodelist"])
        self.__notify_ready = []
        self.__pool = pool
        self.__uri = uri
        self.connection_id = connection_id
        self.initialDelay = initial_delay
        self.maxDelay = max_delay

    @property
    def pool(self):
        return self.__pool

    def buildProtocol(self, addr):
        # Build the protocol.
        p = super().buildProtocol(addr)
        self._initializeProto(p)
        return p

    @defer.inlineCallbacks
    def _initializeProto(self, proto):
        yield proto.connectionReady()
        self.resetDelay()

        uri_options = self.uri["options"]
        slaveok = uri_options.get("slaveok", False)
        if "readpreference" in uri_options:
            slaveok = uri_options["readpreference"] not in _PRIMARY_READ_PREFERENCES

        try:
            if not slaveok:
                # Update our server configuration. This may disconnect if the node
                # is not a master.
                yield self.configure(proto)

            yield self._auth_proto(proto)
            self.setInstance(instance=proto)
        except Exception as e:
            proto.fail(e)

    @staticmethod
    @timeout
    def __send_ismaster(proto, _deadline) -> defer.Deferred[dict]:
        return proto.send_op_query_command("admin", {"ismaster": 1})

    @defer.inlineCallbacks
    def configure(self, proto: MongoProtocol):
        """
        Configures the protocol using the information gathered from the
        remote Mongo instance. Such information may contain the max
        BSON document size, replica set configuration, and the master
        status of the instance.
        """

        if not proto:
            return None

        # Handle the reply from the "ismaster" query. The reply contains
        # configuration information about the peer.
        config = yield self.__send_ismaster(proto, timeout=self.initialDelay)

        # Make sure the command was successful.
        if not config.get("ok"):
            code = config.get("code")
            msg = "TxMongo: " + get_err(config, "Unknown error")
            raise OperationFailure(msg, code)

        # Check that the replicaSet matches.
        set_name = config.get("setName")
        expected_set_name = self.uri["options"].get("replicaset")
        if expected_set_name and (expected_set_name != set_name):
            # Log the invalid replica set failure.
            msg = "TxMongo: Mongo instance does not match requested replicaSet."
            raise ConfigurationError(msg)

        proto.init_from_hello_response(config)
        if config.get("logicalSessionTimeoutMinutes"):
            self.__pool._logical_session_timeout_minutes = min(
                config["logicalSessionTimeoutMinutes"],
                self.__pool._logical_session_timeout_minutes,
            )

        # Track the other hosts in the replica set.
        hosts = config.get("hosts")
        if isinstance(hosts, list) and hosts:
            for host in hosts:
                if ":" not in host:
                    host = (host, 27017)
                else:
                    host = host.split(":", 1)
                    host[1] = int(host[1])
                    host = tuple(host)
                if host not in self.__allnodes:
                    self.__allnodes.append(host)

        # Check if this node is the master.
        ismaster = config.get("ismaster")
        if not ismaster:
            msg = "TxMongo: MongoDB host `%s` is not master." % config.get("me")
            raise AutoReconnect(msg)

    def clientConnectionFailed(self, connector, reason):
        self.instance = None
        if self.continueTrying:
            self.connector = connector
            self.retryNextHost()

    def clientConnectionLost(self, connector, reason):
        self.instance = None
        if self.continueTrying:
            self.connector = connector
            self.retryNextHost()

    def notifyReady(self):
        """
        Returns a deferred that will fire when the factory has created a
        protocol that can be used to communicate with a Mongo server.

        Note that this will not fire until we have connected to a Mongo
        master, unless slaveOk was specified in the Mongo URI connection
        options.
        """
        if self.instance:
            return defer.succeed(self.instance)

        def on_cancel(d):
            self.__notify_ready.remove(d)

        df = defer.Deferred(on_cancel)
        self.__notify_ready.append(df)
        return df

    def retryNextHost(self, connector=None):
        """
        Have this connector connect again, to the next host in the
        configured list of hosts.
        """
        if not self.continueTrying:
            msg = "TxMongo: Abandoning {0} on explicit request.".format(connector)
            log.msg(msg)
            return

        if connector is None:
            if self.connector is None:
                raise ValueError("TxMongo: No additional connector to retry.")
            else:
                connector = self.connector

        delay = False
        self.__index += 1

        if self.__index >= len(self.__allnodes):
            self.__index = 0
            delay = True

        connector.host, connector.port = self.__allnodes[self.__index]

        if delay:
            self.retry(connector)
        else:
            connector.connect()

    def setInstance(self, instance=None, reason=None):
        if instance == self.instance:
            # Should not fail deferreds from __notify_ready if setInstance(None)
            # called when instance is already None
            return
        self.instance = instance
        deferreds, self.__notify_ready = self.__notify_ready, []
        if deferreds:
            for df in deferreds:
                if instance:
                    df.callback(self)
                else:
                    df.errback(reason)

    @property
    def uri(self):
        return self.__uri

    def _auth_proto(self, proto):
        return defer.DeferredList(
            [
                proto.authenticate(database, username, password, mechanism)
                for database, (
                    username,
                    password,
                    mechanism,
                ) in self.__pool.auth_creds.items()
            ],
            consumeErrors=True,
        )

    def authenticate(self, database, username, password, mechanism):
        if self.instance:
            return self.instance.authenticate(database, username, password, mechanism)
        else:
            return defer.succeed(None)


class ConnectionPool:
    __index = 0
    __pool = None
    __pool_size = None
    __uri = None

    __auth_creds: Dict[str, Tuple[str, str, str]]

    __pinger_discovery_interval = 10

    __server_sessions_cache: deque[ServerSession]
    _logical_session_timeout_minutes = 30

    _cluster_time: Optional[Document] = None

    def __init__(
        self,
        uri="mongodb://127.0.0.1:27017",
        pool_size=1,
        ssl_context_factory=None,
        ping_interval=10,
        ping_timeout=10,
        **kwargs,
    ):
        assert isinstance(uri, str)
        assert isinstance(pool_size, int)
        assert pool_size >= 1

        self.__server_sessions_cache = deque()
        self.__auth_creds = {}

        if not uri.startswith("mongodb://") and not uri.startswith("mongodb+srv://"):
            uri = "mongodb://" + uri

        self.__uri = parse_uri(uri)

        wc_options = {**self.__uri["options"], **kwargs}
        self.__write_concern = self.__parse_write_concern_options(wc_options)

        self.__codec_options = kwargs.get(
            "codec_options",
            CodecOptions(uuid_representation=UuidRepresentation.STANDARD),
        )

        retry_delay = kwargs.get("retry_delay", 1.0)
        max_delay = kwargs.get("max_delay", 60.0)
        self.__pool_size = pool_size
        self.__pool = [
            _Connection(self, self.__uri, i, retry_delay, max_delay)
            for i in range(pool_size)
        ]

        if self.__uri["database"] and self.__uri["username"] and self.__uri["password"]:
            auth_db = self.__uri["options"].get("authsource") or self.__uri["database"]
            self.authenticate(
                auth_db,
                self.__uri["username"],
                self.__uri["password"],
                self.__uri["options"].get("authmechanism", "DEFAULT"),
            )

        host, port = self.__uri["nodelist"][0]

        self.ssl_context_factory = ssl_context_factory

        initial_delay = kwargs.get("retry_delay", 30)
        for factory in self.__pool:
            factory.connector = self.__tcp_or_ssl_connect(
                host, port, factory, timeout=initial_delay
            )

        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.__pingers = {}
        self.__pinger_discovery = task.LoopingCall(self.__discovery_nodes_to_ping)
        self.__pinger_discovery.start(self.__pinger_discovery_interval, now=False)

    @staticmethod
    def __parse_write_concern_options(options):
        concern = options.get("w")
        wtimeout = options.get("wtimeout", options.get("wtimeoutms"))
        j = options.get("j", options.get("journal"))
        fsync = options.get("fsync")
        return WriteConcern(concern, wtimeout, j, fsync)

    def __tcp_or_ssl_connect(self, host, port, factory, **kwargs):
        if self.ssl_context_factory:
            return reactor.connectSSL(
                host, port, factory, self.ssl_context_factory, **kwargs
            )
        else:
            return reactor.connectTCP(host, port, factory, **kwargs)

    @property
    def write_concern(self):
        return self.__write_concern

    @property
    def codec_options(self):
        return self.__codec_options

    def getprotocols(self):
        return self.__pool

    def __getitem__(self, name):
        return Database(self, name)

    def __getattr__(self, name):
        return self[name]

    def __repr__(self):
        if self.uri["nodelist"]:
            return "Connection(%r, %r)" % self.uri["nodelist"][0]
        return "Connection()"

    def get_default_database(self):
        if self.uri["database"]:
            return self[self.uri["database"]]
        else:
            return None

    def drop_database(self, name_or_database):
        if isinstance(name_or_database, (bytes, str)):
            db = self[name_or_database]
        elif isinstance(name_or_database, Database):
            db = name_or_database
        else:
            raise TypeError(
                "argument to drop_database() should be database name "
                "or database object"
            )

        return db.command("dropDatabase")

    def disconnect(self):
        if self.__pinger_discovery.running:
            self.__pinger_discovery.stop()
        for pinger in list(self.__pingers.values()):
            pinger.connector.disconnect()

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

    @property
    def auth_creds(self) -> Dict[str, Tuple[str, str, str]]:
        return self.__auth_creds

    def authenticate(self, database, username, password, mechanism="DEFAULT"):
        self.__auth_creds[str(database)] = (username, password, mechanism)

        def fail(failure):
            failure.trap(defer.FirstError)
            raise failure.value.subFailure.value

        return defer.gatherResults(
            [
                connection.authenticate(database, username, password, mechanism)
                for connection in self.__pool
            ],
            consumeErrors=True,
        ).addErrback(fail)

    def getprotocol(self):
        # Get the next protocol available for communication in the pool.
        connection = self.__pool[self.__index]
        self.__index = (self.__index + 1) % self.__pool_size

        # If the connection is already connected, just return it.
        if connection.instance:
            return defer.succeed(connection.instance)

        # Wait for the connection to connection.
        return connection.notifyReady().addCallback(lambda conn: conn.instance)

    @property
    def uri(self):
        return self.__uri

    @timeout
    def command(
        self,
        dbname: str,
        command: Document,
        *,
        allowable_errors=None,
        check=True,
        codec_options: CodecOptions = None,
        write_concern: WriteConcern = None,
        session: ClientSession = None,
        _deadline=None,
    ):
        return defer.ensureDeferred(
            self._command(
                dbname,
                command,
                allowable_errors=allowable_errors,
                check=check,
                codec_options=codec_options,
                write_concern=write_concern,
                session=session,
                _deadline=_deadline,
            )
        )

    async def _command(
        self,
        dbname: str,
        command: Document,
        *,
        allowable_errors=None,
        check=True,
        codec_options: CodecOptions = None,
        write_concern: WriteConcern = None,
        session: ClientSession = None,
        _deadline=None,
    ):
        validate_is_mapping("command", command)
        command["$db"] = dbname
        if codec_options is None:
            codec_options = self.codec_options
        if write_concern is None:
            write_concern = self.write_concern

        async with self._using_session(session, write_concern) as session:
            proto = await self.getprotocol()
            check_deadline(_deadline)

            try:
                reply = await proto.send_msg(
                    self._create_message(
                        session,
                        command,
                        codec_options=codec_options,
                        acknowledged=write_concern.acknowledged,
                    ),
                    codec_options,
                    session,
                    check=check,
                    allowable_errors=allowable_errors,
                )
            except OperationFailure as e:
                clean_command = {**command}
                clean_command.pop("$db", None)
                clean_command.pop("lsid", None)
                e.args = (
                    f"TxMongo: command {clean_command!r} on namespace {self} failed with '{e}'",
                    *e.args[1:],
                )
                raise e
            return reply

    # Pingers are persistent connections that are established to each
    # node of the replicaset to monitor their availability.
    #
    # Every `__pinger_discovery_interval` seconds ConnectionPool compares
    # actual nodes addresses and starts/stops Pingers to ensure that
    # Pinger is started for every node address.
    #
    # Every `ping_interval` seconds pingers send ismaster commands.
    #
    # All pool connections to corresponding TCP address are dropped
    # if one of following happens:
    #   1. Pinger is unable to receive response to ismaster within
    #      `ping_timeout` seconds
    #   2. Pinger is unable to connect to address within `ping_timeout`
    #      seconds
    #
    # If Pinger's connection is closed by server, pool connections are not
    # dropped. Next discovery procedure will recreate the Pinger.

    def __discovery_nodes_to_ping(self):
        existing = set(self.__pingers)
        peers = {
            conn.instance.transport.getPeer() for conn in self.__pool if conn.instance
        }

        for peer in peers - existing:
            pinger = _Pinger(
                self.ping_interval,
                self.ping_timeout,
                self.__on_ping_lost,
                self.__on_ping_fail,
            )
            pinger.connector = self.__tcp_or_ssl_connect(
                peer.host, peer.port, pinger, timeout=self.ping_timeout
            )
            self.__pingers[peer] = pinger

        for unused_peer in existing - peers:
            self.__pingers[unused_peer].connector.disconnect()
            self.__pingers.pop(unused_peer, None)

    def __on_ping_lost(self, addr):
        if addr in self.__pingers:
            self.__pingers[addr].connector.disconnect()
            self.__pingers.pop(addr, None)

    def __on_ping_fail(self, addr):
        # Kill all pool connections to this addr
        for connection in self.__pool:
            if connection.instance and connection.instance.transport.getPeer() == addr:
                connection.instance.transport.abortConnection()

        self.__on_ping_lost(addr)

    def _acquire_server_session(self) -> ServerSession:
        while True:
            try:
                session = self.__server_sessions_cache.popleft()
            except IndexError:
                return ServerSession.create_with_local_id()

            if not session.is_about_to_expire(self._logical_session_timeout_minutes):
                return session

    def _return_server_session(self, server_session: ServerSession) -> None:
        while self.__server_sessions_cache:
            last = self.__server_sessions_cache[-1]
            if last.is_about_to_expire(self._logical_session_timeout_minutes):
                self.__server_sessions_cache.pop()
            else:
                break

        if server_session.is_dirty:
            return
        if server_session.is_about_to_expire(self._logical_session_timeout_minutes):
            return
        self.__server_sessions_cache.appendleft(server_session)

    def start_session(self, options: SessionOptions = None) -> ClientSession:
        if len(self.__auth_creds) > 1:
            raise ValueError(
                "TxMongo: Cannot use sessions when multiple users are authenticated"
            )
        return ClientSession(self, options, implicit=False)

    def _get_implicit_session(self) -> Optional[ClientSession]:
        """May return None if multiple users are authenticated"""
        if len(self.__auth_creds) > 1:
            return None
        return ClientSession(self, None, implicit=True)

    @property
    def cluster_time(self) -> Optional[Document]:
        return self._cluster_time

    def _create_message(
        self,
        session: Optional[ClientSession],
        body: Document,
        payload: Dict[str, List[Document]] = None,
        *,
        codec_options: CodecOptions = DEFAULT_CODEC_OPTIONS,
        acknowledged: bool = True,
    ) -> Msg:
        """Create Msg instance adding Cluster Time gossiping and session id. Mutates the body argument!"""
        if cluster_time := self._get_cluster_time(session):
            body["$clusterTime"] = cluster_time
        if session:
            body["lsid"] = session._use_session_id()
        return Msg.create(
            body,
            payload,
            codec_options=codec_options,
            acknowledged=acknowledged,
        )

    @asynccontextmanager
    async def _using_session(
        self, session: Optional[ClientSession], write_concern: WriteConcern
    ) -> Optional[ClientSession]:
        if session is not None and not write_concern.acknowledged:
            raise ValueError(
                "TxMongo: Cannot use unacknowledged write concern with an explicit session"
            )
        if session and len(self.__auth_creds) > 1:
            raise ValueError(
                "TxMongo: Cannot use sessions when multiple users are authenticated"
            )
        if session is None and write_concern.acknowledged:
            session = self._get_implicit_session()
        try:
            yield session
        finally:
            if session and session.implicit:
                await session.end_session()

    def _get_cluster_time(self, session: Optional[ClientSession]) -> Optional[Document]:
        session_ct = session.cluster_time if session else None
        if session_ct is None:
            return self.cluster_time
        if self.cluster_time is None:
            return session_ct
        if session_ct["clusterTime"] > self.cluster_time["clusterTime"]:
            return session_ct
        return self.cluster_time

    def _advance_cluster_time(
        self, session: Optional[ClientSession], reply: Document
    ) -> None:
        cluster_time = reply.get("$clusterTime")
        if cluster_time is None:
            return
        if session:
            session.advance_cluster_time(cluster_time)
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif self._cluster_time["clusterTime"] < cluster_time["clusterTime"]:
            self._cluster_time = cluster_time


class _PingerProtocol(MongoProtocol):

    __next_call = None

    def __init__(self, interval, timeout, fail_callback):
        MongoProtocol.__init__(self)
        self.interval = interval
        self.timeout = timeout
        self.fail_callback = fail_callback

    def ping(self):
        def on_ok(result):
            if timeout_call.active():
                timeout_call.cancel()
                self.__next_call = reactor.callLater(self.interval, self.ping)

        def on_fail(failure):
            if timeout_call.active():
                timeout_call.cancel()
                on_timeout()

        def on_timeout():
            self.transport.loseConnection()
            self.fail_callback(self.transport.getPeer())

        timeout_call = reactor.callLater(self.timeout, on_timeout)

        self.send_op_query_command("admin", {"ismaster": 1}).addCallbacks(
            on_ok, on_fail
        )

    def connectionMade(self):
        MongoProtocol.connectionMade(self)
        self.ping()

    def connectionLost(self, reason):
        MongoProtocol.connectionLost(self, reason)
        if self.__next_call and self.__next_call.active():
            self.__next_call.cancel()


class _Pinger(ClientFactory):

    def __init__(self, interval, timeout, lost_callback, fail_callback):
        self.interval = interval
        self.timeout = timeout
        self.lost_callback = lost_callback
        self.fail_callback = fail_callback

    def buildProtocol(self, addr):
        proto = _PingerProtocol(self.interval, self.timeout, self.fail_callback)
        proto.factory = self
        return proto

    def setInstance(self, instance=None, reason=None):
        pass

    def clientConnectionLost(self, connector, reason):
        self.lost_callback(connector.getDestination())

    def clientConnectionFailed(self, connector, reason):
        self.fail_callback(connector.getDestination())


###
# Begin Legacy Wrapper
###


class MongoConnection(ConnectionPool):
    def __init__(self, host="127.0.0.1", port=27017, pool_size=1, **kwargs):
        uri = "mongodb://%s:%d/" % (host, port)
        ConnectionPool.__init__(self, uri, pool_size=pool_size, **kwargs)


lazyMongoConnectionPool = MongoConnection
lazyMongoConnection = MongoConnection
MongoConnectionPool = MongoConnection

###
# End Legacy Wrapper
###
