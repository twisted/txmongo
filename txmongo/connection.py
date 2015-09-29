# Copyright 2009-2015 The TxMongo Developers. All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import absolute_import, division

from pymongo.errors import AutoReconnect, ConfigurationError, OperationFailure
from pymongo.uri_parser import parse_uri
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.python import log
from twisted.python.compat import StringType
from txmongo.database import Database
from txmongo.protocol import MongoProtocol, Query


_PRIMARY_READ_PREFERENCES = {ReadPreference.PRIMARY.mode, ReadPreference.PRIMARY_PREFERRED.mode}


class _Connection(ReconnectingClientFactory):
    __notify_ready = None
    __allnodes = None
    __index = -1
    __uri = None
    __conf_loop = None
    __conf_loop_seconds = 300.0

    instance = None
    protocol = MongoProtocol
    maxDelay = 60

    def __init__(self, pool, uri, id, initial_delay):
        self.__allnodes = list(uri["nodelist"])
        self.__notify_ready = []
        self.__pool = pool
        self.__uri = uri
        self.__conf_loop = task.LoopingCall(lambda: self.configure(self.instance))
        self.__conf_loop.start(self.__conf_loop_seconds, now=False)
        self.connectionid = id
        self.initialDelay = initial_delay

        self.__auth_creds = {}

    def buildProtocol(self, addr):
        # Build the protocol.
        p = ReconnectingClientFactory.buildProtocol(self, addr)
        self._initializeProto(p)
        return p

    @defer.inlineCallbacks
    def _initializeProto(self, proto):
        yield proto.connectionReady()
        self.resetDelay()

        uri_options = self.uri['options']
        slaveok = uri_options.get('slaveok', False)
        if 'readpreference' in uri_options:
            slaveok = uri_options['readpreference'] not in _PRIMARY_READ_PREFERENCES

        try:
            if not slaveok:
                # Update our server configuration. This may disconnect if the node
                # is not a master.
                yield self.configure(proto)

            yield self._auth_proto(proto)
            self.setInstance(instance=proto)
        except Exception as e:
            proto.fail(e)

    @defer.inlineCallbacks
    def configure(self, proto):
        """
            Configures the protocol using the information gathered from the
            remote Mongo instance. Such information may contain the max
            BSON document size, replica set configuration, and the master
            status of the instance.
            """

        if not proto:
            defer.returnValue(None)

        query = Query(collection="admin.$cmd", query={"ismaster": 1})
        reply = yield proto.send_QUERY(query)

        # Handle the reply from the "ismaster" query. The reply contains
        # configuration information about the peer.

        # Make sure we got a result document.
        if len(reply.documents) != 1:
            raise OperationFailure("Invalid document length.")

        # Get the configuration document from the reply.
        config = reply.documents[0].decode()

        # Make sure the command was successful.
        if not config.get("ok"):
            code = config.get("code")
            msg = config.get("err", "Unknown error")
            raise OperationFailure(msg, code)

        # Check that the replicaSet matches.
        set_name = config.get("setName")
        expected_set_name = self.uri["options"].get("replicaset")
        if expected_set_name and (expected_set_name != set_name):
            # Log the invalid replica set failure.
            msg = "Mongo instance does not match requested replicaSet."
            raise ConfigurationError(msg)

        # Track max bson object size limit.
        max_bson_size = config.get("maxBsonObjectSize")
        if max_bson_size:
            proto.max_bson_size = max_bson_size

        proto.set_wire_versions(config.get("minWireVersion", 0),
                                config.get("maxWireVersion", 0))

        # Track the other hosts in the replica set.
        hosts = config.get("hosts")
        if isinstance(hosts, list) and hosts:
            for host in hosts:
                if ':' not in host:
                    host = (host, 27017)
                else:
                    host = host.split(':', 1)
                    host[1] = int(host[1])
                    host = tuple(host)
                if host not in self.__allnodes:
                    self.__allnodes.append(host)

        # Check if this node is the master.
        ismaster = config.get("ismaster")
        if not ismaster:
            raise AutoReconnect("MongoDB host `%s` is not master." % config.get('me'))

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
        df = defer.Deferred()
        self.__notify_ready.append(df)
        return df

    def retryNextHost(self, connector=None):
        """
            Have this connector connect again, to the next host in the
            configured list of hosts.
            """
        if not self.continueTrying:
            log.msg("Abandoning %s on explicit request" % (connector,))
            return

        if connector is None:
            if self.connector is None:
                raise ValueError("no connector to retry")
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

    def stopTrying(self):
        ReconnectingClientFactory.stopTrying(self)
        self.__conf_loop.stop()

    @property
    def uri(self):
        return self.__uri

    @defer.inlineCallbacks
    def _auth_proto(self, proto):
        yield defer.DeferredList(
            [proto.authenticate(database, username, password, mechanism)
             for database, (username, password, mechanism) in self.__auth_creds.items()],
            consumeErrors=True
        )

    def authenticate(self, database, username, password, mechanism):
        self.__auth_creds[str(database)] = (username, password, mechanism)

        if self.instance:
            return self.instance.authenticate(database, username, password, mechanism)
        else:
            return defer.succeed(None)


class ConnectionPool(object):
    __index = 0
    __pool = None
    __pool_size = None
    __uri = None

    __wc_possible_options = {'w', "wtimeout", 'j', "fsync"}

    def __init__(self, uri="mongodb://127.0.0.1:27017", pool_size=1, ssl_context_factory=None,
                 **kwargs):
        assert isinstance(uri, StringType)
        assert isinstance(pool_size, int)
        assert pool_size >= 1

        if not uri.startswith("mongodb://"):
            uri = "mongodb://" + uri

        self.__uri = parse_uri(uri)

        wc_options = self.__uri['options'].copy()
        wc_options.update(kwargs)
        wc_options = dict((k, v) for k, v in wc_options.items() if k in self.__wc_possible_options)
        self.__write_concern = WriteConcern(**wc_options)

        retry_delay = kwargs.get('retry_delay', 1.0)
        self.__pool_size = pool_size
        self.__pool = [_Connection(self, self.__uri, i, retry_delay) for i in range(pool_size)]

        if self.__uri['database'] and self.__uri['username'] and self.__uri['password']:
            self.authenticate(self.__uri['database'], self.__uri['username'],
                              self.__uri['password'],
                              self.__uri['options'].get('authmechanism', 'DEFAULT'))

        host, port = self.__uri['nodelist'][0]

        initial_delay = kwargs.get('retry_delay', 30)
        for factory in self.__pool:
            if ssl_context_factory:
                factory.connector = reactor.connectSSL(
                    host, port, factory, ssl_context_factory, initial_delay)
            else:
                factory.connector = reactor.connectTCP(host, port, factory, initial_delay)

    @property
    def write_concern(self):
        return self.__write_concern

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

    @defer.inlineCallbacks
    def authenticate(self, database, username, password, mechanism="DEFAULT"):
        try:
            yield defer.gatherResults(
                [connection.authenticate(database, username, password, mechanism)
                 for connection in self.__pool],
                consumeErrors=True
            )
        except defer.FirstError as e:
            raise e.subFailure.value

    @defer.inlineCallbacks
    def getprotocol(self):
        # Get the next protocol available for communication in the pool.
        connection = self.__pool[self.__index]
        self.__index = (self.__index + 1) % self.__pool_size

        # If the connection is already connected, just return it.
        if connection.instance:
            defer.returnValue(connection.instance)

        # Wait for the connection to connection.
        yield connection.notifyReady()

        defer.returnValue(connection.instance)

    @property
    def uri(self):
        return self.__uri


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
