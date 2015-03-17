# Copyright 2009-2015 The TxMongo Developers. All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from pymongo.errors import AutoReconnect, ConfigurationError, OperationFailure
from pymongo.uri_parser import parse_uri

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.python import log

from txmongo.database import Database
from txmongo.protocol import MongoProtocol, Query


class _Connection(ReconnectingClientFactory):
    __notify_ready = None
    __discovered = None
    __index = -1
    __uri = None
    __conf_loop = None
    __conf_loop_seconds = 300.0

    instance = None
    protocol = MongoProtocol
    maxDelay = 60

    def __init__(self, pool, uri, id):
        self.__discovered = []
        self.__notify_ready = []
        self.__pool = pool
        self.__uri = uri
        self.__conf_loop = task.LoopingCall(lambda: self.configure(self.instance))
        self.__conf_loop.start(self.__conf_loop_seconds, now=False)
        self.connectionid = id

        self.__auth_creds = {}

    def buildProtocol(self, addr):
        # Build the protocol.
        p = ReconnectingClientFactory.buildProtocol(self, addr)

        ready_deferred = p.connectionReady()

        if not self.uri['options'].get('slaveok', False):
            # Update our server configuration. This may disconnect if the node
            # is not a master.
            ready_deferred.addCallback(lambda _: self.configure(p))

        ready_deferred\
            .addCallback(lambda _: self._auth_proto(p))\
            .addBoth(lambda _: self.setInstance(instance=p))
        return p

    def configure(self, proto):
        """
            Configures the protocol using the information gathered from the
            remote Mongo instance. Such information may contain the max
            BSON document size, replica set configuration, and the master
            status of the instance.
            """
        if proto:
            query = Query(collection="admin.$cmd", query={"ismaster": 1})
            df = proto.send_QUERY(query)
            df.addCallback(self._configureCallback, proto)
            return df
        return defer.succeed(None)

    def _configureCallback(self, reply, proto):
        """
            Handle the reply from the "ismaster" query. The reply contains
            configuration information about the peer.
            """
        # Make sure we got a result document.
        if len(reply.documents) != 1:
            proto.fail(OperationFailure("Invalid document length."))
            return

        # Get the configuration document from the reply.
        config = reply.documents[0].decode()

        # Make sure the command was successful.
        if not config.get("ok"):
            code = config.get("code")
            msg = config.get("err", "Unknown error")
            proto.fail(OperationFailure(msg, code))
            return

        # Check that the replicaSet matches.
        set_name = config.get("setName")
        expected_set_name = self.uri["options"].get("setname")
        if expected_set_name and (expected_set_name != set_name):
            # Log the invalid replica set failure.
            msg = "Mongo instance does not match requested replicaSet."
            reason = ConfigurationError(msg)
            proto.fail(reason)
            return

        # Track max bson object size limit.
        max_bson_size = config.get("maxBsonObjectSize")
        if max_bson_size:
            proto.max_bson_size = max_bson_size

        proto.set_wire_versions(config.get("minWireVersion", 0),
                                config.get("maxWireVersion", 0))

        # Track the other hosts in the replica set.
        hosts = config.get("hosts")
        if isinstance(hosts, list) and hosts:
            hostaddrs = []
            for host in hosts:
                if ':' not in host:
                    host = (host, 27017)
                else:
                    host = host.split(':', 1)
                    host[1] = int(host[1])
                hostaddrs.append(host)
            self.__discovered = hostaddrs

        # Check if this node is the master.
        ismaster = config.get("ismaster")
        if not ismaster:
            reason = AutoReconnect("not master")
            proto.fail(reason)
            return

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
        if self.__notify_ready is None:
            self.__notify_ready = []
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

        all_nodes = list(self.uri["nodelist"]) + list(self.__discovered)
        if self.__index >= len(all_nodes):
            self.__index = 0
            delay = True

        connector.host, connector.port = all_nodes[self.__index]

        if delay:
            self.retry(connector)
        else:
            connector.connect()

    def setInstance(self, instance=None, reason=None):
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
             for database, (username, password, mechanism) in self.__auth_creds.iteritems()],
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

    def __init__(self, uri="mongodb://127.0.0.1:27017", pool_size=1, ssl_context_factory=None):
        assert isinstance(uri, basestring)
        assert isinstance(pool_size, int)
        assert pool_size >= 1

        if not uri.startswith("mongodb://"):
            uri = "mongodb://".join(uri)

        self.__uri = parse_uri(uri)
        self.__pool_size = pool_size
        self.__pool = [_Connection(self, self.__uri, i) for i in range(pool_size)]

        if self.__uri['database'] and self.__uri['username'] and self.__uri['password']:
            self.authenticate(self.__uri['database'], self.__uri['username'],
                              self.__uri['password'])

        host, port = self.__uri['nodelist'][0]

        for factory in self.__pool:
            if ssl_context_factory:
                factory.connector = reactor.connectSSL(host, port, factory, ssl_context_factory)
            else:
                factory.connector = reactor.connectTCP(host, port, factory)

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
            raise e.subFailure


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


###
# Begin Legacy Wrapper
###

class MongoConnection(ConnectionPool):
    def __init__(self, host="127.0.0.1", port=27017, pool_size=1):
        uri = "mongodb://%s:%d/" % (host, port)
        ConnectionPool.__init__(self, uri, pool_size=pool_size)


lazyMongoConnectionPool = MongoConnection
lazyMongoConnection = MongoConnection
MongoConnectionPool = MongoConnection

###
# End Legacy Wrapper
###
