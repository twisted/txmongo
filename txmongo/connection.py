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
import pymongo
from pymongo import errors
from pymongo.uri_parser import parse_uri
from pymongo import auth

from bson.son import SON

from twisted.python import log
from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ReconnectingClientFactory

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
    
    def __init__(self, pool, uri,id):
        self.__discovered = []
        self.__notify_ready = []
        self.__pool = pool
        self.__uri = uri
        self.__conf_loop = task.LoopingCall(lambda: self.configure(self.instance))
        self.__conf_loop.start(self.__conf_loop_seconds, now=False)
        self.connectionid = id
        self.auth_set = set()
    
    def buildProtocol(self, addr):
        # Build the protocol.
        p = ReconnectingClientFactory.buildProtocol(self, addr)
        
        # If we do not care about connecting to a slave, then we can simply
        # return the protocol now and fire that we are ready.
        if self.uri['options'].get('slaveok', False):
            p.connectionReady().addCallback(lambda _: self.setInstance(instance=p))
            return p
        
        # Update our server configuration. This may disconnect if the node
        # is not a master.
        p.connectionReady().addCallback(lambda _: self.configure(p))\
                           .addCallback(lambda _: self.resetDelay())
        
        return p
    
    def configure(self, proto):
        """
            Configures the protocol using the information gathered from the
            remote Mongo instance. Such information may contain the max
            BSON document size, replica set configuration, and the master
            status of the instance.
            """
        if proto:
            query = Query(collection='admin.$cmd', query={'ismaster': 1})
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
            proto.fail(errors.OperationFailure('Invalid document length.'))
            return
        
        # Get the configuration document from the reply.
        config = reply.documents[0].decode()
        
        # Make sure the command was successful.
        if not config.get('ok'):
            code = config.get('code')
            msg = config.get('err', 'Unknown error')
            proto.fail(errors.OperationFailure(msg, code))
            return
        
        # Check that the replicaSet matches.
        set_name = config.get('setName')
        expected_set_name = self.uri['options'].get('setname')
        if expected_set_name and (expected_set_name != set_name):
            # Log the invalid replica set failure.
            msg = 'Mongo instance does not match requested replicaSet.'
            reason = pymongo.errros.ConfigurationError(msg)
            proto.fail(reason)
            return
        
        # Track max bson object size limit.
        max_bson_size = config.get('maxBsonObjectSize')
        if max_bson_size:
            proto.max_bson_size = max_bson_size
        
        # Track the other hosts in the replica set.
        hosts = config.get('hosts')
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
        ismaster = config.get('ismaster')
        if not ismaster:
            reason = pymongo.errors.AutoReconnect('not master')
            proto.fail(reason)
            return
        
        # Notify deferreds waiting for completion.
        self.setInstance(instance=proto)
    
    def clientConnectionFailed(self, connector, reason):
        self.instance = None
        self.auth_set = set()
        if self.continueTrying:
            self.connector = connector
            self.retryNextHost()
    
    def clientConnectionLost(self, connector, reason):
        self.instance = None
        self.auth_set = set()
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
        
        allNodes = list(self.uri['nodelist']) + list(self.__discovered)
        if self.__index >= len(allNodes):
            self.__index = 0
            delay = True
        
        connector.host, connector.port = allNodes[self.__index]
        
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
        
        self.cred_cache = {}
        self.__uri = parse_uri(uri)
        self.__pool_size = pool_size
        self.__pool = [_Connection(self, self.__uri,i) for i in xrange(pool_size)]
        
        host, port = self.__uri['nodelist'][0]
        for factory in self.__pool:
            factory.connector = reactor.connectTCP(host, port, factory)
    
    def getprotocols(self):
        return self.__pool
    
    def __getitem__(self, name):
        return Database(self, name)
    
    def __getattr__(self, name):
        return self[name]
    
    def __repr__(self):
        if self.uri['nodelist']:
            return 'Connection(%r, %r)' % self.uri['nodelist'][0]
        return 'Connection()'
    

    @defer.inlineCallbacks
    def authenticate_with_nonce (self,database,name,password) :

        database_name = str(database)
        self.cred_cache[database_name] = (name,password)
        current_connection = self.__pool[self.__index]
        proto = yield self.getprotocol()

        collection_name = database_name+'.$cmd'
        query = Query(collection=collection_name, query={'getnonce': 1})
        result = yield proto.send_QUERY(query)
           
        result = result.documents[0].decode()
        
        if result["ok"] :
            nonce = result["nonce"]
        else :
            defer.returnValue(result["errmsg"])
        
        key = auth._auth_key(nonce, name, password)
        
        # hacky because order matters
        auth_command = SON(authenticate=1)
        auth_command['user'] = unicode(name)
        auth_command['nonce'] = nonce
        auth_command['key'] = key

        query = Query(collection=str(collection_name), query=auth_command)
        result = yield proto.send_QUERY(query)
                
        result = result.documents[0].decode()
                
        if result["ok"]:
            database._authenticated = True
            current_connection.auth_set.add(database_name)
            defer.returnValue(result["ok"])
        else:
            del self.cred_cache[database_name]
            defer.returnValue(result["errmsg"])
            
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
    def get_authenticated_protocol(self,database) :
        # Get the next protocol available for communication in the pool
        connection = self.__pool[self.__index]
        database_name = str(database)
        
        if database_name not in connection.auth_set :
            name  = self.cred_cache[database_name][0]
            password = self.cred_cache[database_name][1]
            yield self.authenticate_with_nonce(database,name,password)
        else :
            self.__index = (self.__index + 1) % self.__pool_size

        defer.returnValue(connection.instance)

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
    def __init__(self, host, port, pool_size=1):
        uri = 'mongodb://%s:%d/' % (host, port)
        ConnectionPool.__init__(self, uri, pool_size=pool_size)
lazyMongoConnectionPool = MongoConnection
lazyMongoConnection = MongoConnection
MongoConnectionPool = MongoConnection

###
# End Legacy Wrapper
###

