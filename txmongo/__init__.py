# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import pymongo
from   pymongo                   import errors
from   pymongo.uri_parser        import parse_uri
from   twisted.internet          import defer, reactor, task
from   twisted.internet.protocol import ReconnectingClientFactory
from   txmongo.database          import Database
from   txmongo.protocol          import MongoProtocol, Query
from   txmongo.connection        import MongoConnection, MongoConnectionPool, lazyMongoConnection, lazyMongoConnectionPool


if __name__ == '__main__':
    import sys
    from twisted.python import log

    log.startLogging(sys.stdout)
    connection = Connection()
    reactor.run()
