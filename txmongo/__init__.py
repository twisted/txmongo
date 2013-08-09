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
