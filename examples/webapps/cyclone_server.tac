#!/usr/bin/env python
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
#
# requires cyclone:
#  http://github.com/fiorix/cyclone
# run:
#  twistd -ny cyclone_server.tac

import _local_path
import txmongo
import cyclone.web
from twisted.internet import defer
from twisted.application import service, internet

class IndexHandler(cyclone.web.RequestHandler):
    @defer.inlineCallbacks
    def get(self):
        name = self.get_argument("name")
        try:
            result = yield self.settings.db.find({"name":name})
        except Exception, e:
            self.write("find failed: %s\n" % str(e))
        else:
            self.write("result(s): %s\n" % repr(result))
        self.finish()

    def post(self):
        name = self.get_argument("name")
        self.settings.db.insert({"name":name})
        self.write("ok\n")


class XmlrpcHandler(cyclone.web.XmlrpcRequestHandler):
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_find(self, spec, limit=10):
        result = yield self.settings.db.find(spec, limit=limit)
        defer.returnValue(repr(result))

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_insert(self, doc):
        result = yield self.settings.db.insert(doc, safe=True)
        defer.returnValue(repr(result))

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_update(self, spec, doc):
        result = yield self.settings.db.update(spec, doc, safe=True)
        defer.returnValue(repr(result))


class WebMongo(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r"/",       IndexHandler),
            (r"/xmlrpc", XmlrpcHandler),
        ]

        mongo = txmongo.lazyMongoConnectionPool()
        settings = {
            "db": mongo.foo.test
            #"static_path": "./static",
        }

        cyclone.web.Application.__init__(self, handlers, **settings)


application = service.Application("webmongo")
srv = internet.TCPServer(8888, WebMongo(), interface="127.0.0.1")
srv.setServiceParent(application)
