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
# run:
#  twistd -ny twistwedweb_server.tac

import _local_path
import txmongo
from twisted.internet import defer
from twisted.web import xmlrpc, server
from twisted.web.resource import Resource
from twisted.application import service, internet

class Root(Resource):
    isLeaf = False

class BaseHandler(object):
    isLeaf = True
    def __init__(self, db):
        self.db = db
        Resource.__init__(self)

class IndexHandler(BaseHandler, Resource):
    def _success(self, value, request, message):
        request.write(message % repr(value))
        request.finish()

    def _failure(self, error, request, message):
        request.write(message % str(error))
        request.finish()
        
    def render_GET(self, request):
        try:
            name = request.args["name"][0]
        except:
            request.setResponseCode(404, "not found")
            return ""

        d = self.db.find({"name":name})
        d.addCallback(self._success, request, "result(s): %s\n")
        d.addErrback(self._failure, request, "find failed: %s\n")
        return server.NOT_DONE_YET

    def render_POST(self, request):
        name = request.args["name"][0]
        self.db.insert({"name":name})
        return "ok\n"


class InfoHandler(BaseHandler, Resource):
    def render_GET(self, request):
        return "mongo: %s\n" % repr(self.db)


class XmlrpcHandler(BaseHandler, xmlrpc.XMLRPC):
    allowNone = True

    @defer.inlineCallbacks
    def xmlrpc_find(self, spec, limit=10):
        result = yield self.db.find(spec, limit=limit)
        defer.returnValue(repr(result))

    @defer.inlineCallbacks
    def xmlrpc_insert(self, doc):
        result = yield self.db.insert(doc, safe=True)
        defer.returnValue(repr(result))

    @defer.inlineCallbacks
    def xmlrpc_update(self, spec, doc):
        result = yield self.db.update(spec, doc, safe=True)
        defer.returnValue(repr(result))


# redis connection
_db = txmongo.lazyMongoConnectionPool()
collection = _db.foo.test

# http resources
root = Root()
root.putChild("", IndexHandler(collection))
root.putChild("xmlrpc", XmlrpcHandler(collection))

application = service.Application("webmongo")
srv = internet.TCPServer(8888, server.Site(root), interface="127.0.0.1")
srv.setServiceParent(application)
