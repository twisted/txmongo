#!/usr/bin/env python
# coding: utf-8

import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # insert
    yield test.insert({"foo":"bar", "name":"bla"}, safe=True)

    # update
    result = yield test.update({"foo":"bar"}, {"$set": {"name":"john doe"}}, safe=True)
    print "result:", result

    # find and modify
    spec = {
        "query": {"foo":"bar"},
        "update": {"$set": {"name": "findAndModify name"}},
        "new": True,
        }
    result = yield test.runCommand("findAndModify", "test", **spec)
    print "findAndModify updated doc:", result

    result = yield test.runCommand("profile", -1)
    print "profile level:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
