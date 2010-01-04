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
    yield test.safe_insert({"foo":"bar", "name":"bla"})

    # update
    result = yield test.safe_update({"foo":"bar"}, {"$set": {"name":"john doe"}})
    print "result:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
