#!/usr/bin/env python
# coding: utf-8

import time
import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # insert some data
    for x in xrange(10000):
        result = yield test.safe_insert({"something":x*time.time()})
        print result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
