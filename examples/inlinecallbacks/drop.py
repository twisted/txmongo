#!/usr/bin/env python
# coding: utf-8

import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    result = yield test.drop(safe=True)
    print result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
