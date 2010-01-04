#!/usr/bin/env python
# coding: utf-8

import txmongo
from txmongo import filter
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    idx = filter.sort(filter.ASCENDING("something") + filter.DESCENDING("else"))
    print "IDX:", idx

    result = yield test.create_index(idx)
    print "create_index:", result

    result = yield test.index_information()
    print "index_information:", result

    result = yield test.drop_index(idx)
    print "drop_index:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
