#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from pymonga import filter as qf
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    idx = qf.sort(qf.ASCENDING("something") + qf.DESCENDING("else"))
    print "IDX:", idx

    result = yield test.create_index(idx)
    print "create_index:", result

    result = yield test.index_information()
    print "index_information:", result

    result = yield test.drop_index(idx)
    print "drop_index:", result

    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
