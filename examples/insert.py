#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from twisted.internet import defer, reactor

def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    # insert some data
    inserts = []
    for x in xrange(10000):
        d = test.insert({"something":x*time.time()}, safe=True)
        inserts.append(d)

    deferred = defer.gatherResults(inserts)
    deferred.addCallback(finish)

def finish(ignore):
    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
