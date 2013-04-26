#!/usr/bin/env python
# coding: utf-8
import sys
import time

from twisted.internet import defer, reactor
from twisted.python import log

import txmongo


def getConnection():
    print "getting connection..."
    return txmongo.MongoConnectionPool()


def insertData(conn):
    coll = conn.foo.test
    print "inserting data..."
    # insert some data, building a deferred list so that we can later check
    # the succes or failure of each deferred result
    for x in xrange(10000):
        d = coll.insert({"something":x*time.time()}, safe=True)
        d.addErrback(log.err)
    return d


def processResult(result):
    print "processing last insert ..."
    print "last inserted id: %s" % result


def finish(ignore):
    print "finishing up..."
    reactor.stop()


def example():
    d = txmongo.MongoConnectionPool()
    d.addCallback(insertData)
    d.addErrback(log.err)
    d.addCallback(processResult)
    d.addErrback(log.err)
    d.addCallback(finish)
    return d


if __name__ == '__main__':
    log.startLogging(sys.stdout)
    example()
    reactor.run()

