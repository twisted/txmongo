#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import sys
import time

from twisted.internet import defer, reactor
from twisted.python import log

import txmongo


def getConnection():
    print "getting connection..."
    return txmongo.MongoConnectionPool()


def getDatabase(conn, dbName):
    print "getting database..."
    return getattr(conn, dbName)


def getCollection(db, collName):
    print "getting collection..."
    return getattr(db, collName)


def insertData(coll):
    print "inserting data..."
    # insert some data, building a deferred list so that we can later check
    # the succes or failure of each deferred result
    deferreds = []
    for x in range(10000):
        d = coll.insert({"something":x*time.time()}, safe=True)
        deferreds.append(d)
    return defer.DeferredList(deferreds)


def processResults(results):
    print "processing results..."
    failures = 0
    successes = 0
    for success, result in results:
        if success:
            successes += 1
        else:
            failures += 1
    print "There were %s successful inserts and %s failed inserts." % (
        successes, failures)


def finish(ignore):
    print "finishing up..."
    reactor.stop()


def example():
    d = getConnection()
    d.addErrback(log.err)
    d.addCallback(getDatabase, "foo")
    d.addCallback(getCollection, "test")
    d.addCallback(insertData)
    d.addErrback(log.err)
    d.addCallback(processResults)
    d.addErrback(log.err)
    d.addCallback(finish)
    return d


if __name__ == '__main__':
    log.startLogging(sys.stdout)
    example()
    reactor.run()
