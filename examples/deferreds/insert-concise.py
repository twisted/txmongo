#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import sys
import time

from twisted.internet import reactor
from twisted.python import log

import txmongo


def insertData(conn):
    print "inserting data..."
    collection = conn.foo.test
    for x in range(10000):
        d = collection.insert({"something":x*time.time()}, safe=True)
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
    d.addCallback(processResult)
    d.addErrback(log.err)
    d.addCallback(finish)
    return d


if __name__ == '__main__':
    log.startLogging(sys.stdout)
    example()
    reactor.run()
