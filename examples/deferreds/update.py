#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
import sys
import time

from twisted.internet import reactor
from twisted.python import log

import txmongo


def updateData(ignored, conn):
    print "updating data..."
    collection = conn.foo.test
    d = collection.update(
        {"foo": "bar"}, {"$set": {"name": "jane doe"}}, safe=True)
    d.addErrback(log.err)
    return d


def insertData(conn):
    print "inserting data..."
    collection = conn.foo.test
    d = collection.insert({"foo": "bar", "name": "john doe"}, safe=True)
    d.addErrback(log.err)
    d.addCallback(updateData, conn)
    return d


def finish(ignore):
    print "finishing up..."
    reactor.stop()


def example():
    d = txmongo.MongoConnection()
    d.addCallback(insertData)
    d.addCallback(finish)
    return d


if __name__ == '__main__':
    log.startLogging(sys.stdout)
    example()
    reactor.run()
