#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import time

from twisted.internet import defer, reactor

import txmongo


@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # insert some data
    for x in range(1000):
        result = yield test.insert_one({"something": x * time.time()})

    # but much more effective like this:
    result = yield test.insert_many({"something": x * time.time()} for x in range(1000))


if __name__ == "__main__":
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
