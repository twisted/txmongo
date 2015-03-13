#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import time
import txmongo
from twisted.internet import defer, reactor


@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # insert some data
    for x in range(10000):
        result = yield test.insert({"something": x*time.time()}, safe=True)
        print result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
