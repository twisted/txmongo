#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
from twisted.internet import defer, reactor

import txmongo
import txmongo.filter


@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # create the filter
    f = txmongo.filter.sort(txmongo.filter.DESCENDING("something"))
    # f += txmongo.filter.hint(txmongo.filter.DESCENDING("myindex"))
    # f += txmongo.filter.explain()

    # fetch some documents
    docs = yield test.find(limit=10, sort=f)
    for n, doc in enumerate(docs):
        print(n, doc)


if __name__ == "__main__":
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
