#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
import txmongo
import txmongo.filter
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo # `foo` database
    test = foo.test # `test` collection

    # specify the fields to be returned by the query
    # reference: http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
    whitelist = {'_id': 1, 'name': 1}
    blacklist = {'_id': 0}
    quickwhite = ['_id', 'name']

    fields = blacklist

    # fetch some documents
    docs = yield test.find(limit=10, fields=fields)
    for n, doc in enumerate(docs):
        print n, doc

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
