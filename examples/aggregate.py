#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    yield test.insert({"src":"Twitter", "content":"bla bla"}, safe=True)
    yield test.insert({"src":"Twitter", "content":"more data"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"blog article 1"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"blog article 2"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"some comments"}, safe=True)

    # Read more about the aggregation pipeline in MongoDB's docs
    pipeline = [
        {'$group': {'_id':'$src', 'content_list': {'$push': '$content'} } }
    ]
    result = yield test.aggregate(pipeline)

    print "result:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
