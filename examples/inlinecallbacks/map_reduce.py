#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.
from twisted.internet import defer
from twisted.internet.task import react

import txmongo


@react
@defer.inlineCallbacks
def example(reactor):
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    yield test.insert_many(
        [
            {"src": "Twitter", "content": "bla bla"},
            {"src": "Twitter", "content": "more data"},
            {"src": "Wordpress", "content": "blog article 1"},
            {"src": "Wordpress", "content": "blog article 2"},
            {"src": "Wordpress", "content": "some comments"},
        ]
    )

    result = yield test.map_reduce(
        "function () { emit(this.src, 1) }",
        "function (key, values) { return Array.sum(values) }",
        out={"inline": 1},
    )

    print("result:", result)
