#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
from twisted.internet import defer, reactor

import txmongo
from txmongo.dbref import DBRef


@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    doc_a = {"username": "foo", "password": "bar"}
    result = yield test.insert_one(doc_a)

    doc_b = {"settings": "foobar", "owner": DBRef(test, result)}
    yield test.insert_one(doc_b)

    doc = yield test.find_one({"settings": "foobar"})
    print("doc is:", doc)

    if isinstance(doc["owner"], DBRef):
        ref = doc["owner"]
        owner = yield foo[ref.collection].find_one(ref.id)
        print("owner:", owner)


if __name__ == "__main__":
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
