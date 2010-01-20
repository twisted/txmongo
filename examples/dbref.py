#!/usr/bin/env python
# coding: utf-8

import txmongo
from txmongo.dbref import DBRef
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    doc_a = {"username":"foo", "password":"bar"}
    result = yield test.insert(doc_a, safe=True)

    doc_b = {"settings":"foobar", "owner":DBRef(test, result)}
    yield test.insert(doc_b, safe=True)

    doc = yield test.find_one({"settings":"foobar"})
    print "doc is:", doc

    if isinstance(doc["owner"], DBRef):
        ref = doc["owner"]
        owner = yield foo[ref.collection].find_one(ref.id)
        print "owner:", owner

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
