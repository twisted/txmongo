#!/usr/bin/env python
# coding: utf-8

import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    yield test.safe_insert({"src":"Twitter", "content":"bla bla"})
    yield test.safe_insert({"src":"Twitter", "content":"more data"})
    yield test.safe_insert({"src":"Wordpress", "content":"blog article 1"})
    yield test.safe_insert({"src":"Wordpress", "content":"blog article 2"})
    yield test.safe_insert({"src":"Wordpress", "content":"some comments"})

    result = yield test.group(keys=["src"],
        initial={"count":0}, reduce="function(obj,prev){prev.count++;}")

    print "result:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
