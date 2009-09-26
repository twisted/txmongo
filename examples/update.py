#!/usr/bin/env python
# coding: utf-8

import pymonga
from twisted.internet import reactor

def safe_update(result):
    print result
    reactor.stop()

def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    # update something
    test.update({"foo":"bar"}, {"foo":"bar", "name":"john doe"}, upsert=True)
    deferred = test.update({"homer":"simpson"}, {"homer":"simpson", "wife":"marge"},
	upsert=True, safe=True)
    deferred.addCallback(safe_update)

if __name__ == '__main__':
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
