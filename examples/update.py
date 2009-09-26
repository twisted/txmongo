#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from twisted.internet import reactor

def safe_update(result):
    print result
    finish()

def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    # update something
    test.update({"foo":"bar"}, {"foo":"bar", "name":"john doe"}, upsert=True)
    deferred = test.update({"foo":"bar"}, {"foo":"bar", "name":"something"}, safe=True)
    deferred.addCallback(safe_update)

def finish():
    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
