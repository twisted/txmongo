#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from pymonga import filter
from twisted.internet import reactor

def show_results(docs, collection):
    print "got %d results" % len(docs)
    for n, doc in enumerate(docs):
	print n, doc
    finish()

def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    # fetch documents
    f = filter.sort(filter.DESCENDING("something"))
    #f += filter.hint(filter.DESCENDING("myindex"))
    #f += filter.explain()
    deferred = test.find(limit=10, filter=f)
    deferred.addCallback(show_results, test)

def finish():
    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
