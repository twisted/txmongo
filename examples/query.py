#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from twisted.internet import reactor

def show_results(docs, collection):
    print "got %d results" % len(docs)
    #for doc in docs:
	#print doc
	#collection.remove(doc["_id"])
    finish()

def connectionMade(db):
    foo = db.foo     # `foo` database
    test = foo.test  # `test` collection

    # fetch documents
    deferred = test.find(limit=200000)
    deferred.addCallback(show_results, test)

def finish(ignore=None):
    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    deferred = pymonga.Connection()
    deferred.addCallback(connectionMade)
    reactor.run()
