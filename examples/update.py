#!/usr/bin/env python
# coding: utf-8

import txmongo
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    # insert
    result = yield test.insert({"foo":"bar", "name":"bla"}, safe=True)
    print "insert result:", result

    # update
    result = yield test.update({"foo":"bar"}, {"$set": {"name":"john doe"}}, safe=True)
    print "update result:", result

    # find and modify
    spec1 = {"query": {"foo":"bar"},
             "update": {"$set": {"name": "findAndModify collection"}},
             "new": True, }
    result = yield test.runCommand("findAndModify", test.name, **spec1)
    print "findAndModify via collection.runCommand updated doc:", result
    
    spec2 = {"query": {"foo":"bar"},
             "update": {"$set": {"name": "findAndModify database"}},
             "new": True, }
    result = yield foo.command("findAndModify", test.name, **spec2)
    print "findAndModify via database.command updated doc:", result
    
    result = yield test.findAndModify(query = {"foo":"bar"},
                                      update = {"$set": {"name": "findAndModify method"}},
                                      **{"new": True} )
    print "findAndModify via collection method updated doc:", result
    
    result = yield test.findAndModify(query = {"foo":"barsert new"},
                                      update = {"$set": {"name": "findAndModify upsert"}},
                                      upsert = True,
                                      **{"new": True} )
    print "findAndModify via collection method upserted doc:", result

    result = yield test.findAndModify(query = {"foo":"barsert old"},
                                      update = {"$set": {"name": "findAndModify upsert"}},
                                      upsert = True, )
    print "findAndModify via collection method result (old doc):", result

    result = yield foo.command("profile", -1)
    print "profile level:", result

    result = yield foo.command("ping")
    print "ping:", result

if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
