#!/usr/bin/env python
# coding: utf-8

import time
import pymonga
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def main():
    conn = yield pymonga.Connection()
    foo = conn.foo # database
    test = foo.test # collection

    yield test.insert({"src":"Twitter", "content":"bla bla"}, safe=True)
    yield test.insert({"src":"Twitter", "content":"more data"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"blog article 1"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"blog article 2"}, safe=True)
    yield test.insert({"src":"Wordpress", "content":"some comments"}, safe=True)

    result = yield test.group(keys=["src"],
        initial={"count":0}, reduce="function(obj,prev){prev.count++;}")

    print result
    print "%s seconds" % (time.time() - startTime)
    reactor.stop()

if __name__ == '__main__':
    startTime = time.time()
    main()
    reactor.run()
