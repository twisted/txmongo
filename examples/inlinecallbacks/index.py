#!/usr/bin/env python
# coding: utf-8
# Copyright 2009-2014 The txmongo authors.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import _local_path
import txmongo
from txmongo import filter
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def example():
    mongo = yield txmongo.MongoConnection()

    foo = mongo.foo  # `foo` database
    test = foo.test  # `test` collection

    idx = filter.sort(filter.ASCENDING("something") + filter.DESCENDING("else"))
    print "IDX:", idx

    result = yield test.create_index(idx)
    print "create_index:", result

    result = yield test.index_information()
    print "index_information:", result

    result = yield test.drop_index(idx)
    print "drop_index:", result

    # Geohaystack example
    geoh_idx = filter.sort(filter.GEOHAYSTACK("loc") + filter.ASCENDING("type"))
    print "IDX:", geoh_idx
    result = yield test.create_index(geoh_idx, **{'bucketSize':1})
    print "index_information:", result

    result = yield test.drop_index(geoh_idx)
    print "drop_index:", result

    # 2D geospatial index
    geo_idx = filter.sort(filter.GEO2D("pos"))
    print "IDX:", geo_idx
    result = yield test.create_index(geo_idx, **{ 'min':-100, 'max':100 })
    print "index_information:", result

    result = yield test.drop_index(geo_idx)
    print "drop_index:", result


if __name__ == '__main__':
    example().addCallback(lambda ign: reactor.stop())
    reactor.run()
