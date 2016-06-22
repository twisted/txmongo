from twisted.internet import defer
from twisted.trial import unittest

import txmongo


class SingleCollectionTest(unittest.TestCase):
    mongo_host = "localhost"
    mongo_port = 27017

    def setUp(self):
        self.conn = txmongo.MongoConnection(self.mongo_host, self.mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.coll.drop()
        yield self.conn.disconnect()
