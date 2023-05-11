from twisted.internet import defer
from twisted.internet.task import react
from txmongo.connection import ConnectionPool


@defer.inlineCallbacks
def main(reactor):
    # default pool_size is 1
    mongo_conn = ConnectionPool("mongodb://localhost:27017/", pool_size=4)

    yield mongo_conn.test.hobbits.insert({"name": "Frodo"})
    yield mongo_conn.disconnect()


react(main)
