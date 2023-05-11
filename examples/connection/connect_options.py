from twisted.internet import defer
from twisted.internet.task import react
from txmongo.connection import ConnectionPool


@defer.inlineCallbacks
def replicaset():
    conn = ConnectionPool("mongodb://localhost:27017/?replicaSet=rs0")
    yield conn.test.fruits.insert({"kind": "Pear"})
    yield conn.disconnect()


@defer.inlineCallbacks
def read_preference():
    conn = ConnectionPool("mongodb://localhost:27017/?readPreference=secondary")
    fruits = yield conn.test.fruits.find()
    print(fruits)
    yield conn.disconnect()


@defer.inlineCallbacks
def main(reactor):
    yield replicaset()
    yield read_preference()


react(main)
