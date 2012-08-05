from txmongo.connection import MongoConnection


# Legacy aliases
lazyMongoConnectionPool = MongoConnection
lazyMongoConnection = MongoConnection
MongoConnectionPool = MongoConnection


if __name__ == '__main__':
    import sys
    from twisted.python import log

    log.startLogging(sys.stdout)
    connection = Connection()
    reactor.run()
