from txmongo.connection import MongoConnection, MongoConnectionPool


# Legacy aliases
lazyMongoConnection = MongoConnection
lazyMongoConnectionPool = MongoConnectionPool


if __name__ == '__main__':
    import sys
    from twisted.python import log

    log.startLogging(sys.stdout)
    connection = Connection()
    reactor.run()
