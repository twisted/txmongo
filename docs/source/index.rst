Welcome to TxMongo's documentation!
===================================

What is TxMongo
---------------

TxMongo is a pure-Python Twisted MongoDB client library that implements:

- Asynchronous client driver to MongoDB

Get it from `PyPI <https://pypi.python.org/pypi/TxMongo>`_, find out what's new in the :doc:`NEWS`!

Quick Usage Example
-------------------

.. code-block:: python

    from OpenSSL import SSL
    from txmongo.connection import ConnectionPool
    from twisted.internet import defer, reactor, ssl

    class ServerTLSContext(ssl.DefaultOpenSSLContextFactory):
        def __init__(self, *args, **kw):
            kw['sslmethod'] = SSL.TLSv1_METHOD
            ssl.DefaultOpenSSLContextFactory.__init__(self, *args, **kw)

    @defer.inlineCallbacks
    def example():
        tls_ctx = ServerTLSContext(privateKeyFileName='./mongodb.key', certificateFileName='./mongodb.crt')
        mongodb_uri = "mongodb://localhost:27017"

        mongo = yield ConnectionPool(mongodb_uri, ssl_context_factory=tls_ctx)

        foo = mongo.foo  # `foo` database
        test = foo.test  # `test` collection

        # fetch some documents
        docs = yield test.find(limit=10)
        for doc in docs:
            print doc

    if __name__ == '__main__':
        example().addCallback(lambda ign: reactor.stop())
        reactor.run()


User's Guide
------------

.. toctree::
   :maxdepth: 1

   txmongo
   txmongo._gridfs

Meta
----

.. toctree::
   :maxdepth: 1

   NEWS
   status
   AUTHORS

Indices and tables
==================

- :ref:`genindex`
- :ref:`modindex`
- :ref:`search`
