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

    from txmongo.connection import ConnectionPool
    from twisted.internet import defer, reactor

    @defer.inlineCallbacks
    def example():
        mongodb_uri = "mongodb://localhost:27017"
        mongo = yield ConnectionPool(mongodb_uri)

        foo = mongo.foo  # `foo` database
        test = foo.test  # `test` collection

        # fetch some documents
        docs = yield test.find(limit=10)
        for doc in docs:
            print(doc)

    if __name__ == '__main__':
        example().addCallback(lambda ign: reactor.stop())
        reactor.run()

TLS
---

Enable TLS with URI options (requires ``pyOpenSSL`` and ``service_identity``,
e.g. ``pip install twisted[tls]``). The server certificate is verified against
the system trust store and each node's hostname:

.. code-block:: python

    mongo = ConnectionPool("mongodb://db.example.com:27017/?tls=true")

Supported options: ``tlsCAFile``, ``tlsCertificateKeyFile`` (client certificate
and key in one PEM file), ``tlsAllowInvalidHostnames``,
``tlsAllowInvalidCertificates`` / ``tlsInsecure``. For anything else pass your own
``ssl_context_factory`` to ``ConnectionPool``; it takes precedence over URI options.


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
